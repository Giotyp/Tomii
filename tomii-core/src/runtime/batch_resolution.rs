/// Batch resolution inner loop: dependency propagation and successor scheduling.
use super::shared_data::SharedData;
use super::successor::{
    collect_successors_for_node_into, conditions_met, decrement_and_collect_ready,
    evaluate_node_condition,
};
use crate::buffers::*;
use crate::debug::print_debug;
use crate::IdType;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tomii_types::*;

/// Inner body of `process_batch_resolution` executed with all four thread-local buffers
/// already borrowed. Separated from the outer function to eliminate the 4-deep
/// `thread_local!` `.with()` nesting while keeping the thread-local declarations intact.
#[allow(clippy::too_many_arguments)]
pub(super) fn process_batch_inner(
    shared: &Arc<SharedData>,
    batch: &mut Vec<(NodeInfo, Option<CmTypes>)>,
    thread_core: usize,
    thread_id: usize,
    thread_slot: usize,
    cond_indexes: &[Vec<usize>],
    stream_slot_activity: &mut HashMap<usize, bool>,
    succ_buf: &mut Vec<(NodeInfo, bool, IdType, Option<usize>)>,
    sched: &mut Vec<NodeInfo>,
    ready: &mut Vec<usize>,
    batch_sched: &mut Vec<NodeInfo>,
) {
    batch_sched.clear();

    for (node_info, result_opt) in batch.drain(..) {
        // Mark stream activity for all nodes (including network nodes id=0)
        stream_slot_activity.insert(node_info.slot, true);

        if node_info.post_node {
            // For post_nodes: result pre-stored by execute_task (None here).
            // Network post_nodes (rare) carry Some(result) and store it now.
            if let Some(r) = result_opt {
                shared.exec.node_results.set(&node_info, r);
            }
            continue;
        }

        // Phase 1: Store result for network packets (Some).
        // Compute task results are already stored by execute_task (None).
        if let Some(r) = result_opt {
            shared.exec.node_results.set(&node_info, r);
        }

        // Phase 2: Decrement task counters
        let node_id_usize = node_info.id as usize;
        let node_cache_entry = &shared.graph_cache.node_cache[node_id_usize];

        if node_cache_entry.is_condition {
            let prev_cond =
                shared.slot_data.pending_cond_tasks[node_info.slot].fetch_sub(1, Ordering::SeqCst);
            if prev_cond <= 10 || prev_cond % 100 == 0 {
                print_debug(|| {
                    format!(
                        "COND task completed: slot={}, node_id={} ({}), prev_pending_cond={}, new={}",
                        node_info.slot, node_id_usize, node_cache_entry.name,
                        prev_cond, prev_cond - 1
                    )
                });
            }
        } else if !node_cache_entry.is_initial {
            let _ = shared.slot_data.pending_tasks[node_info.slot].fetch_sub(1, Ordering::SeqCst);
        }

        // Phase 3: Collect successors and process them (no allocations)
        collect_successors_for_node_into(shared, &node_info, succ_buf);
        sched.clear();

        // Load slot generation once per node (all successors share same slot)
        let slot_gen = shared.slot_data.generation[node_info.slot].load(Ordering::SeqCst) as u32;

        for (_succ_info, has_cond, succ_id, pred_group) in succ_buf.iter() {
            let succ_node_id = *succ_id as usize;

            // Skip condition evaluation if all instances already spawned.
            // Use generational lazy check: if stored gen != slot_gen, treat as full factor.
            if *has_cond {
                let packed = shared.slot_data.cond_instances_to_spawn[node_info.slot][succ_node_id]
                    .load(Ordering::SeqCst);
                let stored_gen = gen_unpack_gen(packed);
                let remaining_spawns = if stored_gen == slot_gen {
                    gen_unpack_val(packed)
                } else {
                    shared.graph_cache.node_cache[succ_node_id].factor as u32 // stale gen → full factor
                };
                if remaining_spawns == 0 {
                    continue;
                }
            }

            // Decrement dependency counter; ready indices written into `ready`.
            // For 1:1 non-barrier deps, `decrement_and_collect_ready` computes
            // the specific successor instance so its result is guaranteed available.
            decrement_and_collect_ready(
                shared,
                node_info.slot,
                node_info.id,
                node_info.index,
                succ_node_id,
                *pred_group,
                1,
                slot_gen,
                ready,
            );

            for &ready_index in ready.iter() {
                let scheduled_succ_info = NodeInfo::new(
                    succ_node_id as IdType,
                    node_info.slot,
                    ready_index,
                    node_info.index,
                );

                if !has_cond {
                    sched.push(scheduled_succ_info);
                } else {
                    dispatch_condition_successor(
                        shared,
                        &node_info,
                        scheduled_succ_info,
                        succ_node_id,
                        slot_gen,
                        cond_indexes,
                        sched,
                    );
                }
            }
        }

        // Accumulate this node's successors into the batch buffer.
        batch_sched.extend_from_slice(sched);
        print_debug(|| {
            format!(
                "Thread {:?} -- Processed node {} in slot {}, scheduled: {:?}",
                thread_id,
                node_info.id,
                node_info.slot,
                sched.iter().map(|n| (n.id, n.index)).collect::<Vec<_>>()
            )
        });

        // Incremental flush: dispatch accumulated successors to workers
        // as soon as we have enough, rather than waiting for the entire
        // batch to finish. This eliminates the dead zone where workers
        // idle while the system thread processes a large batch.
        if batch_sched.len() >= shared.config.batch.flush_threshold {
            super::scheduling::preparation(shared, batch_sched, thread_core, thread_slot);
            batch_sched.clear();
        }
    }

    // Final flush for any remaining successors after the batch loop.
    if !batch_sched.is_empty() {
        super::scheduling::preparation(shared, batch_sched, thread_core, thread_slot);
    }
}

/// Evaluates the condition for a successor node that has `has_cond == true` and either
/// pushes it onto `sched` (condition passed) or increments its dependency counter and
/// resets its sent flag (condition failed).
fn dispatch_condition_successor(
    shared: &Arc<SharedData>,
    node_info: &NodeInfo,
    scheduled_succ_info: NodeInfo,
    succ_node_id: usize,
    slot_gen: u32,
    cond_indexes: &[Vec<usize>],
    sched: &mut Vec<NodeInfo>,
) {
    let cond_idx = shared.graph_cache.node_cache[succ_node_id].cond_index;
    let succ_cache = &shared.graph_cache.node_cache[succ_node_id];

    let condition_passed = if let Some(cond_cache) = &succ_cache.node_condition {
        let node_cond = shared.graph.nodes[succ_node_id].condition.as_ref().unwrap();
        evaluate_node_condition(shared, &scheduled_succ_info, cond_cache, node_cond)
    } else {
        conditions_met(shared, &scheduled_succ_info, &cond_indexes[cond_idx])
    };

    if condition_passed {
        sched.push(scheduled_succ_info.clone());
        // Decrement cond_instances_to_spawn with generational lazy reinit
        let factor = shared.graph_cache.node_cache[succ_node_id].factor as u32;
        let prev_packed = shared.slot_data.cond_instances_to_spawn[node_info.slot][succ_node_id]
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |packed| {
                let stored_gen = gen_unpack_gen(packed);
                let current = if stored_gen == slot_gen {
                    gen_unpack_val(packed)
                } else {
                    factor // lazy reinit
                };
                Some(gen_pack(slot_gen, current.saturating_sub(1)))
            })
            .unwrap();
        let prev_spawns = {
            let sg = gen_unpack_gen(prev_packed);
            if sg == slot_gen {
                gen_unpack_val(prev_packed) as usize
            } else {
                factor as usize
            }
        };
        print_debug(|| {
            format!(
                "Condition passed for node {} ({}) index {}: remaining spawns {} -> {}",
                succ_node_id,
                succ_cache.name,
                scheduled_succ_info.index,
                prev_spawns,
                prev_spawns.saturating_sub(1)
            )
        });
    } else {
        shared
            .exec
            .resolution_state
            .increment_dependency(&scheduled_succ_info, slot_gen);
        shared.exec.resolution_state.reset_sent(
            node_info.slot,
            scheduled_succ_info.id as usize,
            scheduled_succ_info.index,
            slot_gen,
        );
    }
}
