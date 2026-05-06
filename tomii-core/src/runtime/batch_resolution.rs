//! Inner batch-resolution loop: four-phase dependency propagation and successor scheduling.
//!
//! The single public entry point [`process_batch_inner`] implements the four-phase protocol
//! that prevents completion detection from racing with in-flight successor processing:
//! **Phase 1** stores results for network packets (compute results are pre-stored by workers),
//! **Phase 2** decrements the slot task counters, **Phase 3** resolves successor dependencies
//! and accumulates ready nodes, and **Phase 4** (in the *outer* `process_batch_resolution`)
//! decrements `processing_count` after all successor processing is finished.
//!
//! This module does **not** own the outer `processing_count` bookkeeping; that lives in
//! `resolution_loop::process_batch_resolution` which wraps this module's function.

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

/// Inner body of `process_batch_resolution`, invoked with all four thread-local buffers
/// already borrowed to eliminate the 4-deep `thread_local!` `.with()` nesting.
///
/// Implements Phases 1–3 of the completion protocol for every node in `batch`:
/// - **Phase 1**: store the result for network-injected packets (`Some(result_opt)`); compute
///   results are pre-stored by `execute_task` and arrive here as `None`.
/// - **Phase 2**: decrement `pending_cond_tasks` or `pending_tasks` depending on node kind.
/// - **Phase 3**: resolve successor dependencies via [`decrement_and_collect_ready`] and
///   accumulate ready nodes into `batch_sched`, flushing to workers at `flush_threshold`.
///
/// Phase 4 (decrement `processing_count`) is performed by the *caller*
/// (`process_batch_resolution`) after this function returns, ensuring completion detection
/// cannot fire until all successor scheduling for the batch is complete.
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

    let rctx = shared.resolve_ctx();
    let sctx = shared.sched_ctx();

    for (node_info, result_opt) in batch.drain(..) {
        // Mark stream activity for all nodes (including network nodes id=0)
        stream_slot_activity.insert(node_info.slot, true);

        if node_info.post_node {
            // For post_nodes: result pre-stored by execute_task (None here).
            // Network post_nodes (rare) carry Some(result) and store it now.
            if let Some(r) = result_opt {
                rctx.exec.node_results.set(&node_info, r);
            }
            continue;
        }

        // Phase 1: Store result for network packets (Some).
        // Compute task results are already stored by execute_task (None).
        if let Some(r) = result_opt {
            rctx.exec.node_results.set(&node_info, r);
        }

        // Phase 2: Decrement task counters
        let node_id_usize = node_info.id as usize;
        let node_cache_entry = &rctx.cache.node_cache[node_id_usize];

        if node_cache_entry.is_condition {
            let prev_cond =
                rctx.slots.pending_cond_tasks[node_info.slot].fetch_sub(1, Ordering::SeqCst);
            if prev_cond <= 10 || prev_cond.is_multiple_of(100) {
                print_debug(|| {
                    format!(
                        "COND task completed: slot={}, node_id={} ({}), prev_pending_cond={}, new={}",
                        node_info.slot, node_id_usize, node_cache_entry.name,
                        prev_cond, prev_cond - 1
                    )
                });
            }
        } else if !node_cache_entry.is_initial {
            let _ = rctx.slots.pending_tasks[node_info.slot].fetch_sub(1, Ordering::SeqCst);
        }

        // Phase 3: Collect successors and process them (no allocations)
        collect_successors_for_node_into(&shared.graph, rctx.cache, &node_info, succ_buf);
        sched.clear();

        // Load slot generation once per node (all successors share same slot)
        let slot_gen = rctx.slots.generation[node_info.slot].load(Ordering::SeqCst) as u32;

        for (_succ_info, has_cond, succ_id, pred_group) in succ_buf.iter() {
            let succ_node_id = *succ_id as usize;

            // Skip condition evaluation if all instances already spawned.
            // Use generational lazy check: if stored gen != slot_gen, treat as full factor.
            if *has_cond {
                let packed = rctx.slots.cond_instances_to_spawn[node_info.slot][succ_node_id]
                    .load(Ordering::SeqCst);
                let stored_gen = gen_unpack_gen(packed);
                let remaining_spawns = if stored_gen == slot_gen {
                    gen_unpack_val(packed)
                } else {
                    rctx.cache.node_cache[succ_node_id].factor as u32 // stale gen → full factor
                };
                print_debug(|| {
                    format!(
                        "COND_CHECK: pred={} index={} succ={} ({}) remaining_spawns={} slot_gen={} stored_gen={}",
                        node_info.id, node_info.index, succ_node_id,
                        rctx.cache.node_cache[succ_node_id].name,
                        remaining_spawns, slot_gen, stored_gen
                    )
                });
                if remaining_spawns == 0 {
                    continue;
                }
            }

            // Decrement dependency counter; ready indices written into `ready`.
            // For 1:1 non-barrier deps, `decrement_and_collect_ready` computes
            // the specific successor instance so its result is guaranteed available.
            decrement_and_collect_ready(
                &rctx,
                node_info.slot,
                node_info.id,
                node_info.index,
                succ_node_id,
                *pred_group,
                1,
                slot_gen,
                ready,
            );

            print_debug(|| {
                format!(
                    "DECREMENT: pred={} index={} succ={} ({}) ready_count={}",
                    node_info.id,
                    node_info.index,
                    succ_node_id,
                    rctx.cache.node_cache[succ_node_id].name,
                    ready.len()
                )
            });

            // Fanout-bulk: accumulate arrivals for 1:1 bulk dispatch (Upgrade 5).
            // Only applies to non-condition successors (is_fanout_bulk requires !is_condition).
            let succ_entry = &rctx.cache.node_cache[succ_node_id];
            let fanout_bulk_eligible = !has_cond
                && succ_entry.is_fanout_bulk
                && !shared.config.no_fanout_bulk
                && (!shared.config.inline_continuation || shared.config.workers == 1);
            if fanout_bulk_eligible && !ready.is_empty()
            {
                let new_arrived = super::task_execution::fanout_bulk_increment(
                    &rctx.slots.fanout_bulk_arrived[node_info.slot][succ_node_id],
                    slot_gen,
                    ready.len(),
                );
                if new_arrived >= succ_entry.factor {
                    let factor = succ_entry.factor;
                    let n_chunks = shared.config.workers.min(factor).max(1);
                    let base = factor / n_chunks;
                    let extra = factor % n_chunks;
                    let mut start = 0usize;
                    for c in 0..n_chunks {
                        let count = base + if c < extra { 1 } else { 0 };
                        let mut chunk_ni = NodeInfo::new(
                            succ_node_id as IdType,
                            node_info.slot,
                            start,
                            node_info.index,
                        );
                        chunk_ni.bulk_count = count;
                        chunk_ni.gen = slot_gen;
                        sched.push(chunk_ni);
                        start += count;
                    }
                }
                continue;
            }

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
        if batch_sched.len() >= rctx.cfg.batch.flush_threshold {
            super::scheduling::dispatch_nodes(shared, &sctx, batch_sched, thread_core, thread_slot);
            batch_sched.clear();
        }
    }

    // Final flush for any remaining successors after the batch loop.
    if !batch_sched.is_empty() {
        super::scheduling::dispatch_nodes(shared, &sctx, batch_sched, thread_core, thread_slot);
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
        // Condition failed: this instance will not execute. All predecessors have
        // completed (dep counter hit 0), so in a non-loop DAG no future trigger
        // can fire it. Discharge it from pending_cond_tasks so the slot can complete.
        shared.slot_data.pending_cond_tasks[scheduled_succ_info.slot]
            .fetch_sub(1, Ordering::SeqCst);
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
