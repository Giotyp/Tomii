/// Private helpers for `SynRtBuilder::build()` — pure computation, no threading.
use super::node_cache::{node_cache_entry, NodeCacheEntry};
use crate::graph::*;
use crate::scheduler::SchedulerImpl;
use crate::IdType;
use std::sync::atomic::{AtomicU64, AtomicUsize};

/// Build the node cache from graph nodes, computing all pre-derived flags.
///
/// Sets: `successor_count`, `worker_resolvable`, `needs_result_store`,
/// `priority`, and `affinity_group` in addition to the base cache entry.
pub(super) fn build_node_cache(app_graph: &Graph, scheduler: &SchedulerImpl) -> Vec<NodeCacheEntry> {
    let init_objects = app_graph.init_objects.as_ref().unwrap();
    let mut cache: Vec<NodeCacheEntry> = app_graph
        .nodes
        .iter()
        .map(|node| {
            node_cache_entry(
                node,
                init_objects,
                &app_graph.initial_nodes,
                &app_graph.condition_nodes,
            )
        })
        .collect();

    // successor_count — used by inline-continuation to decide whether to elide a spawn
    for (node_id, entry) in cache.iter_mut().enumerate() {
        if node_id < app_graph.successors.len() {
            entry.successor_count = app_graph.successors[node_id].len();
        }
    }

    // worker_resolvable — true when all successors are non-condition nodes;
    // allows the completing worker to resolve deps without touching the batch_queue.
    for node_id in 0..cache.len() {
        let all_non_condition = if node_id < app_graph.successors.len() {
            app_graph.successors[node_id]
                .iter()
                .all(|&succ_id| cache[succ_id as usize].node_condition.is_none())
        } else {
            true // no successors → eligible
        };
        cache[node_id].worker_resolvable = all_non_condition;
    }

    // needs_result_store — false when no successor reads this node via $res.
    // When false, node_results.set() can be elided on the hot path.
    for node_id in 0..cache.len() {
        let has_res_consumer = node_id < app_graph.successors.len()
            && app_graph.successors[node_id].iter().any(|&succ_id| {
                app_graph.nodes[succ_id as usize].args.iter().any(|arg| {
                    arg.type_.is_result()
                        && arg
                            .predecessor
                            .as_ref()
                            .map_or(false, |p| p.id == node_id as IdType)
                })
            });
        cache[node_id].needs_result_store = has_res_consumer;
    }

    // priority and affinity_group — pre-computed to avoid per-task lookups on the hot path
    {
        use crate::custom_scheduler::Priority;
        use crate::graph_struct::NodePriority;
        for (node_id, entry) in cache.iter_mut().enumerate() {
            let node = &app_graph.nodes[node_id];
            entry.priority = match node.priority {
                NodePriority::High => Priority::High,
                NodePriority::Normal => Priority::Normal,
                NodePriority::Low => Priority::Low,
            };
            entry.affinity_group = scheduler.get_affinity_group(node.use_workers.as_ref());
        }
    }

    cache
}

/// Precompute predecessor routing tables from the graph's successor/arg structure.
///
/// Returns three `num_nodes × num_nodes` tables:
/// - `pred_index_filter`: index range `[min, max)` within a predecessor's instances that a
///   successor reads. Used to skip dispatching to successors that don't read the completed instance.
/// - `pred_group_by`: the `group_by` divisor for grouped predecessors.
/// - `pred_succ_1to1_offset`: `indexes[0]` offset for 1:1 non-barrier single-index `$res` deps
///   with equal factor. Enables exact successor dispatch, eliminating `spin_wait` deadlocks.
pub(super) fn build_predecessor_tables(
    app_graph: &Graph,
) -> (
    Vec<Vec<Option<(usize, usize)>>>,
    Vec<Vec<Option<usize>>>,
    Vec<Vec<Option<isize>>>,
) {
    let num_nodes = app_graph.nodes.len();
    let mut filter: Vec<Vec<Option<(usize, usize)>>> = vec![vec![None; num_nodes]; num_nodes];
    let mut group_by: Vec<Vec<Option<usize>>> = vec![vec![None; num_nodes]; num_nodes];
    let mut succ_1to1_offset: Vec<Vec<Option<isize>>> = vec![vec![None; num_nodes]; num_nodes];

    for succ_node in &app_graph.nodes {
        let succ_id = succ_node.id as usize;
        let succ_factor = succ_node.factor;

        for arg in &succ_node.args {
            let Some(pred) = &arg.predecessor else { continue };
            let pred_id = pred.id as usize;
            let pred_factor = app_graph.nodes[pred_id].factor;

            if !pred.indexes.is_empty() {
                let min_idx = *pred.indexes.iter().min().unwrap() as usize;
                let max_idx = *pred.indexes.iter().max().unwrap() as usize;
                let range_len = max_idx - min_idx + 1;

                let should_filter = if pred.group_by.is_some() {
                    true // always filter when group_by present (needed for offset calculation)
                } else if range_len < pred_factor && range_len == pred.indexes.len() {
                    range_len == succ_factor
                } else {
                    false
                };

                if should_filter {
                    filter[succ_id][pred_id] = Some((min_idx, max_idx + 1));
                }
            }

            if let Some(gb) = pred.group_by {
                group_by[succ_id][pred_id] = Some(gb);
            }

            // 1:1 non-barrier single-index $res with equal factors: store offset so we can
            // fire the exact successor instance that reads this predecessor.
            if !arg.is_barrier()
                && pred.group_by.is_none()
                && pred.indexes.len() == 1
                && succ_factor == pred_factor
                && succ_factor > 1
            {
                succ_1to1_offset[succ_id][pred_id] = Some(pred.indexes[0]);
            }
        }
    }

    (filter, group_by, succ_1to1_offset)
}

/// Build the per-slot task counters and condition instance tracking.
///
/// Returns:
/// - `pending_tasks`: per-slot regular (non-condition, non-initial) task count.
/// - `pending_cond_tasks`: per-slot condition task count.
/// - `cond_instances_to_spawn`: generational packed counters for condition node spawn tracking.
pub(super) fn build_slot_counters(
    slots: usize,
    node_cache: &[NodeCacheEntry],
) -> (Vec<AtomicUsize>, Vec<AtomicUsize>, Vec<Vec<AtomicU64>>) {
    let total_tasks: usize = node_cache
        .iter()
        .filter(|nc| !nc.is_initial && !nc.is_condition)
        .map(|nc| nc.factor)
        .sum();
    let total_cond_tasks: usize = node_cache
        .iter()
        .filter(|nc| nc.is_condition)
        .map(|nc| nc.factor)
        .sum();

    let pending_tasks: Vec<AtomicUsize> = (0..slots)
        .map(|_| AtomicUsize::new(total_tasks))
        .collect();
    let pending_cond_tasks: Vec<AtomicUsize> = (0..slots)
        .map(|_| AtomicUsize::new(total_cond_tasks))
        .collect();

    // Packed (gen: u32, remaining_spawns: u32) — generation mismatch triggers lazy reinit.
    let cond_instances_to_spawn: Vec<Vec<AtomicU64>> = (0..slots)
        .map(|_| {
            node_cache
                .iter()
                .map(|nc| {
                    if nc.is_condition {
                        AtomicU64::new(crate::buffers::gen_pack(0, nc.factor as u32))
                    } else {
                        AtomicU64::new(crate::buffers::gen_pack(0, 0))
                    }
                })
                .collect()
        })
        .collect();

    (pending_tasks, pending_cond_tasks, cond_instances_to_spawn)
}
