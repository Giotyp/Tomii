//! Graph compilation helpers: node cache construction and predecessor routing tables.
//!
//! `build_node_cache` and `build_predecessor_tables` are `pub(crate)` so that
//! `graph_gen::GraphSpec::compile()` can call them directly.  `build_slot_counters`
//! remains `pub(super)` because it depends on the runtime `slots` parameter.
//! Pure analysis — no threading, no shared state writes.
use super::node_cache::{node_cache_entry, NodeCacheEntry, ResPredCache};
use crate::graph::*;
use crate::scheduler::SchedulerImpl;
use crate::IdType;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use tomii_types::CmTypes;

/// Build the node cache from graph nodes, computing all pre-derived flags.
///
/// Sets: `successor_count`, `worker_resolvable`, `needs_result_store`,
/// `priority`, and `affinity_group` in addition to the base cache entry.
pub(crate) fn build_node_cache(
    app_graph: &Graph,
    init_objects: &[Vec<tomii_types::CmTypes>],
    scheduler: &SchedulerImpl,
) -> Vec<NodeCacheEntry> {
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
    // Must check both main args AND condition args: a node used only in a
    // successor's condition expression (e.g. classify read by smooth/handle_anomaly)
    // still requires its result to be stored.
    for (node_id, cache_entry) in cache.iter_mut().enumerate() {
        let has_res_consumer = node_id < app_graph.successors.len()
            && app_graph.successors[node_id].iter().any(|&succ_id| {
                let succ_node = &app_graph.nodes[succ_id as usize];
                let main_reads = succ_node.args.iter().any(|arg| {
                    arg.type_.is_result()
                        && arg
                            .predecessor
                            .as_ref()
                            .is_some_and(|p| p.id == node_id as IdType)
                });
                let cond_reads = succ_node.condition.as_ref().is_some_and(|cond| {
                    cond.args.iter().any(|arg| {
                        arg.type_.is_result()
                            && arg
                                .predecessor
                                .as_ref()
                                .is_some_and(|p| p.id == node_id as IdType)
                    })
                });
                main_reads || cond_reads
            });
        cache_entry.needs_result_store = has_res_consumer;
    }

    // is_fanout_bulk — true when the node is eligible for 1:1 fanout bulk dispatch (Upgrade 5).
    // Eligibility: single $res predecessor with equal factor > 1, worker_resolvable,
    // not a condition/network node, no $barrier args.
    for (node_id, cache_entry) in cache.iter_mut().enumerate() {
        if !cache_entry.worker_resolvable
            || cache_entry.is_condition
            || cache_entry.name == "$network"
        {
            continue;
        }
        let node = &app_graph.nodes[node_id];
        if node.args.iter().any(|a| a.is_barrier()) {
            continue;
        }
        let res_args: Vec<_> = node
            .args
            .iter()
            .filter(|a| a.type_.is_result())
            .collect();
        if res_args.len() == 1 {
            if let Some(pred) = res_args[0].predecessor.as_ref() {
                let pred_factor = app_graph.nodes[pred.id as usize].factor;
                if pred_factor == node.factor && node.factor > 1 {
                    cache_entry.is_fanout_bulk = true;
                }
            }
        }
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

    // res_predecessors — pre-resolve predecessor/node metadata for the populate_cached_args_into
    // hot path.  Eliminates `shared.graph.nodes[pred_id]` lookups during task execution.
    // Both main arg_cache and condition arg_cache are populated here so the fast path is
    // always available regardless of which cache path is taken.
    for (node_id, cache_entry) in cache.iter_mut().enumerate() {
        let node = &app_graph.nodes[node_id];

        // Helper closure: build ResPredCache entries from an arg slice.
        // `skip_cond` mirrors the `skip_conditions` flag used in build_arg_cache.
        let build_res_preds =
            |args: &[crate::graph_struct::Arg], skip_cond: bool| -> Vec<ResPredCache> {
                let mut v = Vec::new();
                for arg in args {
                    if skip_cond && arg.is_condition() {
                        continue;
                    }
                    match &arg.type_ {
                        CmTypes::Res(res_nid) => {
                            let pred_node = &app_graph.nodes[*res_nid];
                            v.push(ResPredCache {
                                node_id: node_id as IdType,
                                res_node_id: *res_nid as IdType,
                                indexes: arg
                                    .predecessor
                                    .as_ref()
                                    .map_or_else(Vec::new, |p| p.indexes.clone()),
                                pred_factor: pred_node.factor,
                                pred_group_size: pred_node.group_size,
                                node_group_size: node.group_size,
                                node_factor: node.factor,
                                is_dep: false,
                            });
                        }
                        CmTypes::Dep(res_nid) => {
                            v.push(ResPredCache {
                                node_id: node_id as IdType,
                                res_node_id: *res_nid as IdType,
                                indexes: Vec::new(),
                                pred_factor: 0,
                                pred_group_size: None,
                                node_group_size: None,
                                node_factor: node.factor,
                                is_dep: true,
                            });
                        }
                        _ => {}
                    }
                }
                v
            };

        let main_res_preds = build_res_preds(&node.args, true);
        let cond_res_preds = node
            .condition
            .as_ref()
            .map(|cond| build_res_preds(&cond.args, false));

        cache_entry.arg_cache.res_predecessors = main_res_preds;
        if let (Some(nc), Some(cp)) = (cache_entry.node_condition.as_mut(), cond_res_preds) {
            nc.arg_cache.res_predecessors = cp;
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
#[allow(clippy::type_complexity)]
pub(crate) fn build_predecessor_tables(
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
            let Some(pred) = &arg.predecessor else {
                continue;
            };
            let pred_id = pred.id as usize;
            let pred_factor = app_graph.nodes[pred_id].factor;

            if let Some(range) = pred.index_filter(pred_factor, succ_factor) {
                filter[succ_id][pred_id] = Some(range);
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
/// - `fanout_bulk_arrived`: generational packed arrived counters for fanout-bulk dispatch.
pub(super) fn build_slot_counters(
    slots: usize,
    node_cache: &[NodeCacheEntry],
) -> (
    Vec<AtomicUsize>,
    Vec<AtomicUsize>,
    Vec<Vec<AtomicU64>>,
    Vec<Vec<AtomicU64>>,
) {
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

    let pending_tasks: Vec<AtomicUsize> =
        (0..slots).map(|_| AtomicUsize::new(total_tasks)).collect();
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

    // Packed (gen: u32, arrived_count: u32) — for fanout-bulk arrival tracking.
    // All counters start at 0 (no arrivals in generation 0).
    let fanout_bulk_arrived: Vec<Vec<AtomicU64>> = (0..slots)
        .map(|_| {
            node_cache
                .iter()
                .map(|_| AtomicU64::new(crate::buffers::gen_pack(0, 0)))
                .collect()
        })
        .collect();

    (
        pending_tasks,
        pending_cond_tasks,
        cond_instances_to_spawn,
        fanout_bulk_arrived,
    )
}
