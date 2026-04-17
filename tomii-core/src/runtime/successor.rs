use super::shared_data::SharedData;
use crate::{buffers::*, IdType};
use std::sync::Arc;

/// When a barrier node's instances all become ready simultaneously, this helper
/// creates `min(ready.len(), num_workers)` bulk `NodeInfo`s instead of one per instance.
/// Requires that ready indices form a contiguous range (guaranteed for single-group barriers).
/// Falls back to individual dispatch for small fan-outs or non-contiguous indices.
pub(super) fn push_ready_chunked(
    ready: &[usize],
    succ_id: IdType,
    slot: usize,
    pred_index: usize,
    num_workers: usize,
    coalesce: bool,
    sched: &mut Vec<NodeInfo>,
) {
    if ready.is_empty() {
        return;
    }
    let start = ready[0];
    let contiguous = ready.iter().enumerate().all(|(i, &r)| r == start + i);

    if coalesce && contiguous && num_workers > 0 && ready.len() > num_workers {
        // Chunk into num_workers bulk tasks
        let total = ready.len();
        let num_chunks = num_workers;
        let base = total / num_chunks;
        let extra = total % num_chunks;
        let mut offset = start;
        for c in 0..num_chunks {
            let count = base + if c < extra { 1 } else { 0 };
            let mut ni = NodeInfo::new(succ_id, slot, offset, pred_index);
            ni.bulk_count = count;
            sched.push(ni);
            offset += count;
        }
    } else {
        for &idx in ready {
            sched.push(NodeInfo::new(succ_id, slot, idx, pred_index));
        }
    }
}

#[inline]
pub(super) fn conditions_met(
    shared: &Arc<SharedData>,
    node_info: &NodeInfo,
    arg_indexes: &Vec<usize>,
) -> bool {
    let node = &shared.graph.nodes[node_info.id as usize];
    let mut is_ready = true;

    for arg_idx in arg_indexes {
        let arg = &node.args[*arg_idx];
        let init_condition: &crate::graph_struct::InitCondition =
            &arg.init_condition.as_ref().unwrap();
        // We assume condition has a single predecessor
        let node_factor = shared.graph.nodes[node_info.id as usize].factor;
        let result = &super::arg_resolution::collect_arg_result(
            arg,
            node_info.id,
            node_info.index,
            node_factor,
            node_info.slot,
            node_info.pred_index,
            None,
            shared,
            usize::MAX,
            0,
            &mut false,
        )
        .unwrap()[0];

        let eval = init_condition.evaluate(&result);
        if !eval {
            is_ready = false;
            break;
        }
    }
    is_ready
}

/// Evaluate node-level condition (new format)
/// Returns true if condition passes (node should be scheduled)
#[inline]
pub(super) fn evaluate_node_condition(
    shared: &Arc<SharedData>,
    node_info: &NodeInfo,
    cond_cache: &super::node_cache::NodeConditionCache,
    node_cond: &crate::graph_struct::NodeCondition,
) -> bool {
    // Build condition args using cached arg data
    let mut cond_args = Vec::with_capacity(cond_cache.arg_cache.args.len());
    let _ = super::arg_resolution::populate_cached_args_into(
        &mut cond_args,
        shared,
        &cond_cache.arg_cache,
        node_info.id,
        node_info.index,
        node_info.slot,
        node_info.pred_index,
        usize::MAX,
        0,
    );

    // Execute condition function to get result
    let cond_result = (cond_cache.func_ptr)(&cond_args);

    // Evaluate result against expected value using operation
    node_cond.evaluate(&cond_result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_chunked(ready: &[usize], num_workers: usize, coalesce: bool) -> Vec<NodeInfo> {
        let mut out = Vec::new();
        push_ready_chunked(ready, 1, 0, 0, num_workers, coalesce, &mut out);
        out
    }

    #[test]
    fn test_empty_ready_produces_no_output() {
        assert!(run_chunked(&[], 4, true).is_empty());
    }

    #[test]
    fn test_non_contiguous_always_individual() {
        // Non-contiguous indices → individual dispatch even with coalesce=true
        let ready = vec![0, 2, 5];
        let out = run_chunked(&ready, 2, true);
        assert_eq!(out.len(), 3);
        let indices: Vec<usize> = out.iter().map(|ni| ni.index).collect();
        assert_eq!(indices, vec![0, 2, 5]);
        assert!(out.iter().all(|ni| ni.bulk_count == 1));
    }

    #[test]
    fn test_small_contiguous_below_worker_count_individual() {
        // len <= num_workers → no chunking even when contiguous and coalesce=true
        let ready = vec![0, 1, 2, 3];
        let out = run_chunked(&ready, 4, true);
        assert_eq!(out.len(), 4);
        assert!(out.iter().all(|ni| ni.bulk_count == 1));
    }

    #[test]
    fn test_coalesce_false_always_individual() {
        let ready: Vec<usize> = (0..16).collect();
        let out = run_chunked(&ready, 4, false);
        assert_eq!(out.len(), 16);
        assert!(out.iter().all(|ni| ni.bulk_count == 1));
    }

    #[test]
    fn test_coalesce_true_chunks_into_worker_count() {
        // 16 ready, 4 workers → 4 bulk chunks
        let ready: Vec<usize> = (0..16).collect();
        let out = run_chunked(&ready, 4, true);
        assert_eq!(out.len(), 4);
        let total: usize = out.iter().map(|ni| ni.bulk_count).sum();
        assert_eq!(total, 16);
    }

    #[test]
    fn test_coalesce_bulk_count_sum_equals_total() {
        // Remainder distribution: 10 tasks / 3 workers → chunks of 4, 3, 3
        let ready: Vec<usize> = (0..10).collect();
        let out = run_chunked(&ready, 3, true);
        assert_eq!(out.len(), 3);
        let total: usize = out.iter().map(|ni| ni.bulk_count).sum();
        assert_eq!(total, 10);
    }

    #[test]
    fn test_coalesce_chunks_cover_all_indices_contiguously() {
        // Chunks must cover exactly [start..start+total) with no gaps or overlaps
        let ready: Vec<usize> = (5..21).collect(); // 16 items starting at 5
        let out = run_chunked(&ready, 4, true);
        let mut covered: Vec<usize> = Vec::new();
        for ni in &out {
            for k in 0..ni.bulk_count {
                covered.push(ni.index + k);
            }
        }
        covered.sort();
        let expected: Vec<usize> = (5..21).collect();
        assert_eq!(covered, expected);
    }
}

/// Compute the specific successor-instance index for 1:1 non-barrier predecessor→successor
/// dependencies.
///
/// When a predecessor and its successor have the same factor and the dependency is not a
/// barrier, each predecessor instance `i` should fire exactly one specific successor instance
/// `j` (where `j = (i - offset).rem_euclid(factor)`).  This guarantees the successor's result
/// is already stored when the successor runs its `$res` argument fetch, removing the need for
/// a `spin_wait`.
///
/// Returns `Some(j)` when the 1:1 mapping applies; `None` for fanout / barrier dependencies.
#[inline]
pub(super) fn compute_1to1_succ_idx(
    shared: &SharedData,
    pred_node_id: IdType,
    pred_index: usize,
    succ_node_id: usize,
) -> Option<usize> {
    shared
        .graph_cache
        .pred_succ_1to1_offset
        .get(succ_node_id)
        .and_then(|v| v.get(pred_node_id as usize))
        .and_then(|o| *o)
        .map(|k| {
            let f = shared.graph_cache.node_cache[succ_node_id].factor;
            ((pred_index as isize - k).rem_euclid(f as isize)) as usize
        })
}

/// Decrement the dependency counter of `succ_node_id` and collect any now-ready instance
/// indices into `ready`.
///
/// Combines `compute_1to1_succ_idx` with `decrease_and_get_ready_into` so both the
/// batch-resolution path and the worker-resolution path share the same decrement semantics.
/// The `bulk_count` parameter distinguishes the two callers:
/// - Batch path: always `1` (one completion per node in the batch).
/// - Worker path: `node_info.bulk_count` (bulk tasks complete N instances in one call).
#[inline]
pub(super) fn decrement_and_collect_ready(
    shared: &Arc<SharedData>,
    slot: usize,
    pred_node_id: IdType,
    pred_index: usize,
    succ_node_id: usize,
    pred_group: Option<usize>,
    bulk_count: usize,
    slot_gen: u32,
    ready: &mut Vec<usize>,
) {
    let specific_succ_idx = compute_1to1_succ_idx(shared, pred_node_id, pred_index, succ_node_id);
    shared.exec.resolution_state.decrease_and_get_ready_into(
        slot,
        succ_node_id,
        slot_gen,
        pred_group,
        bulk_count,
        specific_succ_idx,
        ready,
    );
}

/// Collect successor descriptors for `node_info`, appending into `out` (cleared first).
/// Avoids a heap allocation on the hot path when the caller supplies a reusable buffer.
#[inline]
pub(super) fn collect_successors_for_node_into(
    shared: &Arc<SharedData>,
    node_info: &NodeInfo,
    out: &mut Vec<(NodeInfo, bool, IdType, Option<usize>)>,
) {
    out.clear();

    let node_id_usize = node_info.id as usize;

    // Get successor list for this node (immutable, pre-computed)
    let successors: &Vec<IdType> = {
        if node_id_usize >= shared.graph.successors.len() {
            &Vec::new()
        } else {
            &shared.graph.successors[node_id_usize]
        }
    };

    // Collect info for each successor without locks
    for succ_id in successors {
        let succ_id = *succ_id;
        let succ_id_usize = succ_id as usize;

        // Predecessor index range filter: skip if this predecessor instance is outside
        // the declared index range for this successor
        if let Some(Some((start, end))) = shared
            .graph_cache
            .pred_index_filter
            .get(succ_id_usize)
            .and_then(|v| v.get(node_id_usize))
        {
            if node_info.index < *start || node_info.index >= *end {
                continue; // Predecessor instance outside declared range
            }
        }

        let succ_cache = &shared.graph_cache.node_cache[succ_id_usize];

        // Use pre-computed flag for lock-free check
        let has_condition = succ_cache.is_condition;

        // Compute predecessor group for group_by barriers
        let pred_group: Option<usize> = {
            if let Some(Some(gb)) = shared
                .graph_cache
                .pred_group_by
                .get(succ_id_usize)
                .and_then(|v| v.get(node_id_usize))
            {
                // Compute relative index within the declared range
                let range_start = shared
                    .graph_cache
                    .pred_index_filter
                    .get(succ_id_usize)
                    .and_then(|v| v.get(node_id_usize))
                    .and_then(|f| f.map(|(s, _)| s))
                    .unwrap_or(0);
                let relative_idx = node_info.index - range_start;
                Some(relative_idx / gb)
            } else {
                None // No group_by → global decrement
            }
        };

        // Determine which indices of the successor to create.
        let succ_indexes = {
            if pred_group.is_some() {
                // Group-based dependency: placeholder entry (index 0) for decrement
                vec![0]
            } else if node_info.id == 0 {
                // $network node: 1:1 index mapping for pred_index_filter routing
                vec![node_info.index]
            } else {
                // Single entry per (successor, pred_group) pair
                vec![0]
            }
        };

        // Add successor node info for each instance
        for succ_index in succ_indexes {
            let succ_info = NodeInfo::new(succ_id, node_info.slot, succ_index, node_info.index);
            out.push((succ_info, has_condition, succ_id, pred_group));
        }
    }
}
