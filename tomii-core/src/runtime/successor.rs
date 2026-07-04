//! Successor dependency-resolution helpers shared by the batch and worker paths.
//!
//! The main helpers (`decrement_and_collect_ready`, `push_ready_chunked`) are called from
//! both `batch_resolution::process_batch_inner` (system thread) and
//! `task_execution::worker_resolve_successors` (worker thread).  Keeping them here avoids
//! duplicating the logic and ensures both paths share identical dispatch semantics.
//!
//! Successor *enumeration* is a plain slice iteration over
//! [`super::successor_arena::SuccessorArena::edges_for`] — each [`SuccEdge`] carries the
//! pre-joined filter/group/1:1 routing data, so no collection step is needed.
//!
//! This module does **not** update any per-slot counters directly; that is `batch_resolution`
//! and `task_execution`.  It only delegates counter decrements to
//! `resolution_state::decrease_and_get_ready_into`.

use super::shared_data::SharedData;
use super::successor_arena::SuccEdge;
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
            arg.init_condition.as_ref().unwrap();
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

        let eval = init_condition.evaluate(result);
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
    let stale = super::arg_resolution::populate_cached_args_into(
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
    if stale {
        crate::debug::print_debug(|| {
            format!(
                "EVAL_COND stale: node={} index={} slot={}",
                node_info.id, node_info.index, node_info.slot
            )
        });
        return false;
    }

    // Execute condition function to get result
    let cond_result = (cond_cache.func_ptr)(&cond_args);

    let passed = node_cond.evaluate(&cond_result);
    crate::debug::print_debug(|| {
        format!(
            "EVAL_COND: node={} index={} slot={} args={:?} result={:?} passed={}",
            node_info.id, node_info.index, node_info.slot, cond_args, cond_result, passed
        )
    });
    // Evaluate result against expected value using operation
    passed
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

/// Decrement the dependency counter of the successor behind `edge` and collect any
/// now-ready instance indices into `ready` (cleared by the callee first).
///
/// The edge carries the pre-joined routing data (group divisor, 1:1 offset, factor), so
/// this is a pure counter operation — no table lookups.  Both the batch-resolution path
/// and the worker-resolution path call this, sharing identical decrement semantics.
/// The `bulk_count` parameter distinguishes the two callers:
/// - Batch path: always `1` (one completion per node in the batch).
/// - Worker path: `node_info.bulk_count` (bulk tasks complete N instances in one call).
#[inline]
pub(super) fn decrement_and_collect_ready(
    ctx: &super::shared_data::ResolveCtx<'_>,
    slot: usize,
    pred_index: usize,
    edge: &SuccEdge,
    bulk_count: usize,
    slot_gen: u32,
    ready: &mut Vec<usize>,
) {
    // When bulk_count > 1 the completing task represents N instances in one shot.
    // The 1:1 mapping uses pred_index (the bulk start), which would fire only
    // successor[start] — skipping the rest of the range.  Suppress 1:1 dispatch for
    // bulk completions; the full threshold scan in decrease_and_get_ready_into
    // handles it correctly.
    let specific_succ_idx = if bulk_count > 1 {
        None
    } else {
        edge.one_to_one_succ_idx(pred_index)
    };
    ctx.exec.resolution_state.decrease_and_get_ready_into(
        slot,
        edge.succ_id as usize,
        slot_gen,
        edge.pred_group(pred_index),
        bulk_count,
        specific_succ_idx,
        ready,
    );
}
