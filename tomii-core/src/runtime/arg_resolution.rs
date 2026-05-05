//! Argument materialisation: resolves `$ref`, `$res`, `$dep`, and runtime-injected args
//! for a task just before it is handed to the plugin function.
//!
//! The hot path is [`populate_cached_args_into`], which reads pre-built [`ArgCacheEntry`]
//! metadata to avoid per-task graph lookups.  For `$res` arguments it calls
//! [`spin_wait_for_result`] when the predecessor result is not yet visible — this handles the
//! race where threshold-based dispatch fires a successor before its specific predecessor has
//! stored its result.
//!
//! This module does **not** own scheduling or dependency-counter logic.  Its only shared-state
//! read is `node_results.get()`; it never writes shared state.

use super::shared_data::SharedData;
use crate::{buffers::*, graph_struct::*, IdType};
use std::sync::Arc;
use tomii_types::*;

use super::node_cache::{ArgCacheEntry, ResPredCache};

#[inline(always)]
fn process_buffer_refs(arg_vec: &mut [CmTypes], cache: &ArgCacheEntry, node_index: usize) {
    for (i, idx) in cache.buffer_ref_indexes.iter().enumerate() {
        arg_vec[*idx] = get_object_value(&cache.buffer_values[i], node_index);
    }
}

#[inline(always)]
fn process_runtime_refs(
    arg_vec: &mut [CmTypes],
    cache: &ArgCacheEntry,
    node_index: usize,
    workers: usize,
) {
    // Process both types of runtime refs in a single iteration if possible
    if cache.rt_idxs_indexes.len() == cache.rt_workers_indexes.len() {
        for (idx_idx, worker_idx) in cache
            .rt_idxs_indexes
            .iter()
            .zip(cache.rt_workers_indexes.iter())
        {
            arg_vec[*idx_idx] = CmTypes::Usize(node_index);
            arg_vec[*worker_idx] = CmTypes::Usize(workers);
        }
    } else {
        // Fall back to separate processing
        for idx in cache.rt_idxs_indexes.iter() {
            arg_vec[*idx] = CmTypes::Usize(node_index);
        }
        for idx in cache.rt_workers_indexes.iter() {
            arg_vec[*idx] = CmTypes::Usize(workers);
        }
    }
}

/// Patch only the instance-dependent slots of an already-filled arg buffer.
///
/// Called per-instance inside `execute_bulk_task` after the static template has been
/// `extend`ed into `buf` once in the prologue (Tier 1 hoist). Only overwrites
/// `buffer_ref_indexes`, `rt_idxs_indexes`, `rt_workers_indexes`, and `res_indexes`
/// positions — static slots (including `Any`/`AnyHeld` arcs) are left untouched.
///
/// Returns `true` if a stale-task was detected.
#[inline(always)]
#[allow(clippy::too_many_arguments)]
pub(super) fn populate_dynamic_args_into(
    buf: &mut Vec<CmTypes>,
    shared: &Arc<SharedData>,
    args_cache: &ArgCacheEntry,
    node_index: usize,
    slot: usize,
    pred_index: usize,
    exec_slot: usize,
    exec_gen: u32,
    workers: usize,
) -> bool {
    process_buffer_refs(buf, args_cache, node_index);
    process_runtime_refs(buf, args_cache, node_index, workers);

    let mut stale = false;
    for (res_idx, rp) in args_cache
        .res_indexes
        .iter()
        .zip(args_cache.res_predecessors.iter())
    {
        if stale {
            break;
        }
        let result_opt = collect_res_from_cache(
            rp, node_index, slot, pred_index, None, shared, exec_slot, exec_gen, &mut stale,
        );
        if let Some(mut result) = result_opt {
            if result.len() == 1 {
                buf[*res_idx] = result.remove(0);
            } else {
                buf.splice(*res_idx..*res_idx + 1, result);
            }
        }
    }
    stale
}

/// Populate args directly into a provided buffer, avoiding heap allocation.
///
/// Returns `true` if a stale-task was detected (slot generation changed mid-resolution).
/// The caller must drop the task without processing it or decrementing dependency counters.
#[inline(always)]
#[allow(clippy::too_many_arguments)]
pub(super) fn populate_cached_args_into(
    buf: &mut Vec<CmTypes>,
    shared: &Arc<SharedData>,
    args_cache: &ArgCacheEntry,
    _node_id: IdType,
    node_index: usize,
    slot: usize,
    pred_index: usize,
    exec_slot: usize,
    exec_gen: u32,
) -> bool {
    buf.extend(args_cache.args.iter().cloned());

    if args_cache.buffer_ref_indexes.is_empty()
        && args_cache.rt_idxs_indexes.is_empty()
        && args_cache.rt_workers_indexes.is_empty()
        && args_cache.res_indexes.is_empty()
    {
        return false;
    }

    let workers = if !args_cache.rt_workers_indexes.is_empty() {
        shared.config.workers
    } else {
        0
    };

    process_buffer_refs(buf, args_cache, node_index);
    process_runtime_refs(buf, args_cache, node_index, workers);

    let mut stale = false;
    for (res_idx, rp) in args_cache
        .res_indexes
        .iter()
        .zip(args_cache.res_predecessors.iter())
    {
        if stale {
            break;
        }
        let result_opt = collect_res_from_cache(
            rp, node_index, slot, pred_index, None, shared, exec_slot, exec_gen, &mut stale,
        );
        if let Some(mut result) = result_opt {
            if result.len() == 1 {
                buf[*res_idx] = result.remove(0);
            } else {
                buf.splice(*res_idx..*res_idx + 1, result);
            }
        }
    }
    stale
}

#[inline]
pub(super) fn parse_args(
    shared: &Arc<SharedData>,
    args: &[Arg],
    node_index: usize,
    slot: usize,
    pred_index: usize,
    custom_res: Option<&CmTypes>,
) -> Vec<CmTypes> {
    // Pre-allocate capacity to avoid reallocations
    let mut arg_vec = Vec::with_capacity(args.len());
    for arg in args.iter() {
        // continue if arg is a condition
        if arg.is_condition() {
            continue;
        }

        let result_opt = collect_arg_result(
            arg,
            0,
            node_index,
            0,
            slot,
            pred_index,
            custom_res,
            shared,
            usize::MAX,
            0,
            &mut false,
        );
        if let Some(result) = result_opt {
            arg_vec.extend(result);
        }
    }
    arg_vec
}

#[inline(always)]
fn handle_special_ref(obj_id: usize, node_index: usize, workers: usize) -> Option<Vec<CmTypes>> {
    match obj_id {
        0 => Some(vec![CmTypes::Usize(node_index)]),
        1 => Some(vec![CmTypes::Usize(workers)]),
        _ => None,
    }
}

#[inline(always)]
fn get_object_value(obj_vec: &[CmTypes], node_index: usize) -> CmTypes {
    if obj_vec.len() > 1 {
        obj_vec[node_index % obj_vec.len()].clone()
    } else {
        obj_vec[0].clone()
    }
}

/// Spin-wait for a predecessor result that is temporarily absent because its
/// producer task is still executing on a parallel worker.
///
/// This handles the race where the threshold-based dispatcher fires a successor
/// (e.g. copy_op[0]) after *any* predecessor completes, even though the specific
/// predecessor instance that this successor reads (e.g. gen_b[0]) has not yet
/// stored its result.  With a single worker the ordering is serial and the race
/// cannot occur; with multiple workers it can.
///
/// Returns `Some(result)` once the result is visible, or `None` if the slot
/// generation changes (slot recycled → task is stale and should be dropped).
/// When returning `None`, `*stale` is set to `true` to short-circuit remaining
/// arg collection in the caller.
#[cold]
#[inline(never)]
fn spin_wait_for_result(
    shared: &Arc<SharedData>,
    node_info: &NodeInfo,
    exec_slot: usize,
    exec_gen: u32,
    stale: &mut bool,
) -> Option<tomii_types::CmTypes> {
    use std::sync::atomic::Ordering;
    let mut spin_count: u32 = 0;
    loop {
        if let Some(result) = shared.exec.node_results.get(node_info) {
            return Some(result);
        }
        if shared.shutdown_flag.load(Ordering::Acquire) {
            *stale = true;
            return None;
        }
        if exec_slot != usize::MAX {
            let current_gen = shared.slot_data.generation[exec_slot].load(Ordering::Acquire) as u32;
            if exec_gen != current_gen {
                if !shared.config.single_slot_mode {
                    *stale = true;
                }
                return None;
            }
        }
        spin_count += 1;
        let sw = &shared.config.spin_wait;
        if spin_count < sw.spin_iters {
            std::hint::spin_loop();
        } else if spin_count < sw.yield_iters {
            std::thread::yield_now();
        } else {
            std::thread::park_timeout(std::time::Duration::from_nanos(sw.park_ns));
        }
    }
}

/// Shared core of the `$res` resolution path.
///
/// Both [`collect_arg_result`] (live-graph path) and [`collect_res_from_cache`]
/// (precomputed-cache path) converge here after resolving their metadata into a
/// common form.  All three dispatch cases live in exactly one place.
///
/// # Parameters
/// - `res_node_id`: the predecessor node whose result we are reading
/// - `indexes`: the index spec from the graph edge (`Predecessor::indexes`)
/// - `node_index` / `pred_index`: instance indices for this and the triggering predecessor
/// - `node_factor`: parallelism factor of the successor (current) node
/// - `pred_factor`: parallelism factor of the predecessor node
/// - `node_group_size`: `group_size` of the successor node (for grouped barrier edges)
/// - `pred_eff_group_size`: effective group size of the predecessor (pre-resolved by caller)
#[inline(always)]
#[allow(clippy::too_many_arguments)]
fn fetch_res_results(
    res_node_id: IdType,
    indexes: &[isize],
    node_index: usize,
    pred_index: usize,
    node_factor: usize,
    pred_factor: usize,
    node_group_size: Option<usize>,
    pred_eff_group_size: Option<usize>,
    slot: usize,
    shared: &Arc<SharedData>,
    exec_slot: usize,
    exec_gen: u32,
    stale: &mut bool,
) -> Option<Vec<CmTypes>> {
    if indexes.is_empty() {
        return None;
    }

    // Single explicit index: use the declared index, not pred_index.
    if indexes.len() == 1 {
        let dep_idx = if let Some(ngs) = node_group_size {
            let symbol = node_index / ngs;
            let eff_gs = pred_eff_group_size.unwrap_or(pred_factor);
            symbol * eff_gs + indexes[0] as usize
        } else {
            find_pred_index(node_index, indexes[0], pred_factor)
        };
        let node_info = NodeInfo::new(res_node_id, slot, dep_idx, 0);
        if let Some(result) = shared.exec.node_results.get(&node_info) {
            return Some(vec![result]);
        }
        return spin_wait_for_result(shared, &node_info, exec_slot, exec_gen, stale)
            .map(|r| vec![r]);
    }

    // 1:1 mapping: each successor instance reads one predecessor result via pred_index.
    if indexes.len() > 1 && indexes.len() == node_factor {
        let node_info = NodeInfo::new(res_node_id, slot, pred_index % pred_factor, 0);
        if let Some(result) = shared.exec.node_results.get(&node_info) {
            return Some(vec![result]);
        }
        return spin_wait_for_result(shared, &node_info, exec_slot, exec_gen, stale)
            .map(|r| vec![r]);
    }

    // Collect-all path: gather every explicitly listed predecessor index.
    let mut result_vec = Vec::with_capacity(indexes.len());
    for &pred_idx in indexes.iter() {
        let dep_idx = find_pred_index(node_index, pred_idx, pred_factor);
        let node_info = NodeInfo::new(res_node_id, slot, dep_idx, 0);
        if let Some(result) = shared.exec.node_results.get(&node_info) {
            result_vec.push(result);
        } else {
            match spin_wait_for_result(shared, &node_info, exec_slot, exec_gen, stale) {
                Some(result) => result_vec.push(result),
                None => return None,
            }
        }
    }
    if result_vec.len() == indexes.len() {
        Some(result_vec)
    } else {
        None
    }
}

/// Hot-path variant of [`collect_arg_result`] for `$res`/`$dep` arguments.
///
/// Uses pre-resolved [`ResPredCache`] metadata (built once at startup) instead of
/// reading `shared.graph.nodes[...]` on every task dispatch.
#[inline(always)]
#[allow(clippy::too_many_arguments)]
fn collect_res_from_cache(
    rp: &ResPredCache,
    node_index: usize,
    slot: usize,
    pred_index: usize,
    custom_res: Option<&CmTypes>,
    shared: &Arc<SharedData>,
    exec_slot: usize,
    exec_gen: u32,
    stale: &mut bool,
) -> Option<Vec<CmTypes>> {
    if rp.is_dep {
        return Some(vec![CmTypes::None]);
    }

    // Short-circuit: if a previous arg already detected stale, skip remaining
    if *stale {
        return None;
    }

    if let Some(custom) = custom_res {
        return Some(vec![(*custom).clone()]);
    }

    let pred_eff_gs = rp
        .pred_group_size
        .or_else(|| shared.graph_cache.pred_group_by[rp.node_id as usize][rp.res_node_id as usize]);

    fetch_res_results(
        rp.res_node_id,
        &rp.indexes,
        node_index,
        pred_index,
        rp.node_factor,
        rp.pred_factor,
        rp.node_group_size,
        pred_eff_gs,
        slot,
        shared,
        exec_slot,
        exec_gen,
        stale,
    )
}

#[inline]
#[allow(clippy::too_many_arguments)]
pub(super) fn collect_arg_result(
    arg: &Arg,
    node_id: IdType,
    node_index: usize,
    node_factor: usize,
    slot: usize,
    pred_index: usize,
    custom_res: Option<&CmTypes>,
    shared: &Arc<SharedData>,
    exec_slot: usize,
    exec_gen: u32,
    stale: &mut bool,
) -> Option<Vec<CmTypes>> {
    match &arg.type_ {
        CmTypes::Ref(obj_id) => {
            let obj_id = *obj_id;
            if let Some(result) = handle_special_ref(obj_id, node_index, shared.config.workers) {
                return Some(result);
            }

            let obj_vec = &shared.graph_cache.init_objects[obj_id];
            Some(vec![get_object_value(obj_vec, node_index)])
        }
        CmTypes::Dep(_) => {
            // Ordering-only dep: no result fetch needed, provide None directly.
            // The predecessor edge is tracked for scheduling purposes but the
            // result value is not consumed by this successor.
            Some(vec![CmTypes::None])
        }
        CmTypes::Res(res_node_id) => {
            // Short-circuit: if a previous arg already detected stale, skip remaining
            if *stale {
                return None;
            }

            if let Some(custom_res) = custom_res {
                return Some(vec![(*custom_res).clone()]);
            }

            let predecessor = arg.predecessor.as_ref()?;

            let res_node = &shared.graph.nodes[*res_node_id];
            let pred_factor = res_node.factor;
            let current_node = &shared.graph.nodes[node_id as usize];

            // Pre-resolve effective group size for grouped barrier edges.
            let pred_eff_gs = res_node
                .group_size
                .or_else(|| shared.graph_cache.pred_group_by[node_id as usize][*res_node_id]);

            fetch_res_results(
                *res_node_id as IdType,
                &predecessor.indexes,
                node_index,
                pred_index,
                node_factor,
                pred_factor,
                current_node.group_size,
                pred_eff_gs,
                slot,
                shared,
                exec_slot,
                exec_gen,
                stale,
            )
        }
        CmTypes::Barrier(_) => None,
        _ => Some(vec![arg.type_.clone()]),
    }
}
