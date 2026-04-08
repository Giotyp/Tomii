use super::shared_data::SharedData;
use super::thread_locals::WORKER_STATE;
use crate::{buffers::*, graph_struct::*, IdType};
use std::sync::Arc;
use synstream_types::*;

use super::node_cache::{ArgCacheEntry, ResPredCache};


#[inline(always)]
fn process_buffer_refs(arg_vec: &mut Vec<CmTypes>, cache: &ArgCacheEntry, node_index: usize) {
    for (i, idx) in cache.buffer_ref_indexes.iter().enumerate() {
        arg_vec[*idx] = get_object_value(&cache.buffer_values[i], node_index);
    }
}

#[inline(always)]
fn process_runtime_refs(
    arg_vec: &mut Vec<CmTypes>,
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

/// Populate args directly into a provided buffer, avoiding heap allocation.
#[inline(always)]
pub(super) fn populate_cached_args_into(
    buf: &mut Vec<CmTypes>,
    shared: &Arc<SharedData>,
    args_cache: &ArgCacheEntry,
    _node_id: IdType,
    node_index: usize,
    slot: usize,
    pred_index: usize,
) {
    buf.extend(args_cache.args.iter().cloned());

    if args_cache.buffer_ref_indexes.is_empty()
        && args_cache.rt_idxs_indexes.is_empty()
        && args_cache.rt_workers_indexes.is_empty()
        && args_cache.res_indexes.is_empty()
    {
        return;
    }

    let workers = if !args_cache.rt_workers_indexes.is_empty() {
        shared.config.workers
    } else {
        0
    };

    process_buffer_refs(buf, args_cache, node_index);
    process_runtime_refs(buf, args_cache, node_index, workers);

    for (res_idx, rp) in args_cache
        .res_indexes
        .iter()
        .zip(args_cache.res_predecessors.iter())
    {
        let result_opt = collect_res_from_cache(rp, node_index, slot, pred_index, None, shared);
        if let Some(mut result) = result_opt {
            if result.len() == 1 {
                buf[*res_idx] = result.remove(0);
            } else {
                buf.splice(*res_idx..*res_idx + 1, result);
            }
        }
    }
}

#[inline]
pub(super) fn parse_args(
    shared: &Arc<SharedData>,
    args: &Vec<Arg>,
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

        let result_opt =
            collect_arg_result(arg, 0, node_index, 0, slot, pred_index, custom_res, shared);
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
#[cold]
#[inline(never)]
fn spin_wait_for_result(
    shared: &Arc<SharedData>,
    node_info: &NodeInfo,
) -> Option<synstream_types::CmTypes> {
    use std::sync::atomic::Ordering;
    let mut spin_count: u32 = 0;
    loop {
        if let Some(result) = shared.exec.node_results.get(node_info) {
            return Some(result);
        }
        let (exec_slot, exec_gen) = WORKER_STATE.with(|ws| {
            let ws = ws.borrow();
            (ws.executing_slot, ws.executing_gen)
        });
        if exec_slot != usize::MAX {
            let current_gen =
                shared.slot_data.generation[exec_slot].load(Ordering::Acquire) as u32;
            if exec_gen != current_gen {
                if !shared.config.single_slot_mode {
                    WORKER_STATE.with(|ws| ws.borrow_mut().stale_task_detected = true);
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

/// Hot-path variant of [`collect_arg_result`] for `$res`/`$dep` arguments.
///
/// Uses pre-resolved [`ResPredCache`] metadata (built once at startup) instead of reading
/// `shared.graph.nodes[...]` on every task dispatch.  Mirrors the `Res`/`Dep` branches of
/// `collect_arg_result` exactly — any logic change there must be reflected here too.
#[inline(always)]
fn collect_res_from_cache(
    rp: &ResPredCache,
    node_index: usize,
    slot: usize,
    pred_index: usize,
    custom_res: Option<&CmTypes>,
    shared: &Arc<SharedData>,
) -> Option<Vec<CmTypes>> {
    if rp.is_dep {
        return Some(vec![CmTypes::None]);
    }

    // Short-circuit: if a previous arg already detected stale, skip remaining
    if !shared.config.single_slot_mode && WORKER_STATE.with(|ws| ws.borrow().stale_task_detected) {
        return None;
    }

    if let Some(custom) = custom_res {
        return Some(vec![(*custom).clone()]);
    }

    if rp.indexes.is_empty() {
        return None;
    }

    // Single explicit index: use the declared index, not pred_index.
    if rp.indexes.len() == 1 {
        let dep_idx = if let Some(ngs) = rp.node_group_size {
            let symbol = node_index / ngs;
            let pred_eff_gs = rp.pred_group_size.unwrap_or_else(|| {
                shared.graph_cache.pred_group_by[rp.node_id as usize][rp.res_node_id as usize]
                    .unwrap_or(rp.pred_factor)
            });
            let offset = rp.indexes[0] as usize;
            symbol * pred_eff_gs + offset
        } else {
            find_pred_index(node_index, rp.indexes[0], rp.pred_factor)
        };
        let node_info = NodeInfo::new(rp.res_node_id, slot, dep_idx, 0);
        if let Some(result) = shared.exec.node_results.get(&node_info) {
            return Some(vec![result]);
        }
        return match spin_wait_for_result(shared, &node_info) {
            Some(result) => Some(vec![result]),
            None => None,
        };
    }

    // 1:1 mapping: each instance reads exactly one predecessor result via pred_index.
    if rp.indexes.len() > 1 && rp.indexes.len() == rp.node_factor {
        let node_info = NodeInfo::new(rp.res_node_id, slot, pred_index % rp.pred_factor, 0);
        if let Some(result) = shared.exec.node_results.get(&node_info) {
            return Some(vec![result]);
        }
        return match spin_wait_for_result(shared, &node_info) {
            Some(result) => Some(vec![result]),
            None => None,
        };
    }

    // Collect-all path: factor != indexes.len() (e.g., write_res)
    let mut indices = Vec::with_capacity(rp.indexes.len());
    for &pred_idx in rp.indexes.iter() {
        indices.push(find_pred_index(node_index, pred_idx, rp.pred_factor));
    }
    let mut result_vec = Vec::with_capacity(indices.len());
    for dep_idx in indices.iter() {
        let node_info = NodeInfo::new(rp.res_node_id, slot, *dep_idx, 0);
        if let Some(result) = shared.exec.node_results.get(&node_info) {
            result_vec.push(result);
        } else {
            match spin_wait_for_result(shared, &node_info) {
                Some(result) => result_vec.push(result),
                None => return None,
            }
        }
    }
    if result_vec.len() == indices.len() {
        return Some(result_vec);
    }
    None
}

#[inline]
pub(super) fn collect_arg_result(
    arg: &Arg,
    node_id: IdType,
    node_index: usize,
    node_factor: usize,
    slot: usize,
    pred_index: usize,
    custom_res: Option<&CmTypes>,
    shared: &Arc<SharedData>,
) -> Option<Vec<CmTypes>> {
    match &arg.type_ {
        CmTypes::Ref(obj_id) => {
            let obj_id = *obj_id;
            if let Some(result) = handle_special_ref(obj_id, node_index, shared.config.workers) {
                return Some(result);
            }

            let obj_vec = &shared.graph_cache.init_objects[obj_id as usize];
            Some(vec![get_object_value(obj_vec, node_index)])
        }
        CmTypes::Dep(_) => {
            // Ordering-only dep: no result fetch needed, provide None directly.
            // The predecessor edge is tracked for scheduling purposes but the
            // result value is not consumed by this successor.
            return Some(vec![CmTypes::None]);
        }
        CmTypes::Res(res_node_id) => {
            // Short-circuit: if a previous arg already detected stale, skip remaining
            if !shared.config.single_slot_mode && WORKER_STATE.with(|ws| ws.borrow().stale_task_detected) {
                return None;
            }

            if let Some(custom_res) = custom_res {
                return Some(vec![(*custom_res).clone()]);
            }

            // Get predecessor info
            let predecessor = match arg.predecessor.as_ref() {
                Some(p) => p,
                None => return None, // Early return if no predecessor
            };

            // Single explicit index: use the declared index, NOT pred_index.
            // The triggering predecessor may differ from the $res predecessor
            // (e.g., demul's $res reads fft[0] but demul can be triggered by beam).
            if predecessor.indexes.len() == 1 {
                let res_node = &shared.graph.nodes[*res_node_id as usize];
                let res_factor = res_node.factor;
                let current_node = &shared.graph.nodes[node_id as usize];

                let dep_idx = if let Some(ngs) = current_node.group_size {
                    // Current node is grouped: map through symbol level.
                    // symbol = which group/symbol this instance belongs to
                    let symbol = node_index / ngs;
                    // Predecessor's effective group size: its own group_size,
                    // or the barrier's group_by, or fall back to full factor
                    let pred_eff_gs = res_node.group_size.unwrap_or_else(|| {
                        shared.graph_cache.pred_group_by[node_id as usize][*res_node_id as usize]
                            .unwrap_or(res_factor)
                    });
                    let offset = predecessor.indexes[0] as usize;
                    symbol * pred_eff_gs + offset
                } else {
                    find_pred_index(node_index, predecessor.indexes[0], res_factor)
                };

                let node_info = NodeInfo::new(*res_node_id as IdType, slot, dep_idx, 0);
                if let Some(result) = shared.exec.node_results.get(&node_info) {
                    return Some(vec![result]);
                }
                // Result temporarily absent: predecessor may still be executing on a
                // parallel worker (threshold dispatch fired before its store completed).
                // Spin-wait until the result arrives or the slot becomes stale.
                return match spin_wait_for_result(shared, &node_info) {
                    Some(result) => Some(vec![result]),
                    None => None,
                };
            }

            // 1:1 mapping: indexes.len() == node_factor means each instance
            // reads exactly one predecessor result via pred_index (the triggering
            // predecessor IS the $res predecessor in this case).
            if predecessor.indexes.len() > 1 && predecessor.indexes.len() == node_factor {
                let res_node = &shared.graph.nodes[*res_node_id as usize];
                let res_factor = res_node.factor;
                let node_info =
                    NodeInfo::new(*res_node_id as IdType, slot, pred_index % res_factor, 0);
                if let Some(result) = shared.exec.node_results.get(&node_info) {
                    return Some(vec![result]);
                }
                // Spin-wait: predecessor may still be in-flight on another worker.
                return match spin_wait_for_result(shared, &node_info) {
                    Some(result) => Some(vec![result]),
                    None => None,
                };
            }

            // Collect-all path: factor != indexes.len() (e.g., write_res)
            let pred_node = &shared.graph.nodes[predecessor.id as usize];
            let pred_factor = pred_node.factor;

            // Pre-allocate vectors
            let mut indices = Vec::with_capacity(predecessor.indexes.len());
            for &pred_idx in predecessor.indexes.iter() {
                indices.push(find_pred_index(node_index, pred_idx, pred_factor));
            }

            // Lock-free atomic loads - no RwLock contention
            let mut result_vec = Vec::with_capacity(indices.len());

            // Batch collect all results
            for dep_idx in indices.iter() {
                let node_info = NodeInfo::new(*res_node_id as IdType, slot, *dep_idx, 0);
                if let Some(result) = shared.exec.node_results.get(&node_info) {
                    result_vec.push(result);
                } else {
                    // Spin-wait: predecessor may still be in-flight on another worker.
                    match spin_wait_for_result(shared, &node_info) {
                        Some(result) => result_vec.push(result),
                        None => return None, // Stale
                    }
                }
            }

            if result_vec.len() == indices.len() {
                return Some(result_vec);
            }
            None
        }
        CmTypes::Barrier(_) => None,
        _ => Some(vec![arg.type_.clone()]),
    }
}
