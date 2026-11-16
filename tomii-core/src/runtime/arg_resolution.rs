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
    frame_id: usize,
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
    // `$frame_id`: a third, independent runtime-injected ref. Empty for every
    // node that does not reference it (the overwhelming majority), so this
    // loop is a single `is_empty()`-equivalent no-op iteration — no extra work
    // on the hot path for graphs that never use `$frame_id`.
    for idx in cache.rt_frame_indexes.iter() {
        arg_vec[*idx] = CmTypes::Usize(frame_id);
    }
}

/// Patch only the instance-dependent slots of an already-filled arg buffer.
///
/// Called per-instance inside `execute_bulk_task` after the static template has been
/// `extend`ed into `buf` once in the prologue (Tier 1 hoist). Only overwrites
/// `buffer_ref_indexes`, `rt_idxs_indexes`, `rt_workers_indexes`, `rt_frame_indexes`,
/// and `res_indexes` positions — static slots (including `Any`/`AnyHeld` arcs) are
/// left untouched.
///
/// `workers` and `frame_id` are hoisted by the caller (constant across every cell
/// in one bulk dispatch — same pattern for both: `shared.config.workers` /
/// `shared.slot_data.frame_id[slot]` resolve once per bulk task, not once per cell).
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
    frame_id: usize,
) -> bool {
    process_buffer_refs(buf, args_cache, node_index);
    process_runtime_refs(buf, args_cache, node_index, workers, frame_id);

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
        && args_cache.rt_frame_indexes.is_empty()
        && args_cache.res_indexes.is_empty()
    {
        return false;
    }

    let workers = if !args_cache.rt_workers_indexes.is_empty() {
        shared.config.workers
    } else {
        0
    };
    // Only touches `shared.slot_data.frame_id[slot]` (one Relaxed atomic load)
    // when this node actually declares a `$frame_id` arg — zero extra work on
    // the hot path otherwise.
    let frame_id = if !args_cache.rt_frame_indexes.is_empty() {
        shared.slot_data.frame_id[slot].load(std::sync::atomic::Ordering::Relaxed)
    } else {
        0
    };

    process_buffer_refs(buf, args_cache, node_index);
    process_runtime_refs(buf, args_cache, node_index, workers, frame_id);

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
fn handle_special_ref(
    obj_id: usize,
    node_index: usize,
    workers: usize,
    frame_id: usize,
) -> Option<Vec<CmTypes>> {
    match obj_id {
        0 => Some(vec![CmTypes::Usize(node_index)]),
        1 => Some(vec![CmTypes::Usize(workers)]),
        2 => Some(vec![CmTypes::Usize(frame_id)]),
        _ => None,
    }
}

/// Bounds-checked `$frame_id` lookup for the slow/uncached arg-resolution path
/// (`collect_arg_result`, used by condition-arg evaluation, post-node args, and
/// network index-function args).
///
/// `SlotData::frame_id` is sized exactly `config.slots`; `schedule_post_nodes`
/// deliberately dispatches post-nodes on a sentinel "virtual" slot beyond that
/// range (`config.slots + config.system_threads`) that was never assigned a
/// frame. Rather than indexing out of bounds there, this returns
/// [`usize::MAX`] — the SAME "no frame" sentinel `release_and_activate_next`
/// stores on an ordinary slot when it is released (see `slot_management.rs`).
/// The hot per-task path (`populate_cached_args_into`/`populate_dynamic_args_into`)
/// never needs this: `NodeInfo::slot` there is always a real, in-range slot, so
/// it indexes `frame_id` directly with no bounds check.
#[inline]
fn frame_id_for_slot(shared: &SharedData, slot: usize) -> usize {
    shared
        .slot_data
        .frame_id
        .get(slot)
        .map(|a| a.load(std::sync::atomic::Ordering::Relaxed))
        .unwrap_or(usize::MAX)
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
    let sw_t0 = std::time::Instant::now();
    loop {
        if let Some(result) = shared.exec.node_results.get(node_info) {
            super::phase_prof::spin_wait_done(
                node_info.id as usize,
                sw_t0.elapsed().as_nanos() as u64,
            );
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

/// Predecessor instance to fetch for a `$res` arg with a *single* declared index.
///
/// When the consumer has `group_size` set, the declared index is normally an
/// offset *within the predecessor's own per-group chunk* (e.g. `$res fft "0"`
/// alongside a `$barrier` on `fft` grouped by antenna: each of the consumer's
/// symbol-groups reads antenna 0 of its own 4-antenna chunk). That only makes
/// sense when the predecessor is actually produced in lockstep with the
/// consumer's groups — i.e. `pred_eff_group_size` was resolved from a real
/// `group_by` on an edge touching this predecessor, not defaulted.
///
/// When no such grouping exists for this predecessor (`pred_eff_group_size` is
/// `None` — e.g. an *ungrouped* `$barrier` shares this same predecessor), the
/// declared index is a plain broadcast: every instance of the consumer, in
/// every group, reads the same absolute predecessor index. Previously this
/// branch unconditionally multiplied by `pred_factor` as a fallback grouping
/// width, producing an out-of-range predecessor index for every group beyond
/// the first — `node_results` never stores anything at that index, so the
/// requesting worker spins forever (see the module-level stall writeup).
#[inline(always)]
fn single_index_dep_idx(
    node_index: usize,
    declared: isize,
    pred_factor: usize,
    node_group_size: Option<usize>,
    pred_eff_group_size: Option<usize>,
) -> usize {
    match node_group_size {
        Some(ngs) => match pred_eff_group_size {
            Some(eff_gs) => {
                let symbol = node_index / ngs;
                symbol * eff_gs + declared as usize
            }
            // Not grouped in lockstep with this consumer: absolute, broadcast
            // index — identical for every instance regardless of its group.
            None => declared.rem_euclid(pred_factor as isize) as usize,
        },
        None => find_pred_index(node_index, declared, pred_factor),
    }
}

/// Predecessor instance to fetch for a `$res` arg with an explicit per-instance
/// index list (`indexes.len() == node_factor`, one declared index per consumer
/// instance).
///
/// When `pred_factor == node_factor` AND the consumer is dispatched via a
/// genuine 1:1 trigger-paired edge — i.e. `node_has_barrier` is false, see
/// below — this is the common network-ingest shape: `node[i]` is dispatched
/// BY, and reads, `pred[i]`'s own completion, so `pred_index` (the index of
/// the predecessor instance that *triggered* this dispatch) already
/// identifies the right cell and is guaranteed to be stored (no `indexes`
/// lookup needed, and no risk of a not-yet-stored result).
///
/// Otherwise (factors differ, OR `node_has_barrier` is true) `pred_index` is
/// not trustworthy as a per-instance index for THIS predecessor, for two
/// distinct reasons that both need the same fix:
///
/// 1. Factors differ: `indexes.len() == node_factor` here just means "one
///    declared index per consumer instance" (e.g. a per-instance list used to
///    route around `group_size`, or the declared indexes happen to number
///    `node_factor`) — not a true 1:1 fanout. `pred_index` is whichever
///    predecessor instance's completion caused this *group* of consumer
///    instances to become ready, which — when this predecessor is also
///    behind a `$barrier` shared with ANOTHER predecessor's ungrouped/global
///    barrier on the same node — can legitimately belong to that OTHER
///    predecessor instead. Using it directly as a `pred_factor`-space index
///    then points at an unrelated, frequently not-yet-produced predecessor
///    instance, and the requesting worker spins — and because one such
///    mis-dispatch typically fans out across many consumer instances/groups
///    simultaneously, it can occupy every worker at once with nothing left to
///    produce the awaited result (observed as the MIMO demul/fft
///    "pending-frame buffer full" stall).
/// 2. Factors are EQUAL but `node_has_barrier` is true: the node's dependency
///    counter is in `NodeDependencyEntry`'s forced "release the whole factor
///    together" mode (a literal `$barrier` on ANY predecessor — not
///    necessarily this one — or an ambiguous multi-arg predecessor; see
///    `buffers::node_dep::NodeDepMap::new`). That mode releases every
///    instance of this batch with the SAME triggering `pred_index` —
///    whichever predecessor completion (on ANY predecessor feeding this node)
///    happened to zero the shared counter — not a per-instance trigger at
///    all. Trusting it here makes EVERY consumer instance read the SAME
///    predecessor cell instead of its own declared one (confirmed: a node
///    with one 1:1 `$network` `$res` edge plus an unrelated `$barrier` from a
///    second, no-predecessor producer — every instance printed the same
///    `leaf_idx`, the index of whichever network packet happened to zero the
///    barrier-forced shared counter, instead of 0..factor).
///
/// In both cases the correct, context-independent answer is simply the
/// declared index for THIS consumer instance — safe even under case 2's
/// bulk release, since the shared counter cannot reach zero until every
/// declared cell on every predecessor (the UNION `Graph::dependency_count_vec`
/// computes) has completed, so `indexes[node_index]`'s result is already
/// stored by the time any instance fires.
#[inline(always)]
fn per_instance_dep_idx(
    indexes: &[isize],
    node_index: usize,
    pred_index: usize,
    pred_factor: usize,
    node_factor: usize,
    node_has_barrier: bool,
) -> usize {
    if pred_factor == node_factor && !node_has_barrier {
        pred_index % pred_factor
    } else {
        indexes[node_index].rem_euclid(pred_factor as isize) as usize
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
/// - `node_has_barrier`: true when the CURRENT node's dependency counter is in
///   forced bulk-release mode (see [`per_instance_dep_idx`]) — required to tell
///   whether a genuine-1:1-shaped `$res` edge is actually dispatched 1:1 or
///   released in a barrier-triggered batch sharing one `pred_index`.
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
    node_has_barrier: bool,
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
        let dep_idx = single_index_dep_idx(
            node_index,
            indexes[0],
            pred_factor,
            node_group_size,
            pred_eff_group_size,
        );
        let node_info = NodeInfo::new(res_node_id, slot, dep_idx, 0);
        if let Some(result) = shared.exec.node_results.get(&node_info) {
            return Some(vec![result]);
        }
        return spin_wait_for_result(shared, &node_info, exec_slot, exec_gen, stale)
            .map(|r| vec![r]);
    }

    // Per-instance mapping: one declared index per consumer instance.
    if indexes.len() > 1 && indexes.len() == node_factor {
        let dep_idx = per_instance_dep_idx(
            indexes,
            node_index,
            pred_index,
            pred_factor,
            node_factor,
            node_has_barrier,
        );
        let node_info = NodeInfo::new(res_node_id, slot, dep_idx, 0);
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
            let result = spin_wait_for_result(shared, &node_info, exec_slot, exec_gen, stale)?;
            result_vec.push(result);
        }
    }
    if result_vec.len() == indexes.len() {
        Some(result_vec)
    } else {
        None
    }
}

/// Value resolution for a mapped `$res` argument (an `instance_map`-declared
/// predecessor edge — see `crate::instance_map`'s module docs).
///
/// `forward[node_index]` already lists the EXACT, absolute producer instances
/// to fetch, in ascending order — unlike [`fetch_res_results`]'s three
/// branches, there is no `pred_index`/`group_size`/`node_has_barrier`
/// ambiguity to resolve: the mapped edge's routing table is itself the
/// disambiguation (built once at compile time from the validated,
/// already-unioned `instance_map` declaration). An empty list (the
/// "variadic accepts any length incl. 0" case — see module docs) returns
/// `Some(Vec::new())`, which the caller splices as zero elements, removing
/// the placeholder slot entirely; a single-entry list returns one value,
/// spliced in place with no buffer-length change — both callers
/// (`populate_cached_args_into`/`populate_dynamic_args_into`) already handle
/// both shapes identically to the existing collect-all `$res` path.
#[inline(always)]
#[allow(clippy::too_many_arguments)]
fn fetch_mapped_res_results(
    res_node_id: IdType,
    forward: &[Vec<u32>],
    node_index: usize,
    slot: usize,
    shared: &Arc<SharedData>,
    exec_slot: usize,
    exec_gen: u32,
    stale: &mut bool,
) -> Option<Vec<CmTypes>> {
    let list = forward.get(node_index)?;
    let mut result_vec = Vec::with_capacity(list.len());
    for &pred_idx in list {
        let node_info = NodeInfo::new(res_node_id, slot, pred_idx as usize, 0);
        if let Some(result) = shared.exec.node_results.get(&node_info) {
            result_vec.push(result);
        } else {
            let result = spin_wait_for_result(shared, &node_info, exec_slot, exec_gen, stale)?;
            result_vec.push(result);
        }
    }
    Some(result_vec)
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

    // Mapped $res: bypass the ordinary indexes/pred_index/group_size resolution
    // entirely — see `fetch_mapped_res_results`'s doc comment.
    if let Some(routing) = &rp.mapped_forward {
        return fetch_mapped_res_results(
            rp.res_node_id,
            &routing.forward,
            node_index,
            slot,
            shared,
            exec_slot,
            exec_gen,
            stale,
        );
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
        rp.node_has_barrier,
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
            // This is the slow/uncached path (condition args, loop args, network
            // index-function args) — not the per-task hot path, so an
            // unconditional (bounds-checked) frame_id load here is fine; see
            // `frame_id_for_slot`'s doc comment for why it must be bounds-checked
            // on this path specifically (post-node sentinel slot).
            let frame_id = frame_id_for_slot(shared, slot);
            if let Some(result) =
                handle_special_ref(obj_id, node_index, shared.config.workers, frame_id)
            {
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

            // Mirrors `buffers::node_dep::NodeDepMap::new`'s `has_barrier` exactly
            // (see `ResPredCache::node_has_barrier`'s doc comment) — this live-graph
            // path has no precomputed cache, so recompute it from `current_node`.
            let node_has_barrier = current_node.args.iter().any(|a| a.is_barrier())
                || node_has_ambiguous_multi_res_args(current_node);

            fetch_res_results(
                *res_node_id as IdType,
                &predecessor.indexes,
                node_index,
                pred_index,
                node_factor,
                pred_factor,
                current_node.group_size,
                pred_eff_gs,
                node_has_barrier,
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

#[cfg(test)]
mod dep_idx_tests {
    use super::*;

    // -----------------------------------------------------------------------
    // single_index_dep_idx — shape A (ungrouped barrier sharing a $res
    // predecessor, consumer has group_size) and the sibling working shapes.
    // -----------------------------------------------------------------------

    #[test]
    fn single_index_broadcasts_when_predecessor_is_not_grouped_in_lockstep() {
        // Shape A: node has group_size, but this predecessor's barrier is
        // ungrouped (pred_eff_group_size = None) — every group must read the
        // SAME absolute predecessor index, never an out-of-range one.
        let pred_factor = 8;
        let node_group_size = Some(3); // 12/3 = 4 groups
        for node_index in 0..12 {
            let dep_idx = single_index_dep_idx(node_index, 0, pred_factor, node_group_size, None);
            assert_eq!(
                dep_idx, 0,
                "node_index={node_index}: ungrouped predecessor must broadcast index 0, \
                 not scale by group/symbol"
            );
            assert!(
                dep_idx < pred_factor,
                "dep_idx must stay in predecessor's range"
            );
        }
    }

    #[test]
    fn single_index_grouped_predecessor_offsets_by_symbol() {
        // Working sibling: predecessor IS grouped in lockstep (group_by=2 on
        // the same edge) — each symbol group reads its own chunk.
        let pred_factor = 8;
        let node_group_size = Some(3);
        let pred_eff_group_size = Some(2);
        let expected = [0, 0, 0, 2, 2, 2, 4, 4, 4, 6, 6, 6];
        for (node_index, &want) in expected.iter().enumerate() {
            let dep_idx = single_index_dep_idx(
                node_index,
                0,
                pred_factor,
                node_group_size,
                pred_eff_group_size,
            );
            assert_eq!(dep_idx, want, "node_index={node_index}");
        }
    }

    #[test]
    fn single_index_no_group_size_uses_relative_offset() {
        // No group_size at all: unaffected by this fix, same as before.
        assert_eq!(single_index_dep_idx(5, 0, 8, None, None), 5);
        assert_eq!(single_index_dep_idx(5, 2, 8, None, None), 7);
    }

    // -----------------------------------------------------------------------
    // per_instance_dep_idx — shape B (per-instance $res list sharing a
    // dependency-counter group with another predecessor's global barrier).
    // -----------------------------------------------------------------------

    #[test]
    fn per_instance_uses_declared_value_when_factors_differ() {
        // Shape B: pred_factor (4) != node_factor (12) — NOT a genuine 1:1
        // fanout, so `pred_index` must be ignored even when it looks like a
        // plausible (but wrong) predecessor index. The MIMO stall was exactly
        // this: `pred_index` carried an unrelated predecessor's (beam's)
        // triggering index instead of this predecessor's (fft's).
        let indexes: Vec<isize> = (0..12).map(|i| i % 4).collect();
        let pred_factor = 4;
        let node_factor = 12;
        for node_index in 0..12 {
            for bogus_pred_index in [0usize, 1, 2, 3, 999] {
                for node_has_barrier in [false, true] {
                    let dep_idx = per_instance_dep_idx(
                        &indexes,
                        node_index,
                        bogus_pred_index,
                        pred_factor,
                        node_factor,
                        node_has_barrier,
                    );
                    assert_eq!(
                        dep_idx,
                        (node_index % 4),
                        "node_index={node_index} must read its own declared index \
                         regardless of the triggering pred_index={bogus_pred_index}, \
                         node_has_barrier={node_has_barrier}"
                    );
                }
            }
        }
    }

    #[test]
    fn per_instance_true_one_to_one_uses_pred_index() {
        // Genuine 1:1 fanout (pred_factor == node_factor, e.g. network ingest)
        // with NO barrier forcing bulk release: the triggering pred_index is
        // authoritative and already guaranteed stored — this must keep working
        // exactly as before.
        let indexes: Vec<isize> = vec![0; 8]; // content irrelevant on this path
        let pred_factor = 8;
        let node_factor = 8;
        for pred_index in 0..8 {
            let dep_idx = per_instance_dep_idx(
                &indexes,
                pred_index,
                pred_index,
                pred_factor,
                node_factor,
                false,
            );
            assert_eq!(dep_idx, pred_index);
        }
    }

    #[test]
    fn per_instance_one_to_one_shape_but_node_has_barrier_uses_declared_index() {
        // Bug A/B repro shape: pred_factor == node_factor (a true 1:1 $network
        // edge) but the node ALSO carries an unrelated $barrier from a
        // different, no-predecessor producer (e.g. `clear`), forcing
        // `node_has_barrier`. Every consumer instance is released together
        // carrying the SAME triggering pred_index (whichever predecessor
        // completion zeroed the shared counter) — trusting it here would make
        // every instance read the same predecessor cell. Must fall back to
        // the declared per-instance index regardless of what pred_index is.
        let indexes: Vec<isize> = vec![0, 1, 2, 3];
        let pred_factor = 4;
        let node_factor = 4;
        for node_index in 0..4 {
            for triggering_pred_index in 0..4 {
                let dep_idx = per_instance_dep_idx(
                    &indexes,
                    node_index,
                    triggering_pred_index,
                    pred_factor,
                    node_factor,
                    true, // node_has_barrier
                );
                assert_eq!(
                    dep_idx, node_index,
                    "node_index={node_index} must read its own declared index \
                     regardless of the triggering pred_index={triggering_pred_index} \
                     once node_has_barrier forces bulk release"
                );
            }
        }
    }
}

#[cfg(test)]
mod frame_id_tests {
    use super::*;
    use crate::graph_gen::from_json_str;
    use crate::runtime::{RuntimeConfig, TomiiRt, TomiiRtBuilder};
    use crate::scheduler::{create_scheduler, SchedulerConfig, SchedulerType};
    use std::sync::atomic::Ordering;

    /// A single root node (`tagged`) whose only arg is `{"type": "$ref", "value":
    /// "$frame_id"}` — the Python-builder-equivalent idiom is
    /// `TypedValue('$ref', '$frame_id')` (mirrors `$index`/`$workers`; see
    /// `tomii/_schema.py`'s `special_refs`).
    const FRAME_ID_GRAPH: &str = r#"
    {
        "nodes": [
            { "name": "tagged", "factor": 1, "function": "noop",
              "args": [ { "type": "$ref", "value": "$frame_id" } ] }
        ]
    }
    "#;

    /// A node with no `$frame_id` (or any `$ref`) arg at all — the zero-cost
    /// baseline every existing graph (including the MIMO benchmark this
    /// feature is A/B-measured against) takes.
    const NO_FRAME_ID_GRAPH: &str = r#"
    {
        "nodes": [
            { "name": "plain", "factor": 1, "function": "noop", "args": [] }
        ]
    }
    "#;

    fn build_test_rt(json: &str) -> TomiiRt {
        let spec = from_json_str(json, 2).expect("JSON parse failed");
        let scheduler = create_scheduler(SchedulerConfig {
            scheduler_type: SchedulerType::WorkStealing,
            core_offset: 0,
            num_workers: 2,
            record: false,
            external_recorder: None,
            base_instant: std::time::Instant::now(),
            system_threads: 1,
            receiver_threads: 0,
            target_batch_size: 1,
            batch_timeout_us: 10,
            worker_affinity: None,
            worker_hook: None,
        });
        let compiled = spec.compile(&scheduler);
        let config = RuntimeConfig {
            slots: 1,
            max_frames: 1,
            max_runtime: Some(5),
            system_threads: 1,
            workers: 2,
            ..RuntimeConfig::default()
        };
        TomiiRtBuilder::with_config(compiled, scheduler, config)
            .build()
            .expect("build failed")
    }

    #[test]
    fn frame_id_is_delivered_into_the_arg_buffer_at_its_declared_slot() {
        let rt = build_test_rt(FRAME_ID_GRAPH);
        let shared = &rt.shared;
        let node_id: usize = 0; // "tagged" is the only (and first) node

        let node_cache = &shared.graph_cache.node_cache[node_id];
        let args_cache = &node_cache.arg_cache;
        assert_eq!(
            args_cache.rt_frame_indexes,
            vec![0],
            "the sole $frame_id arg must occupy buffer slot 0"
        );

        // Simulate the slot currently belonging to frame 42 (exactly what
        // `assign_frame_to_available_slot`/network admission writes into
        // `SlotData::frame_id` for a real frame).
        shared.slot_data.frame_id[0].store(42, Ordering::Relaxed);

        let mut buf = Vec::new();
        let stale = populate_cached_args_into(
            &mut buf,
            shared,
            args_cache,
            node_id as crate::IdType,
            0, // node_index
            0, // slot
            0, // pred_index
            usize::MAX,
            0,
        );
        assert!(!stale);
        assert_eq!(buf.len(), 1);
        assert_eq!(
            buf[0],
            CmTypes::Usize(42),
            "the $frame_id arg must deliver the CURRENT frame id for its slot, not a stale \
             placeholder or a different slot's id"
        );

        // A new frame takes the slot (generational reuse): the SAME arg must
        // now deliver the NEW frame id, never the old one — no stale read
        // across frame/slot reuse.
        shared.slot_data.frame_id[0].store(43, Ordering::Relaxed);
        buf.clear();
        let stale2 = populate_cached_args_into(
            &mut buf,
            shared,
            args_cache,
            node_id as crate::IdType,
            0,
            0,
            0,
            usize::MAX,
            0,
        );
        assert!(!stale2);
        assert_eq!(
            buf[0],
            CmTypes::Usize(43),
            "$frame_id must track slot reuse across frames, not cache the first value"
        );
    }

    #[test]
    fn no_frame_id_arg_takes_the_zero_cost_early_return_path() {
        let rt = build_test_rt(NO_FRAME_ID_GRAPH);
        let shared = &rt.shared;
        let node_id: usize = 0;
        let node_cache = &shared.graph_cache.node_cache[node_id];
        let args_cache = &node_cache.arg_cache;

        assert!(
            args_cache.rt_frame_indexes.is_empty(),
            "a node with no $frame_id arg must have an empty rt_frame_indexes — this emptiness \
             is exactly what gates the early-return fast path below"
        );
        assert!(args_cache.buffer_ref_indexes.is_empty());
        assert!(args_cache.rt_idxs_indexes.is_empty());
        assert!(args_cache.rt_workers_indexes.is_empty());
        assert!(args_cache.res_indexes.is_empty());

        // Poison the slot's frame_id to a value that would be visibly wrong if
        // (incorrectly) injected anywhere — proves the early-return path never
        // touches `slot_data.frame_id` at all for this node.
        shared.slot_data.frame_id[0].store(usize::MAX, Ordering::Relaxed);

        let mut buf = Vec::new();
        let stale = populate_cached_args_into(
            &mut buf,
            shared,
            args_cache,
            node_id as crate::IdType,
            0,
            0,
            0,
            usize::MAX,
            0,
        );
        assert!(!stale);
        assert!(
            buf.is_empty(),
            "node has zero declared args; the static template extend is the only write"
        );
    }

    #[test]
    fn frame_id_for_slot_returns_sentinel_for_out_of_range_slot() {
        // The post-node "virtual" slot (config.slots + config.system_threads)
        // is deliberately out of SlotData::frame_id's range (sized exactly
        // config.slots) — see `collect_arg_result`'s doc comment. Must return
        // the established "no frame" sentinel, never panic.
        let rt = build_test_rt(NO_FRAME_ID_GRAPH);
        let shared = &rt.shared;
        let out_of_range = shared.slot_data.frame_id.len() + 5;
        assert_eq!(frame_id_for_slot(shared, out_of_range), usize::MAX);
        // A real, in-range slot still resolves normally.
        shared.slot_data.frame_id[0].store(7, Ordering::Relaxed);
        assert_eq!(frame_id_for_slot(shared, 0), 7);
    }
}

#[cfg(test)]
mod mapped_res_value_tests {
    use super::*;
    use crate::buffers::NodeInfo;
    use crate::graph_gen::from_json_str;
    use crate::runtime::{RuntimeConfig, TomiiRt, TomiiRtBuilder};
    use crate::scheduler::{create_scheduler, SchedulerConfig, SchedulerType};

    /// Producer `p` (factor 4, no args) feeding consumer `c` (factor 2) through a
    /// mapped `$res` edge: `c[0]` reads `p[0], p[1]`; `c[1]` reads `p[2], p[3]`.
    /// Every entry has length 2 — the "variadic" shape (see `crate::instance_map`'s
    /// module docs) — deliberately, so the splice/width behaviour is exercised
    /// alongside the uniform-length-1 case below.
    const MAPPED_RES_GRAPH_WIDTH_2: &str = r#"
    {
        "nodes": [
            { "name": "p", "factor": 4, "function": "noop", "args": [] },
            {
                "name": "c",
                "factor": 2,
                "function": "noop",
                "args": [
                    {
                        "type": "$res",
                        "predecessor": { "name": "p", "instance_map": [[0, 1], [2, 3]] }
                    }
                ]
            }
        ]
    }
    "#;

    /// Same shape, but every entry has length exactly 1 — the uniform,
    /// template-stable-eligible case.
    const MAPPED_RES_GRAPH_WIDTH_1: &str = r#"
    {
        "nodes": [
            { "name": "p", "factor": 4, "function": "noop", "args": [] },
            {
                "name": "c",
                "factor": 2,
                "function": "noop",
                "args": [
                    {
                        "type": "$res",
                        "predecessor": { "name": "p", "instance_map": [[1], [3]] }
                    }
                ]
            }
        ]
    }
    "#;

    fn build_test_rt(json: &str) -> TomiiRt {
        let spec = from_json_str(json, 2).expect("JSON parse failed");
        let scheduler = create_scheduler(SchedulerConfig {
            scheduler_type: SchedulerType::WorkStealing,
            core_offset: 0,
            num_workers: 2,
            record: false,
            external_recorder: None,
            base_instant: std::time::Instant::now(),
            system_threads: 1,
            receiver_threads: 0,
            target_batch_size: 1,
            batch_timeout_us: 10,
            worker_affinity: None,
            worker_hook: None,
        });
        let compiled = spec.compile(&scheduler);
        let config = RuntimeConfig {
            slots: 1,
            max_frames: 1,
            max_runtime: Some(5),
            system_threads: 1,
            workers: 2,
            ..RuntimeConfig::default()
        };
        TomiiRtBuilder::with_config(compiled, scheduler, config)
            .build()
            .expect("build failed")
    }

    /// Like `build_test_rt`, but with `slots` > 1 — `spin_wait_for_result`'s
    /// generation-mismatch staleness check is a no-op in `single_slot_mode`
    /// (`slots == 1`, where there is nowhere else for the slot to have gone),
    /// so the forced-wrap staleness test below needs a real multi-slot build.
    fn build_multi_slot_test_rt(json: &str, slots: usize) -> TomiiRt {
        let spec = from_json_str(json, 2).expect("JSON parse failed");
        let scheduler = create_scheduler(SchedulerConfig {
            scheduler_type: SchedulerType::WorkStealing,
            core_offset: 0,
            num_workers: 2,
            record: false,
            external_recorder: None,
            base_instant: std::time::Instant::now(),
            system_threads: 1,
            receiver_threads: 0,
            target_batch_size: 1,
            batch_timeout_us: 10,
            worker_affinity: None,
            worker_hook: None,
        });
        let compiled = spec.compile(&scheduler);
        let config = RuntimeConfig {
            slots,
            max_frames: slots,
            max_runtime: Some(5),
            system_threads: 1,
            workers: 2,
            ..RuntimeConfig::default()
        };
        TomiiRtBuilder::with_config(compiled, scheduler, config)
            .build()
            .expect("build failed")
    }

    /// M2: a producer with a mapped `$res` consumer must have its results
    /// stored (`needs_result_store`), exactly like an ordinary `$res` producer —
    /// `build_node_cache`'s existing `has_res_consumer` check is arg-type/edge-
    /// kind-agnostic (it only inspects `arg.type_.is_result()` and the
    /// predecessor id), so this was already correct with zero code changes;
    /// this test locks that in now that the combination is actually reachable.
    #[test]
    fn producer_with_mapped_res_consumer_needs_result_store() {
        let rt = build_test_rt(MAPPED_RES_GRAPH_WIDTH_2);
        assert!(
            rt.shared.graph_cache.node_cache[0].needs_result_store,
            "producer 'p' feeds a mapped $res consumer; its results must be stored"
        );
    }

    /// M3: value delivery for a mapped `$res` arg whose per-instance list has
    /// length 2 (every consumer instance) — the splice path, same as an
    /// ordinary multi-index $res "collect-all" edge.
    #[test]
    fn mapped_res_width_2_delivers_values_in_ascending_list_order() {
        let rt = build_test_rt(MAPPED_RES_GRAPH_WIDTH_2);
        let shared = &rt.shared;

        // Seed producer results directly (no scheduler dispatch needed to test
        // arg resolution in isolation).
        for i in 0..4u8 {
            shared
                .exec
                .node_results
                .set(&NodeInfo::new(0, 0, i as usize, 0), CmTypes::U8(i * 10));
        }

        let args_cache = &shared.graph_cache.node_cache[1].arg_cache;

        let mut buf0 = Vec::new();
        let stale0 =
            populate_cached_args_into(&mut buf0, shared, args_cache, 1, 0, 0, 0, usize::MAX, 0);
        assert!(!stale0);
        assert_eq!(buf0, vec![CmTypes::U8(0), CmTypes::U8(10)]);

        let mut buf1 = Vec::new();
        let stale1 =
            populate_cached_args_into(&mut buf1, shared, args_cache, 1, 1, 0, 0, usize::MAX, 0);
        assert!(!stale1);
        assert_eq!(buf1, vec![CmTypes::U8(20), CmTypes::U8(30)]);
    }

    /// M3: value delivery for a mapped `$res` arg whose per-instance list has
    /// length 1 (the common, non-"variadic" case) — single value in place, no
    /// splice/width change.
    #[test]
    fn mapped_res_width_1_delivers_single_value_in_place() {
        let rt = build_test_rt(MAPPED_RES_GRAPH_WIDTH_1);
        let shared = &rt.shared;

        for i in 0..4u8 {
            shared
                .exec
                .node_results
                .set(&NodeInfo::new(0, 0, i as usize, 0), CmTypes::U8(i * 10));
        }

        let args_cache = &shared.graph_cache.node_cache[1].arg_cache;

        let mut buf0 = Vec::new();
        populate_cached_args_into(&mut buf0, shared, args_cache, 1, 0, 0, 0, usize::MAX, 0);
        assert_eq!(buf0, vec![CmTypes::U8(10)]); // c[0] reads p[1]

        let mut buf1 = Vec::new();
        populate_cached_args_into(&mut buf1, shared, args_cache, 1, 1, 0, 0, usize::MAX, 0);
        assert_eq!(buf1, vec![CmTypes::U8(30)]); // c[1] reads p[3]
    }

    /// The `all_width_one`/`template_stable` eligibility check (init.rs) must
    /// track the ACTUAL per-instance mapped width, not just `rp.indexes`
    /// (always empty for a mapped arg) — uniform length-1 is eligible, any
    /// non-1 length anywhere disqualifies it, exactly like the ordinary
    /// multi-index collect-all path.
    #[test]
    fn template_stable_tracks_mapped_width_not_declared_indexes() {
        let rt_width_1 = build_test_rt(MAPPED_RES_GRAPH_WIDTH_1);
        assert!(
            rt_width_1.shared.graph_cache.node_cache[1].template_stable,
            "every mapped entry has length 1 — must be template-stable eligible"
        );

        let rt_width_2 = build_test_rt(MAPPED_RES_GRAPH_WIDTH_2);
        assert!(
            !rt_width_2.shared.graph_cache.node_cache[1].template_stable,
            "mapped entries of length 2 vary the arg vector's width — must NOT be \
             template-stable eligible (would grow the persistent template unboundedly)"
        );
    }

    /// M4 (staleness, part 1): ordinary slot reuse. `LockFreeResultMap::reinit_slot`
    /// unconditionally clears EVERY cell of a slot before it is re-dispatched to a
    /// new frame — a generic guarantee, not instance_map-specific — so a mapped
    /// `$res` read issued after reuse must see the NEW frame's values, never a
    /// leftover from the previous occupant of the same slot/cells.
    #[test]
    fn mapped_res_does_not_leak_a_value_across_ordinary_slot_reuse() {
        let rt = build_test_rt(MAPPED_RES_GRAPH_WIDTH_2);
        let shared = &rt.shared;
        let args_cache = &shared.graph_cache.node_cache[1].arg_cache;

        // "Frame A".
        for i in 0..4u8 {
            shared
                .exec
                .node_results
                .set(&NodeInfo::new(0, 0, i as usize, 0), CmTypes::U8(i));
        }
        let mut buf = Vec::new();
        populate_cached_args_into(&mut buf, shared, args_cache, 1, 0, 0, 0, usize::MAX, 0);
        assert_eq!(buf, vec![CmTypes::U8(0), CmTypes::U8(1)]);

        // Slot reuse for "Frame B" — the runtime always fully clears a slot's
        // result storage before re-dispatching it.
        shared.exec.node_results.reinit_slot(0);
        for i in 0..4u8 {
            shared
                .exec
                .node_results
                .set(&NodeInfo::new(0, 0, i as usize, 0), CmTypes::U8(100 + i));
        }

        let mut buf2 = Vec::new();
        populate_cached_args_into(&mut buf2, shared, args_cache, 1, 0, 0, 0, usize::MAX, 0);
        assert_eq!(
            buf2,
            vec![CmTypes::U8(100), CmTypes::U8(101)],
            "must read frame B's values, not frame A's leftover"
        );
    }

    /// M4 (staleness, part 2): a mapped `$res` read still in flight (waiting on a
    /// producer result that never arrives for THIS attempt) when the slot's
    /// generation changes — including across the 2^32 wrap boundary, forced
    /// exactly like
    /// `slot_management::generation_tests::test_bump_slot_generation_wrap_sweeps_instance_map_stale_entries`
    /// — must abort as stale rather than hang or (worse) later deliver a value
    /// that belongs to the superseded attempt. Needs `slots > 1`:
    /// `spin_wait_for_result`'s generation-mismatch check is a deliberate no-op
    /// in `single_slot_mode` (nothing else the slot could have become).
    #[test]
    fn mapped_res_read_aborts_as_stale_across_a_forced_generation_wrap() {
        use std::sync::atomic::Ordering;

        let rt = build_multi_slot_test_rt(MAPPED_RES_GRAPH_WIDTH_2, 2);
        let shared = &rt.shared;
        let args_cache = &shared.graph_cache.node_cache[1].arg_cache;

        // This hypothetical task was dispatched under generation `u32::MAX`...
        let exec_gen = u32::MAX;
        shared.slot_data.generation[0].store(u64::from(exec_gen), Ordering::SeqCst);
        // ...but before its mapped producer's result ever arrives, the slot is
        // released and reassigned, wrapping the low-32 generation back to 0
        // (forced exactly as `bump_slot_generation`'s own wrap tests do).
        crate::runtime::slot_management::bump_slot_generation(shared, 0);
        assert_eq!(
            shared.slot_data.generation[0].load(Ordering::SeqCst) as u32,
            0,
            "generation must have wrapped to 0"
        );

        // No producer result was ever stored for this (now-superseded) attempt —
        // `fetch_mapped_res_results` must fall through to `spin_wait_for_result`,
        // observe the mismatch on its very first poll, and abort immediately.
        let mut buf = Vec::new();
        let stale =
            populate_cached_args_into(&mut buf, shared, args_cache, 1, 0, 0, 0, 0, exec_gen);
        assert!(
            stale,
            "a mapped $res read spanning a generation change must be dropped as stale, \
             never silently deliver a value (or hang) past the wrap"
        );
    }
}
