//! Worker-side task execution: runs plugin functions, manages the stale-task guard, and
//! resolves successor dependencies inline when the node is `worker_resolvable`.
//!
//! The primary entry point is [`execute_task`], called from the Rayon task closure built in
//! `scheduling::send_to_scheduler`.  After running the plugin function it either calls
//! [`worker_resolve_successors`] (fast path: all successors are non-condition) or sends a
//! completion token to `batch_queue` for the resolution thread (slow path: condition
//! successors require evaluation on a system thread).
//!
//! This module does **not** own scheduling (that is `scheduling`) or dependency-counter
//! bookkeeping (that is `batch_resolution`/`buffers`).  Its only shared-state writes are
//! `node_results.set()` and `pending_tasks`/`pending_cond_tasks` decrements.

use super::arg_resolution::{populate_cached_args_into, populate_dynamic_args_into};
use super::ordering::{slot_gen_load, slot_gen_rmw};
use super::scheduling::send_to_scheduler;
use super::shared_data::{SchedCtx, SharedData};
use super::successor::{
    collect_successors_for_node_into, decrement_and_collect_ready, push_ready_chunked,
};
use super::thread_locals::{WorkerResolutionBuffers, ARG_BUF, WORKER_BUFS};
use crate::buffers::*;
use crate::debug::print_debug;
use std::sync::Arc;
use tomii_types::*;

/// Atomically add `count` arrivals to the gen-packed fanout-bulk counter for a successor.
///
/// Returns the new total arrived count for this slot generation.  If the stored generation
/// differs from `slot_gen`, the counter is treated as 0 before adding (lazy reset).
///
/// Uses AcqRel ordering: the fetch_update acquires the previous release from a completing
/// predecessor, establishing the happens-before chain needed for result visibility on ARM.
#[inline]
pub(super) fn fanout_bulk_increment(counter: &std::sync::atomic::AtomicU64, slot_gen: u32, count: usize) -> usize {
    use crate::buffers::{gen_pack, gen_unpack_gen, gen_unpack_val};
    use std::sync::atomic::Ordering;
    let result = counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |packed| {
            let stored_gen = gen_unpack_gen(packed);
            let cur = if stored_gen == slot_gen {
                gen_unpack_val(packed)
            } else {
                0
            };
            Some(gen_pack(slot_gen, cur.saturating_add(count as u32)))
        })
        .unwrap(); // fetch_update closure always returns Some
    let stored_gen = gen_unpack_gen(result);
    let old_count = if stored_gen == slot_gen {
        gen_unpack_val(result)
    } else {
        0
    };
    (old_count as usize) + count
}

/// Worker-side dependency resolution: resolves successors directly on the worker
/// thread that completed the task, bypassing the batch_queue → resolution thread
/// round-trip. Only called for nodes where all successors are non-condition
/// (worker_resolvable == true), ensuring correctness without condition evaluation.
///
/// Returns `Some(NodeInfo)` when `inline_continuation` is enabled and a ready
/// successor was reserved for this worker thread to execute immediately.
#[inline]
fn worker_resolve_successors(
    shared: &Arc<SharedData>,
    sctx: &SchedCtx<'_>,
    node_info: &NodeInfo,
) -> Option<NodeInfo> {
    let slot = node_info.slot;
    let ssm = sctx.cfg.single_slot_mode;

    // Step 1: Increment processing_count to prevent premature completion detection.
    sctx.slots.processing_count[slot].fetch_add(1, slot_gen_rmw(ssm));

    // Step 2: Verify generation — if slot was recycled, bail out.
    let current_gen = sctx.slots.generation[slot].load(slot_gen_load(ssm)) as u32;
    if current_gen != node_info.gen {
        sctx.slots.processing_count[slot].fetch_sub(1, slot_gen_rmw(ssm));
        sctx.slots.needs_check[slot].store(true, std::sync::atomic::Ordering::Release);
        return None;
    }

    // Step 3: Decrement task counters (Phase 2 equivalent).
    // For bulk tasks, decrement by bulk_count to account for all instances handled.
    let node_cache_entry = &sctx.cache.node_cache[node_info.id as usize];
    if node_cache_entry.is_condition {
        sctx.slots.pending_cond_tasks[slot].fetch_sub(node_info.bulk_count, slot_gen_rmw(ssm));
    } else if !node_cache_entry.is_initial {
        sctx.slots.pending_tasks[slot].fetch_sub(node_info.bulk_count, slot_gen_rmw(ssm));
    }

    // Steps 4-6: Collect successors, resolve dependencies, schedule ready nodes.
    let rctx = shared.resolve_ctx();
    let inline_result = WORKER_BUFS.with(|bufs| {
        let mut bufs = bufs.borrow_mut();
        bufs.sched.clear();

        // Step 4: Collect successors.
        collect_successors_for_node_into(&shared.graph, rctx.cache, node_info, &mut bufs.succ);

        // Load slot generation once for all successors.
        let slot_gen = rctx.slots.generation[slot].load(slot_gen_load(ssm)) as u32;

        // Step 5: Resolve dependencies for each successor (all non-condition).
        // Destructure into separate field borrows so the borrow checker allows
        // iterating `succ` while mutating `ready` and `sched` simultaneously.
        let WorkerResolutionBuffers {
            succ, ready, sched, ..
        } = &mut *bufs;
        for (_succ_info, _has_cond, succ_id, pred_group) in succ.iter() {
            let succ_node_id = *succ_id as usize;

            decrement_and_collect_ready(
                &rctx,
                slot,
                node_info.id,
                node_info.index,
                succ_node_id,
                *pred_group,
                node_info.bulk_count,
                slot_gen,
                ready,
            );

            let succ_entry = &rctx.cache.node_cache[succ_node_id];
            // Only apply fanout-bulk when inline_continuation is disabled or W=1.
            // With inline_continuation + W>1, ingest→transform runs zero-cost inline;
            // batching would block that pipeline and lose the Rayon-free fast path.
            let fanout_bulk_eligible = succ_entry.is_fanout_bulk
                && !sctx.cfg.no_fanout_bulk
                && (!sctx.cfg.inline_continuation || sctx.cfg.workers == 1);
            if fanout_bulk_eligible && !ready.is_empty() {
                // Fanout-bulk path: accumulate arrivals; dispatch one bulk task
                // when all factor instances have completed.
                let new_arrived = fanout_bulk_increment(
                    &rctx.slots.fanout_bulk_arrived[slot][succ_node_id],
                    slot_gen,
                    ready.len(),
                );
                ready.clear();
                if new_arrived >= succ_entry.factor {
                    let factor = succ_entry.factor;
                    let n_chunks = sctx.cfg.workers.min(factor).max(1);
                    let base = factor / n_chunks;
                    let extra = factor % n_chunks;
                    let mut start = 0usize;
                    for c in 0..n_chunks {
                        let count = base + if c < extra { 1 } else { 0 };
                        let mut chunk_ni = NodeInfo::new(*succ_id, slot, start, node_info.index);
                        chunk_ni.bulk_count = count;
                        chunk_ni.gen = slot_gen;
                        sched.push(chunk_ni);
                        start += count;
                    }
                }
            } else {
                push_ready_chunked(
                    ready,
                    succ_node_id as crate::IdType,
                    slot,
                    node_info.index,
                    sctx.cfg.workers,
                    sctx.cfg.coalesce_barriers,
                    sched,
                );
            }
        }

        // Inline continuation: reserve one ready successor for this worker
        // thread instead of spawning it through the scheduler.
        // Stamp slot_gen so the trampoline's stale check passes on streams > 0.
        let inline = if sctx.cfg.inline_continuation && !bufs.sched.is_empty() {
            bufs.sched.pop().map(|mut ni| {
                ni.gen = slot_gen;
                ni
            })
        } else {
            None
        };

        // Step 6: Schedule remaining ready successors.
        if !bufs.sched.is_empty() {
            let n = bufs.sched.len();
            bufs.args.clear();
            bufs.args.resize(n, None);
            send_to_scheduler(shared, sctx, &bufs.sched, &bufs.args, None);
        }

        inline
    });

    // Step 7: Decrement processing_count AFTER all successor processing.
    sctx.slots.processing_count[slot].fetch_sub(1, slot_gen_rmw(ssm));
    sctx.slots.needs_check[slot].store(true, std::sync::atomic::Ordering::Release);

    inline_result
}

/// Execute a single scheduled task, returning an optional inline continuation.
///
/// **Stale-task guard**: before running the plugin function, the slot generation stored in
/// `node_info.gen` is compared against the current `slot_data.generation[slot]`.  If they
/// differ, the slot was recycled while this task waited in the Rayon queue; the task is
/// silently dropped to prevent reading cleared predecessor results or corrupting the new
/// stream's counters.  Post-nodes are exempt (they carry `gen = 0` and always run after all
/// streams finish).
///
/// Returns `Some(NodeInfo)` when an inline continuation was produced by
/// `worker_resolve_successors` (i.e. the completing worker will immediately execute the
/// returned successor); `None` otherwise.
#[inline(always)]
pub(super) fn execute_task(
    shared: &Arc<SharedData>,
    sctx: &SchedCtx<'_>,
    func: CmPtr,
    node_info: &NodeInfo,
    pre_built_args: Option<Vec<CmTypes>>,
    spawn_ns: u128,
) -> Option<NodeInfo> {
    // Stale-task guard: if the slot's generation has advanced since this task was
    // scheduled, the slot was recycled (stream completed + reassigned) while the
    // task sat in the Rayon queue.  Executing it would read cleared predecessor
    // results → panic, or corrupt the new stream's dependency counters.
    // Post-nodes are exempt (gen is always 0 and they run after all streams finish).
    if !node_info.post_node {
        let current_gen =
            sctx.slots.generation[node_info.slot].load(std::sync::atomic::Ordering::Acquire) as u32;
        if current_gen != node_info.gen {
            print_debug(|| {
                format!(
                    "Stale task dropped: node {} slot {} index {} gen {} (current {})",
                    node_info.id, node_info.slot, node_info.index, node_info.gen, current_gen
                )
            });
            return None;
        }
    }

    if node_info.bulk_count > 1 {
        return execute_bulk_task(shared, sctx, func, node_info);
    }
    execute_single_task(shared, sctx, func, node_info, pre_built_args, spawn_ns)
}

/// Bulk execution path: run `bulk_count` consecutive instances in a tight loop.
///
/// Spawned by `push_ready_chunked` when a barrier fan-out produces N > num_workers
/// ready instances simultaneously (e.g. wavefront diagonal). Eliminates O(N) individual
/// Rayon spawns by covering a contiguous range `index..index+bulk_count` in one task.
///
/// **Tier 1 (arg hoist)**: The static arg template is extended into `buf` once as a
/// prologue; per-iteration only the instance-dependent slots (buffer refs, runtime
/// index/workers, $res results) are patched via `populate_dynamic_args_into`. Static
/// `Arc`s (including `Any` objects) are cloned once per bulk task rather than once
/// per cell.
///
/// **Tier 2 (lock hoist)**: After the static extend, each `CmTypes::Any` slot in
/// `buf` is upgraded to `CmTypes::AnyHeld`, which carries a pre-acquired
/// `ArcRwLockReadGuard`. `with_any` inside the kernel body then returns without
/// calling `RwLock::read()` again — the guard is held for the full bulk range.
fn execute_bulk_task(
    shared: &Arc<SharedData>,
    sctx: &SchedCtx<'_>,
    func: CmPtr,
    node_info: &NodeInfo,
) -> Option<NodeInfo> {
    let node_cache = &sctx.cache.node_cache[node_info.id as usize];
    let args_cache = &node_cache.arg_cache;
    let exec_slot = node_info.slot;
    let exec_gen = node_info.gen;
    let mut bulk_stale = false;

    let workers = if !args_cache.rt_workers_indexes.is_empty() {
        shared.config.workers
    } else {
        0
    };
    let has_dynamic = !args_cache.buffer_ref_indexes.is_empty()
        || !args_cache.rt_idxs_indexes.is_empty()
        || !args_cache.rt_workers_indexes.is_empty()
        || !args_cache.res_indexes.is_empty();

    ARG_BUF.with(|buf_cell| {
        let mut buf = buf_cell.borrow_mut();

        // Tier 1 prologue: clone the static template once (Arc::clone per static Any,
        // not per cell). Placeholders for dynamic slots remain as CmTypes::None.
        buf.extend(args_cache.args.iter().cloned());

        // Tier 2 prologue: upgrade Any slots to AnyHeld so with_any inside the kernel
        // body skips RwLock::read() for the entire bulk range.
        // data_ptr() returns a raw pointer to the Box inside the RwLock's storage;
        // the cloned Arc keeps the allocation alive for the full loop.
        let any_idxs: Vec<usize> = (0..buf.len())
            .filter(|&i| matches!(buf[i], CmTypes::Any(_)))
            .collect();
        for i in any_idxs {
            let held = match &buf[i] {
                CmTypes::Any(arc) => {
                    let ptr = arc.data_ptr() as *const Box<dyn std::any::Any + Send + Sync>;
                    CmTypes::AnyHeld(tomii_types::AnyHeldData::new(ptr, Arc::clone(arc)))
                }
                _ => unreachable!(),
            };
            buf[i] = held;
        }

        // Tier 4 fast path: call the bulk kernel once for the entire range.
        // Preconditions: bulk_func is Some AND needs_result_store is false.
        // (When needs_result_store is true, per-cell results must be stored; fall back
        // to the per-cell loop so sctx.exec.node_results.set runs once per instance.)
        let use_bulk_fast_path = node_cache
            .bulk_func
            .is_some_and(|_| !node_cache.needs_result_store);

        if use_bulk_fast_path {
            // SAFETY: checked is_some above.
            let bulk_fn = node_cache.bulk_func.unwrap();
            // Tier 1+2 prologue already ran: buf is populated and Any slots are upgraded
            // to AnyHeld.  The bulk kernel owns its own `start..end` iteration; no
            // populate_dynamic_args_into call is needed.
            let _result = bulk_fn(
                node_info.index,
                node_info.index + node_info.bulk_count,
                &buf,
            );
            // bulk_stale remains false: generation was checked at the top of
            // execute_task before dispatch into execute_bulk_task.
        } else {
            // Per-cell loop: Tiers 1–3 (no bulk symbol, or needs_result_store).
            for inst_idx in node_info.index..node_info.index + node_info.bulk_count {
                if has_dynamic {
                    let stale = populate_dynamic_args_into(
                        &mut buf,
                        shared,
                        args_cache,
                        inst_idx,
                        node_info.slot,
                        node_info.pred_index,
                        exec_slot,
                        exec_gen,
                        workers,
                    );
                    if stale {
                        bulk_stale = true;
                        break;
                    }
                }
                let result = func(&buf);
                if node_cache.needs_result_store {
                    let mut inst_info = node_info.clone();
                    inst_info.index = inst_idx;
                    inst_info.bulk_count = 1;
                    sctx.exec.node_results.set(&inst_info, result);
                }
            }
        }

        buf.clear(); // release all Arc refs (including AnyHeld guards) once after loop
    });

    if bulk_stale {
        return None;
    }
    worker_resolve_successors(shared, sctx, node_info)
}

/// Single-instance execution path for regular and post-nodes.
///
/// Builds args from the cache (or uses pre-built args for post-nodes), runs the
/// plugin function, stores the result, and either resolves successors worker-side
/// (if `worker_resolvable`) or sends a completion token to the batch queue.
/// Returns `Some(NodeInfo)` when an inline continuation was produced by
/// `worker_resolve_successors`; `None` otherwise.
fn execute_single_task(
    shared: &Arc<SharedData>,
    sctx: &SchedCtx<'_>,
    func: CmPtr,
    node_info: &NodeInfo,
    pre_built_args: Option<Vec<CmTypes>>,
    spawn_ns: u128,
) -> Option<NodeInfo> {
    let exec_start_ns = sctx.telemetry.base_instant.elapsed().as_nanos();
    let worker_id = crate::scheduler::get_current_worker_id().unwrap_or(usize::MAX);

    if sctx.telemetry.async_recorder.is_some()
        && super::reporting::should_record_slot(sctx.cfg, sctx.slots, node_info.slot)
    {
        let job_id = sctx
            .telemetry
            .job_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        crate::async_recorder::submit_record(crate::Record {
            slot: node_info.slot,
            job_id,
            start_ns: spawn_ns,
            end_ns: exec_start_ns,
            worker: worker_id,
            task_id: crate::IdType::MAX - 3 * (node_info.id as crate::IdType),
            index: node_info.index,
        });
    }

    let start_time = if !node_info.post_node {
        sctx.telemetry.measure_start()
    } else {
        None
    };

    let exec_slot = node_info.slot;
    let exec_gen = node_info.gen;

    let result = if let Some(ref args) = pre_built_args {
        func(args)
    } else {
        let node_cache = &sctx.cache.node_cache[node_info.id as usize];
        let result_opt = ARG_BUF.with(|buf_cell| {
            let mut buf = buf_cell.borrow_mut();
            buf.clear();
            let stale = populate_cached_args_into(
                &mut buf,
                shared,
                &node_cache.arg_cache,
                node_info.id,
                node_info.index,
                node_info.slot,
                node_info.pred_index,
                exec_slot,
                exec_gen,
            );
            if stale {
                buf.clear();
                return None::<CmTypes>;
            }
            let r = func(&buf);
            buf.clear();
            Some(r)
        });
        match result_opt {
            Some(r) => r,
            None => {
                print_debug(|| {
                    format!(
                        "Stale task dropped during arg collection: node {} slot {} index {} gen {}",
                        node_info.id, node_info.slot, node_info.index, node_info.gen
                    )
                });
                return None;
            }
        }
    };

    let node_name = &sctx.cache.node_cache[node_info.id as usize].name;
    sctx.telemetry
        .record_timing(start_time, node_info.slot, node_name, worker_id);

    // Post-nodes always store their result so schedule_post_nodes can poll result_exists.
    // Regular nodes store only when a $res successor reads the result.
    if node_info.post_node || sctx.cache.node_cache[node_info.id as usize].needs_result_store {
        sctx.exec.node_results.set(node_info, result);
    }

    if !node_info.post_node && sctx.cache.node_cache[node_info.id as usize].worker_resolvable {
        worker_resolve_successors(shared, sctx, node_info)
    } else {
        let _ = sctx.exec.batch_queue_tx.send(node_info.clone());
        None
    }
}
