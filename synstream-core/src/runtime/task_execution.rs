use super::arg_resolution::populate_cached_args_into;
use super::scheduling::send_to_scheduler;
use super::shared_data::{SharedData, slot_load_ordering, slot_rmw_ordering};
use super::successor::{collect_successors_for_node_into, push_ready_chunked};
use super::thread_locals::{ARG_BUF, WORKER_BUFS, WORKER_STATE, WorkerResolutionBuffers};
use crate::buffers::*;
use crate::debug::print_debug;
use std::sync::Arc;
use synstream_types::*;

/// Worker-side dependency resolution: resolves successors directly on the worker
/// thread that completed the task, bypassing the batch_queue → resolution thread
/// round-trip. Only called for nodes where all successors are non-condition
/// (worker_resolvable == true), ensuring correctness without condition evaluation.
#[inline]
fn worker_resolve_successors(shared: &Arc<SharedData>, node_info: &NodeInfo) {
    let slot = node_info.slot;

    // Step 1: Increment processing_count to prevent premature completion detection.
    shared.slot_data.processing_count[slot].fetch_add(1, slot_rmw_ordering(shared));

    // Step 2: Verify generation — if slot was recycled, bail out.
    let current_gen = shared.slot_data.generation[slot].load(slot_load_ordering(shared)) as u32;
    if current_gen != node_info.gen {
        shared.slot_data.processing_count[slot].fetch_sub(1, slot_rmw_ordering(shared));
        shared.slot_data.needs_check[slot].store(true, std::sync::atomic::Ordering::Release);
        return;
    }

    // Step 3: Decrement task counters (Phase 2 equivalent).
    // For bulk tasks, decrement by bulk_count to account for all instances handled.
    let node_cache_entry = &shared.graph_cache.node_cache[node_info.id as usize];
    if node_cache_entry.is_condition {
        shared.slot_data.pending_cond_tasks[slot].fetch_sub(node_info.bulk_count, slot_rmw_ordering(shared));
    } else if !node_cache_entry.is_initial {
        shared.slot_data.pending_tasks[slot].fetch_sub(node_info.bulk_count, slot_rmw_ordering(shared));
    }

    // Steps 4-6: Collect successors, resolve dependencies, schedule ready nodes.
    WORKER_BUFS.with(|bufs| {
        let mut bufs = bufs.borrow_mut();
        bufs.sched.clear();

        // Step 4: Collect successors.
        collect_successors_for_node_into(shared, node_info, &mut bufs.succ);

        // Load slot generation once for all successors.
        let slot_gen = shared.slot_data.generation[slot].load(slot_load_ordering(shared)) as u32;

        // Step 5: Resolve dependencies for each successor (all non-condition).
        // Destructure into separate field borrows so the borrow checker allows
        // iterating `succ` while mutating `ready` and `sched` simultaneously.
        let WorkerResolutionBuffers { succ, ready, sched, .. } = &mut *bufs;
        for (_succ_info, _has_cond, succ_id, pred_group) in succ.iter() {
            let succ_node_id = *succ_id as usize;

            // For 1:1 non-barrier deps, fire the specific successor instance
            // that reads this predecessor (result guaranteed available).
            let specific_succ_idx = shared
                .graph_cache.pred_succ_1to1_offset
                .get(succ_node_id)
                .and_then(|v| v.get(node_info.id as usize))
                .and_then(|o| *o)
                .map(|k| {
                    let f = shared.graph_cache.node_cache[succ_node_id].factor;
                    ((node_info.index as isize - k).rem_euclid(f as isize)) as usize
                });

            shared.exec.resolution_state.decrease_and_get_ready_into(
                slot,
                succ_node_id,
                slot_gen,
                *pred_group,
                node_info.bulk_count,
                specific_succ_idx,
                ready,
            );

            push_ready_chunked(
                ready,
                succ_node_id as crate::IdType,
                slot,
                node_info.index,
                shared.config.workers,
                shared.config.coalesce_barriers,
                sched,
            );
        }

        // Inline continuation: reserve one ready successor for this worker
        // thread instead of spawning it through the scheduler.
        // Stamp slot_gen so the trampoline's stale check passes on streams > 0.
        let inline = if shared.config.inline_continuation && !bufs.sched.is_empty() {
            bufs.sched.pop().map(|mut ni| { ni.gen = slot_gen; ni })
        } else {
            None
        };

        // Step 6: Schedule remaining ready successors.
        if !bufs.sched.is_empty() {
            let n = bufs.sched.len();
            bufs.args.clear();
            bufs.args.resize(n, None);
            send_to_scheduler(shared, &bufs.sched, &bufs.args, None);
        }

        if let Some(ni) = inline {
            WORKER_STATE.with(|ws| ws.borrow_mut().inline_continuation = Some(ni));
        }
    });

    // Step 7: Decrement processing_count AFTER all successor processing.
    shared.slot_data.processing_count[slot].fetch_sub(1, slot_rmw_ordering(shared));
    shared.slot_data.needs_check[slot].store(true, std::sync::atomic::Ordering::Release);
}

#[inline(always)]
pub(super) fn execute_task(
    shared: &Arc<SharedData>,
    func: CmPtr,
    node_info: &NodeInfo,
    pre_built_args: Option<Vec<CmTypes>>,
    spawn_ns: u128,
) {
    // Stale-task guard: if the slot's generation has advanced since this task was
    // scheduled, the slot was recycled (stream completed + reassigned) while the
    // task sat in the Rayon queue.  Executing it would read cleared predecessor
    // results → panic, or corrupt the new stream's dependency counters.
    // Post-nodes are exempt (gen is always 0 and they run after all streams finish).
    if !node_info.post_node {
        let current_gen =
            shared.slot_data.generation[node_info.slot].load(std::sync::atomic::Ordering::Acquire) as u32;
        if current_gen != node_info.gen {
            print_debug(|| {
                format!(
                    "Stale task dropped: node {} slot {} index {} gen {} (current {})",
                    node_info.id, node_info.slot, node_info.index,
                    node_info.gen, current_gen
                )
            });
            return;
        }
    }

    if node_info.bulk_count > 1 {
        execute_bulk_task(shared, func, node_info);
        return;
    }
    execute_single_task(shared, func, node_info, pre_built_args, spawn_ns);
}

/// Bulk execution path: run `bulk_count` consecutive instances in a tight loop.
///
/// Spawned by `push_ready_chunked` when a barrier fan-out produces N > num_workers
/// ready instances simultaneously (e.g. wavefront diagonal). Eliminates O(N) individual
/// Rayon spawns by covering a contiguous range `index..index+bulk_count` in one task.
fn execute_bulk_task(shared: &Arc<SharedData>, func: CmPtr, node_info: &NodeInfo) {
    if !shared.config.single_slot_mode {
        WORKER_STATE.with(|ws| {
            let mut ws = ws.borrow_mut();
            ws.stale_task_detected = false;
            ws.executing_slot = node_info.slot;
            ws.executing_gen = node_info.gen;
        });
    }

    let node_cache = &shared.graph_cache.node_cache[node_info.id as usize];
    ARG_BUF.with(|buf_cell| {
        let mut buf = buf_cell.borrow_mut();
        for inst_idx in node_info.index..node_info.index + node_info.bulk_count {
            buf.clear();
            populate_cached_args_into(
                &mut buf,
                shared,
                &node_cache.arg_cache,
                node_info.id,
                inst_idx,
                node_info.slot,
                node_info.pred_index,
            );
            if !shared.config.single_slot_mode && WORKER_STATE.with(|ws| ws.borrow().stale_task_detected) {
                buf.clear();
                return; // Slot recycled mid-bulk — drop remaining instances
            }
            let result = func(&buf);
            buf.clear(); // release Arc refs promptly
            if node_cache.needs_result_store {
                let mut inst_info = node_info.clone();
                inst_info.index = inst_idx;
                inst_info.bulk_count = 1;
                shared.exec.node_results.set(&inst_info, result);
            }
        }
    });

    if !shared.config.single_slot_mode && WORKER_STATE.with(|ws| ws.borrow().stale_task_detected) {
        return;
    }
    worker_resolve_successors(shared, node_info);
}

/// Single-instance execution path for regular and post-nodes.
///
/// Builds args from the cache (or uses pre-built args for post-nodes), runs the
/// plugin function, stores the result, and either resolves successors worker-side
/// (if `worker_resolvable`) or sends a completion token to the batch queue.
fn execute_single_task(
    shared: &Arc<SharedData>,
    func: CmPtr,
    node_info: &NodeInfo,
    pre_built_args: Option<Vec<CmTypes>>,
    spawn_ns: u128,
) {
    if !shared.config.single_slot_mode {
        WORKER_STATE.with(|ws| {
            let mut ws = ws.borrow_mut();
            ws.stale_task_detected = false;
            ws.executing_slot = node_info.slot;
            ws.executing_gen = node_info.gen;
        });
    }

    let exec_start_ns = shared.telemetry.base_instant.elapsed().as_nanos();
    let worker_id = crate::scheduler::get_current_worker_id().unwrap_or(usize::MAX);

    if shared.telemetry.async_recorder.is_some() && super::reporting::should_record_slot(shared, node_info.slot) {
        let job_id = shared.telemetry.job_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
        shared.telemetry.measure_start()
    } else {
        None
    };

    let result = if let Some(ref args) = pre_built_args {
        func(args)
    } else {
        let node_cache = &shared.graph_cache.node_cache[node_info.id as usize];
        let result_opt = ARG_BUF.with(|buf_cell| {
            let mut buf = buf_cell.borrow_mut();
            buf.clear();
            populate_cached_args_into(
                &mut buf, shared, &node_cache.arg_cache,
                node_info.id, node_info.index, node_info.slot, node_info.pred_index,
            );
            if !shared.config.single_slot_mode && WORKER_STATE.with(|ws| ws.borrow().stale_task_detected) {
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
                print_debug(|| format!(
                    "Stale task dropped during arg collection: node {} slot {} index {} gen {}",
                    node_info.id, node_info.slot, node_info.index, node_info.gen
                ));
                return;
            }
        }
    };

    let node_name = &shared.graph_cache.node_cache[node_info.id as usize].name;
    shared.telemetry.record_timing(start_time, node_info.slot, node_name, worker_id);

    if shared.graph_cache.node_cache[node_info.id as usize].needs_result_store {
        shared.exec.node_results.set(node_info, result);
    }

    if !node_info.post_node && shared.graph_cache.node_cache[node_info.id as usize].worker_resolvable {
        worker_resolve_successors(shared, node_info);
    } else {
        let _ = shared.exec.batch_queue_tx.send(node_info.clone());
    }
}
