use super::arg_resolution::{
    populate_cached_args_into, ARG_BUF, EXECUTING_GEN, EXECUTING_SLOT, STALE_TASK_DETECTED,
};
use super::scheduling::send_to_scheduler;
use super::shared_data::{SharedData, slot_load_ordering, slot_rmw_ordering};
use super::successor::{collect_successors_for_node_into, push_ready_chunked};
use crate::buffers::*;
use crate::debug::print_debug;
use crate::time_buffer::TimingMethod;
use std::cell::RefCell;
use std::sync::Arc;
use std::time::Instant;
use synstream_types::*;

thread_local! {
    // Worker-side dependency resolution buffers.
    // Used by worker_resolve_successors to avoid heap allocation on the hot path.
    static WORKER_SUCC_BUF: RefCell<Vec<(NodeInfo, bool, crate::IdType, Option<usize>)>> =
        RefCell::new(Vec::with_capacity(32));
    static WORKER_READY_BUF: RefCell<Vec<usize>> = RefCell::new(Vec::with_capacity(32));
    static WORKER_SCHED_BUF: RefCell<Vec<NodeInfo>> = RefCell::new(Vec::with_capacity(32));
    static WORKER_ARGS_BUF: RefCell<Vec<Option<Vec<CmTypes>>>> = RefCell::new(Vec::with_capacity(32));

    /// Set by worker_resolve_successors when inline_continuation is enabled.
    /// Consumed by the send_to_scheduler trampoline after execute_task returns.
    pub(super) static INLINE_CONTINUATION: RefCell<Option<NodeInfo>> = RefCell::new(None);
}

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
    WORKER_SUCC_BUF.with(|sbuf| {
        WORKER_READY_BUF.with(|rbuf| {
            WORKER_SCHED_BUF.with(|tbuf| {
                WORKER_ARGS_BUF.with(|abuf| {
                    let mut succ_buf = sbuf.borrow_mut();
                    let mut ready = rbuf.borrow_mut();
                    let mut sched = tbuf.borrow_mut();
                    let mut args_buf = abuf.borrow_mut();
                    sched.clear();

                    // Step 4: Collect successors.
                    collect_successors_for_node_into(shared, node_info, &mut succ_buf);

                    // Load slot generation once for all successors.
                    let slot_gen = shared.slot_data.generation[slot].load(slot_load_ordering(shared)) as u32;

                    // Step 5: Resolve dependencies for each successor (all non-condition).
                    for (_succ_info, _has_cond, succ_id, pred_group) in succ_buf.iter() {
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
                            &mut ready,
                        );

                        push_ready_chunked(
                            &ready,
                            succ_node_id as crate::IdType,
                            slot,
                            node_info.index,
                            shared.config.workers,
                            shared.config.coalesce_barriers,
                            &mut sched,
                        );
                    }

                    // Inline continuation: reserve one ready successor for this worker
                    // thread instead of spawning it through the scheduler.
                    // Stamp slot_gen so the trampoline's stale check passes on streams > 0.
                    let inline = if shared.config.inline_continuation && !sched.is_empty() {
                        sched.pop().map(|mut ni| { ni.gen = slot_gen; ni })
                    } else {
                        None
                    };

                    // Step 6: Schedule remaining ready successors.
                    if !sched.is_empty() {
                        args_buf.clear();
                        args_buf.resize(sched.len(), None);
                        send_to_scheduler(shared, &sched, &args_buf, None);
                    }

                    if let Some(ni) = inline {
                        INLINE_CONTINUATION.with(|c| *c.borrow_mut() = Some(ni));
                    }
                });
            });
        });
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

    // Bulk execute path: run multiple consecutive instances in a tight loop on this worker.
    // Spawned by push_ready_chunked when a barrier fan-out produces N > num_workers ready
    // instances simultaneously (e.g. wavefront diagonal completion).  Each bulk task covers
    // a contiguous range `index..index+bulk_count`, eliminating O(N) individual Rayon spawns.
    if node_info.bulk_count > 1 {
        // Set stale-detection TLS context — required by populate_cached_args_into.
        // In single_slot_mode mid-execution slot recycling is impossible; skip TLS writes.
        if !shared.config.single_slot_mode {
            STALE_TASK_DETECTED.with(|f| *f.borrow_mut() = false);
            EXECUTING_SLOT.with(|s| *s.borrow_mut() = node_info.slot);
            EXECUTING_GEN.with(|g| *g.borrow_mut() = node_info.gen);
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
                if !shared.config.single_slot_mode && STALE_TASK_DETECTED.with(|f| *f.borrow()) {
                    buf.clear();
                    return; // Slot recycled mid-bulk — drop remaining instances
                }
                let result = func(&buf);
                buf.clear(); // release Arc refs promptly
                // Store result for this specific instance using a per-instance NodeInfo
                if node_cache.needs_result_store {
                    let mut inst_info = node_info.clone();
                    inst_info.index = inst_idx;
                    inst_info.bulk_count = 1;
                    shared.exec.node_results.set(&inst_info, result);
                }
            }
        });

        // Stale check: if slot was recycled during bulk execution, skip completion accounting
        if !shared.config.single_slot_mode && STALE_TASK_DETECTED.with(|f| *f.borrow()) {
            return;
        }

        // Single call to worker_resolve_successors accounts for all bulk_count instances
        // via the bulk_count-aware counter decrements and decrease_and_get_ready_into call.
        worker_resolve_successors(shared, node_info);
        return;
    }

    // Initialize stale-detection context for collect_arg_result.
    // In single_slot_mode, mid-execution slot recycling is impossible; skip TLS writes.
    if !shared.config.single_slot_mode {
        STALE_TASK_DETECTED.with(|f| *f.borrow_mut() = false);
        EXECUTING_SLOT.with(|s| *s.borrow_mut() = node_info.slot);
        EXECUTING_GEN.with(|g| *g.borrow_mut() = node_info.gen);
    }

    // Capture execution start timestamp immediately
    let exec_start_ns = shared.telemetry.base_instant.elapsed().as_nanos();

    let worker_id = crate::scheduler::get_current_worker_id().unwrap_or(usize::MAX);

    // Record scheduling latency if async recorder enabled and slot should be recorded
    if shared.telemetry.async_recorder.is_some() {
        if super::reporting::should_record_slot(shared, node_info.slot) {
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
    }

    // Look up time_buf from shared (avoids Option<Arc> clone per task in closure capture)
    let time_buf = &shared.telemetry.time_buffer;

    // Start timing for actual function execution
    let start_time = if !node_info.post_node {
        Some(if let Some(tb) = time_buf {
            tb.measure_time()
        } else {
            TimingMethod::Instant(Instant::now())
        })
    } else {
        None
    };

    let result = if let Some(ref args) = pre_built_args {
        // For post-nodes or special cases with pre-built args
        func(args)
    } else {
        // For regular nodes, build args from cache using thread-local buffer
        let node_cache = &shared.graph_cache.node_cache[node_info.id as usize];
        let result_opt = ARG_BUF.with(|buf_cell| {
            let mut buf = buf_cell.borrow_mut();
            buf.clear();
            populate_cached_args_into(
                &mut buf,
                shared,
                &node_cache.arg_cache,
                node_info.id,
                node_info.index,
                node_info.slot,
                node_info.pred_index,
            );
            // Check stale BEFORE calling func (result missing due to reinit_slot race)
            if !shared.config.single_slot_mode && STALE_TASK_DETECTED.with(|f| *f.borrow()) {
                buf.clear();
                return None::<CmTypes>;
            }
            let r = func(&buf);
            buf.clear(); // release Arc refs promptly
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
                return; // Silently drop — slot was recycled during arg collection
            }
        }
    };

    if let Some(start) = start_time {
        if let Some(tb) = time_buf {
            let end_time = tb.measure_time();
            let duration = tb.measure_duration(start, end_time);
            // Look up node_name from cache (avoids String clone per task in closure capture)
            let node_name = &shared.graph_cache.node_cache[node_info.id as usize].name;
            tb.add_task_time(node_info.slot, node_name, worker_id, duration);
        }
    }

    // Pre-store result in node_results before resolving or sending completion token.
    // Skip the store when no successor reads this result via $res (barrier-only successors).
    if shared.graph_cache.node_cache[node_info.id as usize].needs_result_store {
        shared.exec.node_results.set(node_info, result);
    }

    // Worker-side dependency resolution: if all successors are non-condition,
    // resolve dependencies and schedule successors directly on this worker thread,
    // bypassing the batch_queue → resolution thread round-trip (~5-15μs → ~100-500ns).
    if !node_info.post_node && shared.graph_cache.node_cache[node_info.id as usize].worker_resolvable {
        worker_resolve_successors(shared, node_info);
    } else {
        // Send the lightweight NodeInfo token. Use blocking send (not try_send) so that
        // a full queue causes the worker to wait rather than silently dropping the token.
        // Dropping a token would leave slot_pending_tasks > 0 forever → hang.
        let _ = shared.exec.batch_queue_tx.send(node_info.clone());
    }
}
