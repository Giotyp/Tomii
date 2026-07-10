//! Task dispatch: builds argument vectors, stamps slot generation, and submits to the scheduler.
//!
//! [`send_to_scheduler`] is the single choke-point through which every ready `NodeInfo` is
//! submitted to the Rayon-backed [`crate::scheduler::SchedulerImpl`].  It stamps the current
//! slot generation onto each task so [`execute_task`] can detect stale tasks that linger in
//! the Rayon queue across a slot boundary.
//!
//! [`dispatch_nodes`] is the higher-level wrapper used by resolution threads: it reuses the
//! `PREP_ARGS_BUF` thread-local to avoid a `vec![None; N]` heap allocation on every flush
//! and delegates to `send_to_scheduler` after recording timing.
//!
//! This module does **not** implement task execution (that is `task_execution`) or successor
//! resolution (that is `batch_resolution` / `task_execution::worker_resolve_successors`).

use super::reporting::should_record_slot;
use super::shared_data::{SchedCtx, SharedData};
use super::task_execution::execute_task;
use super::thread_locals::PREP_ARGS_BUF;
use crate::async_recorder::submit_record;
use crate::buffers::*;
use crate::func_reg::get_func;
use crate::IdType;
use crate::Record;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tomii_types::*;

/// Submit `nodes_to_schedule` to the scheduler, one task per node.
///
/// **Typed fast path (P2)**: on the Custom scheduler, regular nodes are spawned as POD
/// [`crate::custom_scheduler::NodeTaskDesc`] values — no per-task `Box`, closure capture,
/// or `Arc<SharedData>` clone. Workers execute them via [`run_node_desc`] through the
/// node-executor hook installed at runtime init (see `TomiiRtBuilder::build`).
///
/// **Boxed fallback**: Rayon/Plugin schedulers, post-nodes, custom-func nodes, and nodes
/// with pre-built args take the closure path. Each boxed task is a trampoline loop that
/// runs `execute_task` and, if an inline continuation is returned, immediately executes
/// the next node on the same worker thread without re-entering the scheduler. Post-nodes
/// are cold (function and priority looked up from `graph.post_nodes`) because they are
/// rare end-of-run events and their metadata is not mirrored in `node_cache`.
///
/// `gen` is stamped onto each `NodeInfo` clone here so stale tasks can be detected cheaply
/// inside `execute_task` without a shared-state read at stamp time.
#[inline]
pub(super) fn send_to_scheduler(
    shared: &Arc<SharedData>,
    sctx: &SchedCtx<'_>,
    nodes_to_schedule: &[NodeInfo],
    pre_built_args_vec: &[Option<Vec<CmTypes>>],
    custom_func_vec: Option<&[Option<CmPtr>]>,
) {
    // Hoisted out of the loop: one variant check per batch, not per task.
    let typed_spawn = sctx.exec.scheduler.has_typed_spawn();

    for (i, node_info) in nodes_to_schedule.iter().enumerate() {
        // Look up func_ptr, priority, and affinity from pre-computed cache.
        // Post-nodes use the cold path since they're rare (end-of-run only).
        let custom_func = custom_func_vec.and_then(|v| v[i]);
        let (func_ptr, task_priority, affinity_group) = if node_info.post_node {
            let nodes = &shared
                .graph
                .post_nodes
                .as_ref()
                .expect("Post nodes not initialized");
            let node = &nodes[node_info.id as usize];

            let func = custom_func.unwrap_or_else(|| {
                get_func(&node.func_name).unwrap_or_else(|| {
                    panic!(
                        "Post-node function '{}' not found in registry",
                        node.func_name
                    )
                })
            });

            use crate::custom_scheduler::Priority;
            use crate::graph_struct::NodePriority;
            let priority = match node.priority {
                NodePriority::High => Priority::High,
                NodePriority::Normal => Priority::Normal,
                NodePriority::Low => Priority::Low,
            };
            let group = sctx
                .exec
                .scheduler
                .get_affinity_group(node.use_workers.as_ref());
            (func, priority, group)
        } else {
            let cache = &sctx.cache.node_cache[node_info.id as usize];
            let func = custom_func.unwrap_or(cache.func_ptr);
            (func, cache.priority, cache.affinity_group)
        };

        let should_record = should_record_slot(sctx.cfg, sctx.slots, node_info.slot);
        let mut node_info = node_info.clone();
        // Stamp the current slot generation so execute_task can detect stale tasks.
        // Post-nodes are exempt: they run after all frames complete and have no generation risk.
        if !node_info.post_node {
            node_info.gen = sctx.slots.generation[node_info.slot].load(Ordering::Acquire) as u32;
        }

        // Per-task spawn timestamp for accurate scheduling latency measurement.
        let spawn_ns = sctx.telemetry.base_instant.elapsed().as_nanos();

        // Typed fast path: POD descriptor through the Custom scheduler's queue.
        // Post-nodes, custom-func nodes, and pre-built-args nodes stay boxed —
        // run_node_desc always resolves the function from node_cache and builds
        // args from the arg cache, which only holds for regular nodes.
        if typed_spawn
            && !node_info.post_node
            && custom_func.is_none()
            && pre_built_args_vec[i].is_none()
        {
            sctx.exec.scheduler.spawn_node(
                affinity_group,
                task_priority,
                crate::custom_scheduler::NodeTaskDesc {
                    node: node_info,
                    func: func_ptr,
                    spawn_ns,
                    job_id: 0, // assigned by spawn_node
                    should_record,
                },
            );
            continue;
        }

        let shared_clone = Arc::clone(shared);
        let meta_data = crate::TaskMeta {
            task_id: node_info.id,
            slot: node_info.slot,
            index: node_info.index,
            should_record,
        };
        let pre_built_args = pre_built_args_vec[i].clone();
        let task = move || {
            // Build sctx once per task — it's just field borrows from the Arc.
            let sctx = shared_clone.sched_ctx();
            let mut current = node_info;
            let mut current_func = func_ptr;
            let mut first = true;
            loop {
                let args = if first { pre_built_args.clone() } else { None };
                first = false;
                match execute_task(&shared_clone, &sctx, current_func, &current, args, spawn_ns) {
                    Some(next) => {
                        current_func = sctx.cache.node_cache[next.id as usize].func_ptr;
                        current = next;
                    }
                    None => break,
                }
            }
        };

        if affinity_group > 0 {
            sctx.exec.scheduler.spawn_to_group_with_meta(
                affinity_group,
                task_priority,
                Some(meta_data),
                task,
            );
        } else {
            sctx.exec
                .scheduler
                .spawn_task_with_meta_priority(task_priority, Some(meta_data), task);
        }
    }
}

/// Execute a typed node task from the Custom scheduler's zero-alloc queue.
///
/// Body is identical to the boxed-task trampoline in [`send_to_scheduler`]: run the
/// node, then follow inline continuations on this worker thread without re-entering
/// the scheduler. Called via the node-executor hook installed in `TomiiRtBuilder::build`;
/// the hook upgrades a `Weak<SharedData>` before calling here, so `shared` is alive for
/// the whole trampoline.
pub(super) fn run_node_desc(shared: &Arc<SharedData>, desc: crate::custom_scheduler::NodeTaskDesc) {
    // Build sctx once per task — it's just field borrows from the Arc.
    let sctx = shared.sched_ctx();
    let mut current = desc.node;
    let mut current_func = desc.func;
    // Typed spawns never carry pre-built args (post-nodes stay on the boxed path).
    while let Some(next) = execute_task(shared, &sctx, current_func, &current, None, desc.spawn_ns)
    {
        current_func = sctx.cache.node_cache[next.id as usize].func_ptr;
        current = next;
    }
}

/// High-level dispatch helper used by resolution threads.
///
/// Reuses the `PREP_ARGS_BUF` thread-local (`Vec<Option<Vec<CmTypes>>>`) to construct the
/// `None`-filled args slice passed to [`send_to_scheduler`], avoiding a `vec![None; N]` heap
/// allocation on every incremental flush (~77 flushes per frame at default batch sizes).
/// Records timing and an optional async-recorder event around the scheduler submission.
pub(super) fn dispatch_nodes(
    shared: &Arc<SharedData>,
    sctx: &SchedCtx<'_>,
    nodes_to_schedule: &[NodeInfo],
    thread_core: usize,
    thread_slot: usize,
) {
    let start_time = sctx.telemetry.measure_start();
    let start_ns = sctx.telemetry.base_instant.elapsed().as_nanos();

    // Schedule Task - args will be built in the worker thread.
    // Reuse thread-local buffer to avoid vec![None; N] heap allocation per flush.
    PREP_ARGS_BUF.with(|abuf| {
        let mut args_buf = abuf.borrow_mut();
        let n = nodes_to_schedule.len();
        args_buf.clear();
        args_buf.resize(n, None);
        send_to_scheduler(shared, sctx, nodes_to_schedule, &args_buf, None);
    });

    sctx.telemetry
        .record_timing(start_time, thread_slot, "Preparation", usize::MAX);

    // Lock-free recording via per-worker channel
    let should_record = sctx.telemetry.async_recorder.is_some()
        && nodes_to_schedule
            .iter()
            .any(|n| should_record_slot(sctx.cfg, sctx.slots, n.slot));
    if should_record {
        let end_ns = sctx.telemetry.base_instant.elapsed().as_nanos();
        let job_id = sctx.telemetry.job_counter.fetch_add(1, Ordering::SeqCst);
        submit_record(Record {
            slot: thread_slot,
            job_id,
            start_ns,
            end_ns,
            worker: thread_core,
            task_id: IdType::MAX - 1,
            index: 0,
        });
    }
}

impl super::TomiiRt {
    pub(super) fn schedule_post_nodes(&mut self) {
        use std::thread::sleep;
        use std::time::Duration;
        let nodes = &self.shared.graph.post_nodes;
        if let Some(post_nodes) = nodes {
            let frame_use = self.shared.config.slots + self.shared.config.system_threads; // Use last available slot for post-nodes
            for post_node in post_nodes {
                let mut post_schedule: Vec<NodeInfo> = Vec::new();
                let mut pre_build_args: Vec<Option<Vec<CmTypes>>> = Vec::new();
                let mut functions: Vec<Option<CmPtr>> = Vec::new();
                for index in 0..post_node.factor {
                    let mut node_info = NodeInfo::new(post_node.id, frame_use, index, 0);
                    node_info.set_post_node(true);

                    let arg_vec = super::arg_resolution::parse_args(
                        &self.shared,
                        &post_node.args,
                        index,
                        frame_use,
                        0,
                        None,
                    );

                    let func: Option<CmPtr> = get_func(&post_node.func_name);
                    pre_build_args.push(Some(arg_vec));
                    functions.push(func);
                    post_schedule.push(node_info);
                }
                let sctx = self.shared.sched_ctx();
                send_to_scheduler(
                    &self.shared,
                    &sctx,
                    &post_schedule,
                    &pre_build_args,
                    Some(&functions),
                );
                crate::debug::print_debug(|| format!("Added post node: {}", post_node.name));
                // Wait until all are completed by checking node_results
                let mut completed_count = 0;
                while completed_count < post_node.factor {
                    sleep(Duration::from_millis(10));
                    completed_count = 0;
                    // Lock-free check - no RwLock needed
                    for i in 0..post_node.factor {
                        let node_info = NodeInfo::new(post_node.id, frame_use, i, 0);
                        if self.shared.exec.node_results.result_exists(&node_info) {
                            completed_count += 1;
                        }
                    }
                }
            }
            crate::debug::print_debug(|| "All post-nodes completed".to_string());
        }
    }
}
