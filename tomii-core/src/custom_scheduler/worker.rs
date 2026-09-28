use super::channels::{try_recv_all, try_steal_shards, ChannelSet, Job, ScheduledTask};
use super::NodeTaskDesc;
use crate::async_recorder::{set_worker_recorder, submit_record, AsyncRecorder};
use crate::Record;
use core_affinity::CoreId;
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

/// Executor hook for typed node tasks — installed once at runtime init via
/// [`super::CustomScheduler::set_node_executor`]. The runtime's hook holds a
/// `Weak<SharedData>` (not an `Arc`) so the scheduler, which is owned by
/// `SharedData.exec`, does not form a reference cycle with it.
pub(super) type NodeExecutor = Box<dyn Fn(NodeTaskDesc) + Send + Sync>;

/// Shared state for all workers
pub(super) struct SharedWorkerState {
    /// Global channels (fallback when group channels empty)
    pub(super) global_channels: ChannelSet,
    /// Per-group channels. In the default (non-affinity) configuration these are used
    /// as SHARDS: dispatch round-robins across them and workers steal across them, so
    /// no single channel is contended by every worker at high W. In an affinity
    /// configuration they are exclusive worker groups instead (see `sharded`).
    pub(super) group_channels: Vec<Arc<ChannelSet>>,
    /// True when `group_channels` are shards (default config): default dispatch
    /// round-robins across them and idle workers steal across them. False for the
    /// affinity configuration, where a `group_id == 0` task goes to the global pool and
    /// groups are exclusive (no cross-group stealing).
    pub(super) sharded: bool,
    /// Round-robin cursor used as the tie-break/start point for least-loaded sharded
    /// dispatch.
    pub(super) next_shard: AtomicUsize,
    /// Per-shard count of jobs currently executing on that shard's workers. A worker
    /// increments its shard's entry just before running a job and decrements right
    /// after (a stolen job counts under the executing worker's shard — that is the shard
    /// whose worker is actually busy). Least-loaded dispatch adds this to the queue
    /// length so it routes by real occupancy: a shard whose workers are all busy on
    /// long tasks with an empty queue still reads as loaded, so a task is not routed
    /// into it while another shard's workers sleep. Only maintained when `sharded`.
    pub(super) in_flight: Vec<AtomicUsize>,
    /// Shutdown signal
    pub(super) shutdown: AtomicBool,
    /// Total tasks spawned (for metrics)
    pub(super) total_spawned: AtomicUsize,
    /// Total tasks completed (for metrics)
    pub(super) total_completed: AtomicUsize,
    /// Pending tasks (spawned - completed)
    pub(super) pending_tasks: AtomicUsize,
    /// Optional async recorder
    pub(super) async_recorder: Option<Arc<AsyncRecorder>>,
    /// Base instant for timing
    pub(super) base_instant: Arc<Instant>,
    /// System core offset for recorder channel indexing
    pub(super) system_core_offset: usize,
    /// Typed node-task executor (set before any `Job::Node` is spawned)
    pub(super) node_exec: OnceLock<NodeExecutor>,
    /// Optional per-worker lifecycle hook — called once at worker start and
    /// exit, never on the task hot path.
    pub(super) worker_hook: Option<Arc<dyn crate::WorkerHook>>,
}

// Per-worker state accessible via thread-local
thread_local! {
    static WORKER_STATE: Cell<WorkerState> = Cell::new(WorkerState::default());
}

#[derive(Debug, Clone, Copy, Default)]
struct WorkerState {
    #[allow(dead_code)] // future per-worker metrics / diagnostics
    worker_id: usize, // Global worker index
    #[allow(dead_code)] // future per-group routing decisions
    group_id: usize, // Which group this worker belongs to
    core_id: usize, // Physical core ID
    #[allow(dead_code)] // future per-worker throughput reporting
    tasks_executed: usize, // Counter for metrics
}

/// Worker thread main loop.
///
/// Three phases per iteration:
/// 1. Non-blocking try_recv from all channels in priority order
/// 2. Adaptive spin: brief user-space spinning with try_recv checks
/// 3. Block on crossbeam select! until a channel has data or timeout
///
/// The spin phase catches tasks arriving shortly after the initial check,
/// avoiding the ~1-5us futex wake latency for burst arrivals.
pub(super) fn worker_loop(
    worker_id: usize,
    group_id: usize,
    core_id: CoreId,
    shared: Arc<SharedWorkerState>,
    group_channels: Arc<ChannelSet>,
    allow_global_steal: bool,
    spin_iterations: usize,
) {
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    // Pin to core
    core_affinity::set_for_current(core_id);

    // Set thread-local state
    crate::scheduler::set_current_worker_id(core_id.id);
    crate::scheduler::set_current_worker_index(worker_id);

    WORKER_STATE.with(|s| {
        s.set(WorkerState {
            worker_id,
            group_id,
            core_id: core_id.id,
            tasks_executed: 0,
        });
    });

    // Initialize async recorder channel if enabled
    if let Some(ref recorder) = shared.async_recorder {
        let channel_index = core_id.id - shared.system_core_offset;
        if let Some(tx) = recorder.get_worker_sender(channel_index) {
            set_worker_recorder(tx);
        }
    }

    // Lifecycle hook: worker fully initialized (pinned, thread-locals set),
    // no task has run yet.
    if let Some(ref hook) = shared.worker_hook {
        hook.on_worker_start(worker_id);
    }

    let has_recorder = shared.async_recorder.is_some();
    let park_timeout = Duration::from_micros(500);

    // Extract channel references for select! macro
    let grp_high = &group_channels.high_rx;
    let grp_norm = &group_channels.normal_rx;
    let grp_low = &group_channels.low_rx;

    // Run a job, marking this shard busy for least-loaded dispatch while it executes.
    // A stolen job counts under this (the executing) worker's shard, which is the
    // shard whose worker is actually occupied.
    let run_job = |task: Job| {
        if shared.sharded {
            shared.in_flight[group_id].fetch_add(1, Ordering::Relaxed);
            execute_job(&shared, task, has_recorder);
            shared.in_flight[group_id].fetch_sub(1, Ordering::Relaxed);
        } else {
            execute_job(&shared, task, has_recorder);
        }
    };

    loop {
        // Check shutdown first
        if shared.shutdown.load(Ordering::Acquire) {
            break;
        }

        // Phase 1: Non-blocking priority-ordered scan of own shard + global, then (in
        // the sharded config) steal from other shards so an imbalance drains promptly.
        if let Some(task) =
            try_recv_all(&group_channels, &shared.global_channels, allow_global_steal).or_else(
                || {
                    if shared.sharded {
                        try_steal_shards(&shared.group_channels, group_id, group_id + 1)
                    } else {
                        None
                    }
                },
            )
        {
            run_job(task);
            continue;
        }

        // Phase 2: Adaptive spin — stay in user-space briefly to catch burst arrivals
        // Avoids ~1-5us futex wake latency for tasks arriving shortly after Phase 1
        let mut found_in_spin = false;
        for _ in 0..spin_iterations {
            std::hint::spin_loop();
            if let Some(task) =
                try_recv_all(&group_channels, &shared.global_channels, allow_global_steal).or_else(
                    || {
                        if shared.sharded {
                            try_steal_shards(&shared.group_channels, group_id, group_id + 1)
                        } else {
                            None
                        }
                    },
                )
            {
                run_job(task);
                found_in_spin = true;
                break;
            }
        }
        if found_in_spin {
            continue;
        }

        // Phase 3: Block on channels with timeout via select!
        // crossbeam select! handles efficient OS-level park/wake.
        // When a task arrives on any monitored channel, the blocked worker
        // wakes immediately (futex-based, ~1-5us latency).
        let task = if allow_global_steal {
            let gbl_high = &shared.global_channels.high_rx;
            let gbl_norm = &shared.global_channels.normal_rx;
            let gbl_low = &shared.global_channels.low_rx;
            crossbeam_channel::select! {
                recv(grp_high) -> msg => msg.ok(),
                recv(grp_norm) -> msg => msg.ok(),
                recv(grp_low) -> msg => msg.ok(),
                recv(gbl_high) -> msg => msg.ok(),
                recv(gbl_norm) -> msg => msg.ok(),
                recv(gbl_low) -> msg => msg.ok(),
                default(park_timeout) => None,
            }
        } else {
            crossbeam_channel::select! {
                recv(grp_high) -> msg => msg.ok(),
                recv(grp_norm) -> msg => msg.ok(),
                recv(grp_low) -> msg => msg.ok(),
                default(park_timeout) => None,
            }
        };

        if let Some(task) = task {
            run_job(task);
        }
    }

    // Lifecycle hook: worker is about to exit (shutdown signalled).
    if let Some(ref hook) = shared.worker_hook {
        hook.on_worker_exit(worker_id);
    }
}

/// Execute one channel item, dispatching on its variant.
#[inline]
pub(super) fn execute_job(shared: &SharedWorkerState, job: Job, has_recorder: bool) {
    match job {
        Job::Boxed(st) => execute_boxed(shared, st, has_recorder),
        Job::Node(desc) => execute_node(shared, desc, has_recorder),
    }
}

/// Execute a typed node task via the node-executor hook (zero-alloc path).
///
/// Recording semantics match the boxed path: one Record spanning the whole
/// trampoline (initial node + any inline continuations), keyed by the job id
/// assigned at spawn and the initial node's id/slot/index.
#[inline]
fn execute_node(shared: &SharedWorkerState, desc: NodeTaskDesc, has_recorder: bool) {
    use std::sync::atomic::Ordering;

    let Some(exec) = shared.node_exec.get() else {
        // Runtime bug: Job::Node spawned before set_node_executor. Drop the
        // task but keep the pending/completed counters consistent.
        debug_assert!(false, "Job::Node received before set_node_executor");
        shared.pending_tasks.fetch_sub(1, Ordering::Relaxed);
        shared.total_completed.fetch_add(1, Ordering::Relaxed);
        return;
    };

    if has_recorder && desc.should_record {
        // Copy identity fields out before `desc` moves into the hook.
        let (job_id, task_id, slot, index) =
            (desc.job_id, desc.node.id, desc.node.slot, desc.node.index);
        let start = shared.base_instant.elapsed().as_nanos();
        exec(desc);
        let end = shared.base_instant.elapsed().as_nanos();
        let worker = WORKER_STATE.with(|s| s.get().core_id);
        submit_record(Record {
            slot,
            job_id,
            start_ns: start,
            end_ns: end,
            worker,
            task_id,
            index,
        });
    } else {
        exec(desc);
    }

    shared.pending_tasks.fetch_sub(1, Ordering::Relaxed);
    shared.total_completed.fetch_add(1, Ordering::Relaxed);
}

/// Execute a single boxed task, handling recording and metrics.
#[inline]
fn execute_boxed(shared: &SharedWorkerState, st: ScheduledTask, has_recorder: bool) {
    use std::sync::atomic::Ordering;

    if let Some(meta) = st.meta {
        if has_recorder {
            let start = shared.base_instant.elapsed().as_nanos();
            (st.task)();
            let end = shared.base_instant.elapsed().as_nanos();

            let worker = WORKER_STATE.with(|s| s.get().core_id);
            submit_record(Record {
                slot: meta.slot,
                job_id: meta.job_id,
                start_ns: start,
                end_ns: end,
                worker,
                task_id: meta.task_id,
                index: meta.index,
            });
        } else {
            (st.task)();
        }
    } else {
        (st.task)();
    }

    shared.pending_tasks.fetch_sub(1, Ordering::Relaxed);
    shared.total_completed.fetch_add(1, Ordering::Relaxed);
}
