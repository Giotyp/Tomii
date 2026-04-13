use super::channels::{ChannelSet, ScheduledTask, try_recv_all};
use crate::async_recorder::{set_worker_recorder, submit_record, AsyncRecorder};
use crate::Record;
use core_affinity::CoreId;
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;
use std::time::Instant;

/// Shared state for all workers
pub(super) struct SharedWorkerState {
    /// Global channels (fallback when group channels empty)
    pub(super) global_channels: ChannelSet,
    /// Per-group channels
    pub(super) group_channels: Vec<Arc<ChannelSet>>,
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

    let has_recorder = shared.async_recorder.is_some();
    let park_timeout = Duration::from_micros(500);

    // Extract channel references for select! macro
    let grp_high = &group_channels.high_rx;
    let grp_norm = &group_channels.normal_rx;
    let grp_low = &group_channels.low_rx;

    loop {
        // Check shutdown first
        if shared.shutdown.load(Ordering::Acquire) {
            break;
        }

        // Phase 1: Non-blocking priority-ordered scan
        if let Some(task) =
            try_recv_all(&group_channels, &shared.global_channels, allow_global_steal)
        {
            execute_task(&shared, task, has_recorder);
            continue;
        }

        // Phase 2: Adaptive spin — stay in user-space briefly to catch burst arrivals
        // Avoids ~1-5us futex wake latency for tasks arriving shortly after Phase 1
        let mut found_in_spin = false;
        for _ in 0..spin_iterations {
            std::hint::spin_loop();
            if let Some(task) =
                try_recv_all(&group_channels, &shared.global_channels, allow_global_steal)
            {
                execute_task(&shared, task, has_recorder);
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
            execute_task(&shared, task, has_recorder);
        }
    }
}

/// Execute a single scheduled task, handling recording and metrics.
#[inline]
pub(super) fn execute_task(shared: &SharedWorkerState, st: ScheduledTask, has_recorder: bool) {
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
