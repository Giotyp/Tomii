//! Per-worker lifecycle hook — the Taskflow `WorkerInterface` analog.
//!
//! A [`WorkerHook`] is passed to the scheduler at construction time via
//! [`crate::scheduler::SchedulerConfig::worker_hook`] and is invoked **on the
//! worker thread itself**, once at startup (after CPU affinity and
//! thread-locals are set, before the first task runs) and once at shutdown.
//! Typical uses: NUMA-local allocator setup, per-worker logging/profiling
//! registration, or attaching an embedded interpreter's thread state.
//!
//! Both methods default to no-ops, and the hook is only touched at thread
//! start/exit — it adds zero cost to the task hot path.

/// Callbacks invoked on each worker thread at lifecycle boundaries.
///
/// `worker_index` is the pool-local index in `0..workers` (the same index
/// reported by the scheduler's per-worker metrics), not a CPU core id.
pub trait WorkerHook: Send + Sync {
    /// Called on the worker thread after it is pinned and initialized,
    /// before it executes any task.
    fn on_worker_start(&self, worker_index: usize) {
        let _ = worker_index;
    }

    /// Called on the worker thread just before it exits.
    fn on_worker_exit(&self, worker_index: usize) {
        let _ = worker_index;
    }
}
