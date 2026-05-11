//! Example external scheduler plugin for Tomii.
//!
//! [`FifoScheduler`] implements [`tomii_core::scheduler::TaskScheduler`] using a
//! single shared FIFO work queue backed by a `Mutex<VecDeque>`. All tasks are
//! enqueued in arrival order regardless of priority; worker threads block on a
//! `Condvar` when the queue is empty.
//!
//! This is intentionally simple. It demonstrates the full `TaskScheduler` API
//! surface without any Tomii-internal dependencies. See
//! `tomii-core/PLUGIN_SCHEDULER_API.md` for the stability contract.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use tomii_core::scheduler::TaskScheduler;
// SchedulerPriority is used in spawn_task_with_priority.
// Import CoreSpec and SchedulerWorkerRange when overriding main_core or
// get_affinity_group respectively; this example uses the default impls.
use tomii_types::SchedulerPriority;

/// ABI version embedded in the crate for forward-compatibility checks.
pub const SCHEDULER_ABI_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Inner shared state
// ---------------------------------------------------------------------------

type Task = Box<dyn FnOnce() + Send + 'static>;

struct FifoInner {
    /// `Some(queue)` while running; `None` after shutdown signal.
    queue: Mutex<Option<VecDeque<Task>>>,
    condvar: Condvar,
}

// ---------------------------------------------------------------------------
// Public scheduler type
// ---------------------------------------------------------------------------

/// FIFO scheduler backed by a single shared work queue.
///
/// All tasks are dispatched in arrival order. Priority hints from
/// `spawn_task_with_priority` are accepted but ignored — this is intentional
/// to keep the implementation minimal and demonstrative.
///
/// # Construction
///
/// Use [`FifoScheduler::new`]; it returns `Arc<Self>` ready to pass to
/// `TomiiRtBuilder::new_with_plugin`.
///
/// # Shutdown
///
/// Dropping the `Arc<FifoScheduler>` sends a shutdown signal to all worker
/// threads via a `None` sentinel on the queue. Workers drain any remaining
/// tasks already dequeued before the sentinel, then exit cleanly.
pub struct FifoScheduler {
    inner: Arc<FifoInner>,
    workers: usize,
    core_offset: usize,
}

impl FifoScheduler {
    /// Spawn `workers` FIFO worker threads and return a shared handle.
    ///
    /// `core_offset` is returned verbatim from [`TaskScheduler::core_offset`];
    /// this implementation does **not** perform CPU pinning (see README for
    /// limitations).
    pub fn new(workers: usize, core_offset: usize) -> Arc<Self> {
        let inner = Arc::new(FifoInner {
            queue: Mutex::new(Some(VecDeque::new())),
            condvar: Condvar::new(),
        });

        let sched = Arc::new(FifoScheduler {
            inner: Arc::clone(&inner),
            workers,
            core_offset,
        });

        for i in 0..workers {
            let inner2 = Arc::clone(&inner);
            thread::Builder::new()
                .name(format!("fifo-worker-{i}"))
                .spawn(move || worker_loop(inner2))
                .expect("failed to spawn fifo worker");
        }

        sched
    }
}

/// Main loop executed by each worker thread.
fn worker_loop(inner: Arc<FifoInner>) {
    loop {
        let task = {
            let mut guard = inner.queue.lock().expect("fifo queue poisoned");
            loop {
                match guard.as_mut() {
                    // Shutdown sentinel — exit cleanly.
                    None => return,
                    Some(q) => {
                        if let Some(t) = q.pop_front() {
                            break t;
                        }
                        guard = inner.condvar.wait(guard).expect("condvar wait failed");
                    }
                }
            }
        };

        let name = thread::current()
            .name()
            .unwrap_or("?")
            .to_owned();
        eprintln!("[{name}] running task");
        task();
    }
}

impl Drop for FifoScheduler {
    fn drop(&mut self) {
        // Poison the queue with `None` so all waiting workers wake and exit.
        let mut guard = self.inner.queue.lock().expect("fifo queue poisoned on drop");
        *guard = None;
        self.inner.condvar.notify_all();
    }
}

// ---------------------------------------------------------------------------
// TaskScheduler impl
// ---------------------------------------------------------------------------

impl TaskScheduler for FifoScheduler {
    /// Enqueue `task` at the back of the FIFO queue.
    ///
    /// `priority` is accepted but ignored — all tasks are served in arrival
    /// order. Override this method if you need priority-aware dispatch.
    fn spawn_task_with_priority(
        &self,
        _priority: SchedulerPriority,
        task: Box<dyn FnOnce() + Send + 'static>,
    ) {
        let mut guard = self.inner.queue.lock().expect("fifo queue poisoned");
        if let Some(q) = guard.as_mut() {
            q.push_back(task);
            self.inner.condvar.notify_one();
        }
        // If the queue is None (shutdown in progress) we silently drop the task.
    }

    // `spawn_to_group` and `get_affinity_group` use their default impls:
    // - `spawn_to_group` delegates to `spawn_task_with_priority`.
    // - `get_affinity_group` returns 0 (global pool).

    fn workers(&self) -> usize {
        self.workers
    }

    fn core_offset(&self) -> usize {
        self.core_offset
    }

    fn system_threads(&self) -> usize {
        1
    }

    fn receiver_core_offset(&self) -> usize {
        0
    }

    fn receiver_threads(&self) -> usize {
        0
    }

    // `write_record` and `main_core` use their default no-op / None impls.
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Verify that all spawned tasks complete and the counter reaches `n`.
    /// Order of completion is non-deterministic with multiple workers, so we
    /// collect results into a sorted vec and compare against 0..n.
    #[test]
    fn fifo_runs_tasks_in_order() {
        let sched = FifoScheduler::new(2, 0);
        let counter = Arc::new(AtomicUsize::new(0));
        let n = 20;
        let (tx, rx) = std::sync::mpsc::channel();

        for i in 0..n {
            let c = Arc::clone(&counter);
            let tx2 = tx.clone();
            sched.spawn_task_with_priority(
                SchedulerPriority::Normal,
                Box::new(move || {
                    c.fetch_add(1, Ordering::Relaxed);
                    let _ = tx2.send(i);
                }),
            );
        }
        drop(tx);

        let mut results: Vec<usize> = rx.iter().collect();
        results.sort_unstable();
        assert_eq!(results, (0..n).collect::<Vec<_>>());
        assert_eq!(counter.load(Ordering::Relaxed), n);
    }

    /// Smoke-test that the scheduler accepts tasks after being cloned via Arc.
    #[test]
    fn spawn_via_arc_clone() {
        let sched = FifoScheduler::new(1, 0);
        let (tx, rx) = std::sync::mpsc::channel::<()>();

        let sched2: Arc<dyn TaskScheduler> = Arc::clone(&sched) as Arc<dyn TaskScheduler>;
        sched2.spawn_task_with_priority(
            SchedulerPriority::High,
            Box::new(move || {
                let _ = tx.send(());
            }),
        );

        rx.recv().expect("task did not complete");
    }

    /// Verify that dropping the scheduler shuts down worker threads cleanly —
    /// no panic or deadlock after Drop.
    #[test]
    fn drop_is_clean() {
        let sched = FifoScheduler::new(4, 0);
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let b2 = Arc::clone(&barrier);

        sched.spawn_task_with_priority(
            SchedulerPriority::Low,
            Box::new(move || {
                b2.wait();
            }),
        );
        barrier.wait(); // ensure at least one task ran before drop
        drop(sched);    // shutdown signal sent; workers will exit
    }
}
