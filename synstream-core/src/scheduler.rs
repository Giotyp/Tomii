#![allow(unused_imports)]
#![allow(dead_code)]
use core_affinity;
use rayon::{prelude::*, vec};
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

pub trait Scheduler {
    fn spawn_task<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static;

    fn workers(&self) -> usize {
        // Default implementation returns 1 worker
        1
    }

    /// Get the number of jobs currently pending/executing in the pool
    fn pending_jobs(&self) -> usize {
        0 // Default implementation
    }

    /// Get the total number of jobs spawned since creation
    fn total_jobs_spawned(&self) -> usize {
        0 // Default implementation
    }

    /// Get the total number of jobs completed since creation
    fn total_jobs_completed(&self) -> usize {
        0 // Default implementation
    }
}

pub struct FifoScheduler {
    threadpool: ThreadPool,
    pending_jobs: Arc<AtomicUsize>,
    total_spawned: Arc<AtomicUsize>,
    total_completed: Arc<AtomicUsize>,
}

impl FifoScheduler {
    fn new(core_offset: usize, workers: usize) -> Self {
        // Create threadpool and pin workers to cores
        let mut core_ids = core_affinity::get_core_ids().unwrap();
        core_ids.sort();
        let cores_to_use: Vec<core_affinity::CoreId> =
            core_ids[core_offset..core_offset + workers].to_vec();

        let threadpool = ThreadPoolBuilder::new()
            .num_threads(workers)
            .start_handler(move |thread_index| {
                // Pin each thread to a specific core
                let core_id = cores_to_use[thread_index];
                core_affinity::set_for_current(core_id);
            })
            .build()
            .unwrap();

        Self {
            threadpool,
            pending_jobs: Arc::new(AtomicUsize::new(0)),
            total_spawned: Arc::new(AtomicUsize::new(0)),
            total_completed: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Scheduler for FifoScheduler {
    fn spawn_task<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.pending_jobs.fetch_add(1, Ordering::SeqCst);
        self.total_spawned.fetch_add(1, Ordering::SeqCst);

        let pending = Arc::clone(&self.pending_jobs);
        let completed = Arc::clone(&self.total_completed);

        self.threadpool.spawn_fifo(move || {
            task();
            pending.fetch_sub(1, Ordering::SeqCst);
            completed.fetch_add(1, Ordering::SeqCst);
        });
    }

    fn workers(&self) -> usize {
        self.threadpool.current_num_threads()
    }

    fn pending_jobs(&self) -> usize {
        self.pending_jobs.load(Ordering::SeqCst)
    }

    fn total_jobs_spawned(&self) -> usize {
        self.total_spawned.load(Ordering::SeqCst)
    }

    fn total_jobs_completed(&self) -> usize {
        self.total_completed.load(Ordering::SeqCst)
    }
}

pub struct WorkStealScheduler {
    threadpool: ThreadPool,
    pending_jobs: Arc<AtomicUsize>,
    total_spawned: Arc<AtomicUsize>,
    total_completed: Arc<AtomicUsize>,
}

impl WorkStealScheduler {
    fn new(core_offset: usize, workers: usize) -> Self {
        // Create threadpool and pin workers to cores
        let mut core_ids = core_affinity::get_core_ids().unwrap();
        core_ids.sort();
        let cores_to_use: Vec<core_affinity::CoreId> =
            core_ids[core_offset..core_offset + workers].to_vec();

        let threadpool = ThreadPoolBuilder::new()
            .num_threads(workers)
            .start_handler(move |thread_index| {
                // Pin each thread to a specific core
                let core_id = cores_to_use[thread_index];
                core_affinity::set_for_current(core_id);
            })
            .build()
            .unwrap();

        Self {
            threadpool,
            pending_jobs: Arc::new(AtomicUsize::new(0)),
            total_spawned: Arc::new(AtomicUsize::new(0)),
            total_completed: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Scheduler for WorkStealScheduler {
    fn spawn_task<F>(&self, task_clos: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.pending_jobs.fetch_add(1, Ordering::SeqCst);
        self.total_spawned.fetch_add(1, Ordering::SeqCst);

        let pending = Arc::clone(&self.pending_jobs);
        let completed = Arc::clone(&self.total_completed);

        self.threadpool.spawn(move || {
            task_clos();
            pending.fetch_sub(1, Ordering::SeqCst);
            completed.fetch_add(1, Ordering::SeqCst);
        });
    }

    fn workers(&self) -> usize {
        self.threadpool.current_num_threads()
    }

    fn pending_jobs(&self) -> usize {
        self.pending_jobs.load(Ordering::SeqCst)
    }

    fn total_jobs_spawned(&self) -> usize {
        self.total_spawned.load(Ordering::SeqCst)
    }

    fn total_jobs_completed(&self) -> usize {
        self.total_completed.load(Ordering::SeqCst)
    }
}

pub enum SchedulerImpl {
    Fifo(FifoScheduler),
    WorkStealing(WorkStealScheduler),
}

impl Scheduler for SchedulerImpl {
    fn spawn_task<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        match self {
            SchedulerImpl::Fifo(scheduler) => scheduler.spawn_task(task),
            SchedulerImpl::WorkStealing(scheduler) => scheduler.spawn_task(task),
        }
    }

    fn workers(&self) -> usize {
        match self {
            SchedulerImpl::Fifo(scheduler) => scheduler.workers(),
            SchedulerImpl::WorkStealing(scheduler) => scheduler.workers(),
        }
    }

    fn pending_jobs(&self) -> usize {
        match self {
            SchedulerImpl::Fifo(scheduler) => scheduler.pending_jobs(),
            SchedulerImpl::WorkStealing(scheduler) => scheduler.pending_jobs(),
        }
    }

    fn total_jobs_spawned(&self) -> usize {
        match self {
            SchedulerImpl::Fifo(scheduler) => scheduler.total_jobs_spawned(),
            SchedulerImpl::WorkStealing(scheduler) => scheduler.total_jobs_spawned(),
        }
    }

    fn total_jobs_completed(&self) -> usize {
        match self {
            SchedulerImpl::Fifo(scheduler) => scheduler.total_jobs_completed(),
            SchedulerImpl::WorkStealing(scheduler) => scheduler.total_jobs_completed(),
        }
    }
}

pub enum SchedulerType {
    Fifo,
    WorkStealing,
}

pub fn create_scheduler(
    scheduler_type: SchedulerType,
    core_offset: usize,
    num_workers: usize,
) -> SchedulerImpl {
    match scheduler_type {
        SchedulerType::Fifo => SchedulerImpl::Fifo(FifoScheduler::new(core_offset, num_workers)),
        SchedulerType::WorkStealing => {
            SchedulerImpl::WorkStealing(WorkStealScheduler::new(core_offset, num_workers))
        }
    }
}
