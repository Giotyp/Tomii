#![allow(unused_imports)]
#![allow(dead_code)]
use core_affinity;
use rayon::{prelude::*, vec};
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::thread;

pub trait Scheduler {
    fn spawn_task<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static;

    fn workers(&self) -> usize {
        // Default implementation returns 1 worker
        1
    }
}

pub struct FifoScheduler {
    threadpool: ThreadPool,
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

        Self { threadpool }
    }
}

impl Scheduler for FifoScheduler {
    fn spawn_task<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.threadpool.spawn_fifo(move || {
            task();
        });
    }

    fn workers(&self) -> usize {
        self.threadpool.current_num_threads()
    }
}

pub struct WorkStealScheduler {
    threadpool: ThreadPool,
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

        Self { threadpool }
    }
}

impl Scheduler for WorkStealScheduler {
    fn spawn_task<F>(&self, task_clos: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.threadpool.spawn(move || {
            task_clos();
        });
    }

    fn workers(&self) -> usize {
        self.threadpool.current_num_threads()
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
