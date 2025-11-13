#![allow(unused_imports)]
#![allow(dead_code)]
use core_affinity;
use rayon::{prelude::*, vec};
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::cell::Cell;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

use crate::IdType;

thread_local! {
    // Worker id assigned to each thread in the pool. usize::MAX means unassigned.
    static WORKER_ID: Cell<usize> = Cell::new(usize::MAX);
}

/// Get the current thread's worker id if assigned by the scheduler
pub fn get_current_worker_id() -> Option<usize> {
    let id = WORKER_ID.with(|c| c.get());
    if id == usize::MAX {
        None
    } else {
        Some(id)
    }
}

pub trait Scheduler {
    fn spawn_task<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static;

    /// Optional: spawn task with metadata tuple (task_id, slot, index). Default delegates to `spawn_task`.
    fn spawn_task_with_meta<F>(&self, _meta: Option<(IdType, usize, usize)>, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.spawn_task(task)
    }

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

#[derive(Debug, Clone)]
struct Record {
    job_id: usize,
    start_ns: u128,
    end_ns: u128,
    worker: usize,
    task_id: IdType,
    index: usize,
}

/// Recorder type: maps "slot" -> Vec<Record>
type Recorder = Arc<Mutex<HashMap<usize, Vec<Record>>>>;

pub struct FifoScheduler {
    threadpool: ThreadPool,
    pending_jobs: Arc<AtomicUsize>,
    total_spawned: Arc<AtomicUsize>,
    total_completed: Arc<AtomicUsize>,

    // low-overhead recorder: optional
    recorder: Option<Recorder>,
    base_instant: Arc<Instant>,
}

impl FifoScheduler {
    fn new(core_offset: usize, workers: usize, record: bool) -> Self {
        // Create threadpool and pin workers to cores
        let mut core_ids = core_affinity::get_core_ids().unwrap();
        core_ids.sort();

        let available_cores = core_ids.len();

        // If requested workers exceed available cores, ignore offset and use max-1 workers
        let (actual_offset, actual_workers) = if core_offset + workers > available_cores {
            eprintln!(
                "Warning: Requested {} workers with offset {} exceeds available {} cores. \n                 Using {} workers (max-1) with no offset.",
                workers,
                core_offset,
                available_cores,
                available_cores.saturating_sub(1)
            );
            (0, available_cores.saturating_sub(1))
        } else {
            (core_offset, workers)
        };

        let cores_to_use: Vec<core_affinity::CoreId> =
            core_ids[actual_offset..actual_offset + actual_workers].to_vec();

        // Print worker->core correspondence
        println!("FifoScheduler: Worker -> Core Mapping:");
        for (idx, core_id) in cores_to_use.iter().enumerate() {
            println!("  Worker {}: Core {:?}", idx, core_id);
        }

        let threadpool = ThreadPoolBuilder::new()
            .num_threads(actual_workers)
            .start_handler(move |thread_index| {
                // Assign a worker id to this thread for timing attribution
                WORKER_ID.with(|c| c.set(thread_index));
                // Pin each thread to a specific core
                let core_id = cores_to_use[thread_index];
                core_affinity::set_for_current(core_id);
            })
            .build()
            .unwrap();

        let recorder = if record {
            Some(Arc::new(Mutex::new(HashMap::new())))
        } else {
            None
        };

        Self {
            threadpool,
            pending_jobs: Arc::new(AtomicUsize::new(0)),
            total_spawned: Arc::new(AtomicUsize::new(0)),
            total_completed: Arc::new(AtomicUsize::new(0)),
            recorder,
            base_instant: Arc::new(Instant::now()),
        }
    }

    fn write_records_to_csv(&self, path: &str) {
        if let Some(rec) = &self.recorder {
            if let Ok(map) = rec.lock() {
                if map.is_empty() {
                    println!("FifoScheduler: no recorded events to write");
                    return;
                }
                match File::create(path) {
                    Ok(mut f) => {
                        let _ = writeln!(f, "slot,job_id,start_ns,end_ns,worker,task_id,index");
                        for (slot, vec) in map.iter() {
                            for r in vec.iter() {
                                let _ = writeln!(
                                    f,
                                    "{},{},{},{},{},{},{}",
                                    slot,
                                    r.job_id,
                                    r.start_ns,
                                    r.end_ns,
                                    r.worker,
                                    r.task_id,
                                    r.index
                                );
                            }
                        }
                        println!("FifoScheduler: wrote {} slots to {}", map.len(), path);
                    }
                    Err(e) => {
                        eprintln!("FifoScheduler: failed to create {}: {}", path, e);
                    }
                }
            }
        } else {
            println!("FifoScheduler: recorder not enabled");
        }
    }
}

impl Scheduler for FifoScheduler {
    fn spawn_task<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        // delegate to spawn_task_with_meta with no metadata
        self.spawn_task_with_meta(None, task)
    }

    fn spawn_task_with_meta<F>(&self, meta: Option<(IdType, usize, usize)>, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        // job id
        let job_id = self.total_spawned.fetch_add(1, Ordering::SeqCst);
        self.pending_jobs.fetch_add(1, Ordering::SeqCst);

        let pending = Arc::clone(&self.pending_jobs);
        let completed = Arc::clone(&self.total_completed);
        let base = Arc::clone(&self.base_instant);
        let recorder_opt = self.recorder.as_ref().map(Arc::clone);

        // parse meta: expected format "slot=<num>;name=<task>"; fallback slot=usize::MAX
        let (task_id, slot, index) = if let Some(data) = meta {
            data
        } else {
            (IdType::MIN, usize::MIN, usize::MIN)
        };

        self.threadpool.spawn_fifo(move || {
            let start = (*base).elapsed().as_nanos();
            let worker = get_current_worker_id().unwrap_or(usize::MAX);
            task();
            let end = (*base).elapsed().as_nanos();

            if let Some(rec) = recorder_opt.as_ref() {
                if let Ok(mut map) = rec.lock() {
                    let vec = map.entry(slot).or_insert_with(Vec::new);
                    vec.push(Record {
                        job_id,
                        start_ns: start,
                        end_ns: end,
                        worker,
                        task_id,
                        index,
                    });
                }
            }

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

    recorder: Option<Recorder>,
    base_instant: Arc<Instant>,
}

impl WorkStealScheduler {
    fn new(core_offset: usize, workers: usize, record: bool) -> Self {
        // Create threadpool and pin workers to cores
        let mut core_ids = core_affinity::get_core_ids().unwrap();
        core_ids.sort();

        let available_cores = core_ids.len();

        // If requested workers exceed available cores, ignore offset and use max-1 workers
        let (actual_offset, actual_workers) = if core_offset + workers > available_cores {
            eprintln!(
                "Warning: Requested {} workers with offset {} exceeds available {} cores. \n                 Using {} workers (max-1) with no offset.",
                workers,
                core_offset,
                available_cores,
                available_cores.saturating_sub(1)
            );
            (0, available_cores.saturating_sub(1))
        } else {
            (core_offset, workers)
        };

        let cores_to_use: Vec<core_affinity::CoreId> =
            core_ids[actual_offset..actual_offset + actual_workers].to_vec();

        // Print worker->core correspondence
        println!("WorkStealScheduler: Worker -> Core Mapping:");
        for (idx, core_id) in cores_to_use.iter().enumerate() {
            println!("  Worker {}: Core {:?}", idx, core_id);
        }

        let threadpool = ThreadPoolBuilder::new()
            .num_threads(actual_workers)
            .start_handler(move |thread_index| {
                // Assign a worker id to this thread for timing attribution
                WORKER_ID.with(|c| c.set(thread_index));
                // Pin each thread to a specific core
                let core_id = cores_to_use[thread_index];
                core_affinity::set_for_current(core_id);
            })
            .build()
            .unwrap();

        let recorder = if record {
            Some(Arc::new(Mutex::new(HashMap::new())))
        } else {
            None
        };

        Self {
            threadpool,
            pending_jobs: Arc::new(AtomicUsize::new(0)),
            total_spawned: Arc::new(AtomicUsize::new(0)),
            total_completed: Arc::new(AtomicUsize::new(0)),
            recorder,
            base_instant: Arc::new(Instant::now()),
        }
    }

    fn write_records_to_csv(&self, path: &str) {
        if let Some(rec) = &self.recorder {
            if let Ok(map) = rec.lock() {
                if map.is_empty() {
                    println!("WorkStealScheduler: no recorded events to write");
                    return;
                }
                match File::create(path) {
                    Ok(mut f) => {
                        let _ = writeln!(f, "slot,job_id,start_ns,end_ns,worker,task_id,index");
                        for (slot, vec) in map.iter() {
                            for r in vec.iter() {
                                let _ = writeln!(
                                    f,
                                    "{},{},{},{},{},{},{}",
                                    slot,
                                    r.job_id,
                                    r.start_ns,
                                    r.end_ns,
                                    r.worker,
                                    r.task_id,
                                    r.index
                                );
                            }
                        }
                        println!("WorkStealScheduler: wrote {} slots to {}", map.len(), path);
                    }
                    Err(e) => {
                        eprintln!("WorkStealScheduler: failed to create {}: {}", path, e);
                    }
                }
            }
        } else {
            println!("WorkStealScheduler: recorder not enabled");
        }
    }
}

impl Scheduler for WorkStealScheduler {
    fn spawn_task<F>(&self, task_clos: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.spawn_task_with_meta(None, task_clos)
    }

    fn spawn_task_with_meta<F>(&self, meta: Option<(IdType, usize, usize)>, task_clos: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job_id = self.total_spawned.fetch_add(1, Ordering::SeqCst);
        self.pending_jobs.fetch_add(1, Ordering::SeqCst);

        let pending = Arc::clone(&self.pending_jobs);
        let completed = Arc::clone(&self.total_completed);
        let base = Arc::clone(&self.base_instant);
        let recorder_opt = self.recorder.as_ref().map(Arc::clone);

        let (task_id, slot, index) = if let Some(data) = meta {
            data
        } else {
            (IdType::MIN, usize::MIN, usize::MIN)
        };

        self.threadpool.spawn(move || {
            let start = (*base).elapsed().as_nanos();
            let worker = get_current_worker_id().unwrap_or(usize::MAX);
            task_clos();
            let end = (*base).elapsed().as_nanos();

            if let Some(rec) = recorder_opt.as_ref() {
                if let Ok(mut map) = rec.lock() {
                    let vec = map.entry(slot).or_insert_with(Vec::new);
                    vec.push(Record {
                        job_id,
                        start_ns: start,
                        end_ns: end,
                        worker,
                        task_id,
                        index,
                    });
                }
            }

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

    fn spawn_task_with_meta<F>(&self, meta: Option<(IdType, usize, usize)>, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        match self {
            SchedulerImpl::Fifo(scheduler) => scheduler.spawn_task_with_meta(meta, task),
            SchedulerImpl::WorkStealing(scheduler) => scheduler.spawn_task_with_meta(meta, task),
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

impl SchedulerImpl {
    /// Dump recorded schedule to CSV at `path` (slot,job_id,start_ns,end_ns,worker,task_name)
    pub fn write_record(&self, path: &str) {
        match self {
            SchedulerImpl::Fifo(s) => s.write_records_to_csv(path),
            SchedulerImpl::WorkStealing(s) => s.write_records_to_csv(path),
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
    record: bool,
) -> SchedulerImpl {
    match scheduler_type {
        SchedulerType::Fifo => {
            SchedulerImpl::Fifo(FifoScheduler::new(core_offset, num_workers, record))
        }
        SchedulerType::WorkStealing => {
            SchedulerImpl::WorkStealing(WorkStealScheduler::new(core_offset, num_workers, record))
        }
    }
}
