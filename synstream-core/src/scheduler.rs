#![allow(unused_imports)]
#![allow(dead_code)]
use core_affinity;
use crossbeam_channel::Sender;
use rayon::{prelude::*, vec};
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::cell::Cell;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::async_recorder::{set_worker_recorder, submit_record, AsyncRecorder};
use crate::batch_queue::{Receiver as BatchReceiver, Sender as BatchSender};
use crate::buffers::NodeInfo;
use crate::debug::print_debug;
use crate::{IdType, Record};
use synstream_types::CmTypes;

thread_local! {
    // Physical core ID where this thread is pinned. usize::MAX means unassigned.
    static WORKER_ID: Cell<usize> = Cell::new(usize::MAX);
    // Worker thread index (0-based) for metrics. usize::MAX means not a worker thread.
    static WORKER_INDEX: Cell<usize> = Cell::new(usize::MAX);
}

/// Get the current thread's physical core ID
pub fn get_current_worker_id() -> Option<usize> {
    let id = WORKER_ID.with(|c| c.get());
    if id == usize::MAX {
        None
    } else {
        Some(id)
    }
}

/// Get the current thread's worker index (0-based, for metrics)
pub fn get_current_worker_index() -> Option<usize> {
    let idx = WORKER_INDEX.with(|c| c.get());
    if idx == usize::MAX {
        None
    } else {
        Some(idx)
    }
}

/// Set the current thread's physical core ID
pub fn set_current_worker_id(core_id: usize) {
    WORKER_ID.with(|c| c.set(core_id));
}

/// Set the current thread's worker index
pub fn set_current_worker_index(index: usize) {
    WORKER_INDEX.with(|c| c.set(index));
}

/// Create Threadpool with Rayon
/// Returns: (ThreadPool, system_core_offset, worker_core_offset)
pub fn create_threadpool(
    core_offset: usize,
    workers: usize,
    receiver_threads: usize,
    system_threads: usize,
    async_recorder: Option<Arc<AsyncRecorder>>,
) -> (
    ThreadPool,
    usize,
    usize,
    usize,
    usize,
    usize,
    Option<core_affinity::CoreId>,
) {
    // Create threadpool and pin workers to cores
    let mut core_ids = core_affinity::get_core_ids().unwrap();
    core_ids.sort();

    let available_cores = core_ids.len();

    // CRITICAL: Allocate cores in sequential order: [system][receivers][workers]
    // With core_offset=1, system=1, receivers=2, workers=10:
    //   System: core 1
    //   Receivers: cores 2-3 (allocated in runtime.rs, not here)
    //   Workers: cores 4-13

    let total_needed = system_threads + receiver_threads + workers;

    let (
        system_core_offset,
        receiver_offset,
        worker_offset,
        actual_workers,
        actual_receivers,
        actual_system_threads,
        main_core_opt,
    ) = if available_cores < 2 {
        panic!(
            "Insufficient cores: need minimum 2 cores (1 system + 1 worker), found {}",
            available_cores
        );
    } else if core_offset + total_needed + 1 <= available_cores {
        // We can reserve an extra core for the main thread at `core_offset`.
        let main_idx = core_offset;
        let sys_start = core_offset + 1;
        let recv_start = sys_start + system_threads;
        let worker_start = recv_start + receiver_threads;
        (
            sys_start,
            recv_start,
            worker_start,
            workers,
            receiver_threads,
            system_threads,
            Some(core_ids[main_idx].clone()),
        )
    } else if core_offset + total_needed <= available_cores {
        // Can honor requested offset but no spare core for main
        let sys_start = core_offset;
        let recv_start = core_offset + system_threads;
        let worker_start = recv_start + receiver_threads;
        (
            sys_start,
            recv_start,
            worker_start,
            workers,
            receiver_threads,
            system_threads,
            None,
        )
    } else if total_needed <= available_cores {
        // Fit all threads but not with requested offset: use offset 0
        eprintln!(
            "Warning: Cannot honor core_offset {}. Using offset 0 instead.",
            core_offset
        );
        let sys_start = 0;
        let recv_start = system_threads;
        let worker_start = recv_start + receiver_threads;
        (
            sys_start,
            recv_start,
            worker_start,
            workers,
            receiver_threads,
            system_threads,
            None,
        )
    } else {
        // Not enough cores: reduce proportionally
        let max_system = 1; // at least one system thread
        let remaining = available_cores.saturating_sub(max_system);
        let max_receivers = receiver_threads.min(remaining / 2).max(0);
        let max_workers = remaining.saturating_sub(max_receivers).max(1);
        eprintln!(
                "Warning: Requested {} system + {} receivers + {} workers = {} total exceeds {} available cores.\nUsing {} system at core 0, {} receivers starting at core {}, {} workers starting at core {}.",
                system_threads, receiver_threads, workers, total_needed, available_cores,
                max_system, max_receivers, max_system, max_workers, max_system + max_receivers
            );
        (
            0,
            max_system,
            max_system + max_receivers,
            max_workers,
            max_receivers,
            max_system,
            None,
        )
    };

    // VERIFICATION: Ensure proper sequential allocation with no overlaps
    assert!(
        system_core_offset + actual_system_threads <= receiver_offset,
        "Core allocation bug: system cores [{}..{}) overlap with receiver cores [{}..{})",
        system_core_offset,
        system_core_offset + actual_system_threads,
        receiver_offset,
        receiver_offset + actual_receivers
    );
    assert!(
        receiver_offset + actual_receivers <= worker_offset,
        "Core allocation bug: receiver cores [{}..{}) overlap with worker cores [{}..{})",
        receiver_offset,
        receiver_offset + actual_receivers,
        worker_offset,
        worker_offset + actual_workers
    );

    let worker_cores_to_use: Vec<core_affinity::CoreId> =
        core_ids[worker_offset..worker_offset + actual_workers].to_vec();

    // Print core allocation
    println!("========== Core Allocation ==========");
    println!("Available cores: {}", available_cores);
    if let Some(main_core) = main_core_opt.clone() {
        println!("Main thread: pinned at core {:?}", main_core);
    }
    println!(
        "System threads: {} at cores {}..{}",
        actual_system_threads,
        system_core_offset,
        system_core_offset + actual_system_threads - 1
    );
    println!(
        "Receiver threads: {} at cores {}..{} (managed by runtime, not Rayon)",
        actual_receivers,
        receiver_offset,
        receiver_offset + actual_receivers - 1
    );
    println!(
        "Worker threads: {} at cores {}..{}",
        actual_workers,
        worker_offset,
        worker_offset + actual_workers - 1
    );
    println!("Worker -> Core Mapping:");
    for (idx, core_id) in worker_cores_to_use.iter().enumerate() {
        println!("  Worker {}: Core {:?}", idx, core_id);
    }
    println!("======================================");

    let recorder_clone = async_recorder.clone();
    let worker_threadpool = ThreadPoolBuilder::new()
        .num_threads(actual_workers)
        .start_handler(move |thread_index| {
            // Pin to core
            let core_id = worker_cores_to_use[thread_index];
            core_affinity::set_for_current(core_id);

            // Set WORKER_ID to physical core ID (for CSV recording)
            WORKER_ID.with(|c| c.set(core_id.id));
            // Set WORKER_INDEX to thread index (for metrics array indexing)
            WORKER_INDEX.with(|c| c.set(thread_index));

            // Universal channel indexing: channel_index = physical_core_id - system_core_offset
            let channel_index = core_id.id - system_core_offset;
            if let Some(ref recorder) = recorder_clone {
                if let Some(tx) = recorder.get_worker_sender(channel_index) {
                    set_worker_recorder(tx);
                }
            }
        })
        .build()
        .unwrap();

    (
        worker_threadpool,
        system_core_offset,
        actual_system_threads,
        receiver_offset,
        actual_receivers,
        worker_offset,
        main_core_opt,
    )
}

/// Per-worker utilization tracking (optional, only when recording enabled)
#[derive(Debug)]
pub struct WorkerMetrics {
    /// Number of tasks executed by each worker
    tasks_per_worker: Vec<AtomicUsize>,
    /// Cumulative idle time per worker (in nanoseconds)
    idle_time_per_worker: Vec<AtomicUsize>,
    /// Last timestamp when worker became idle
    last_idle_timestamp: Vec<Mutex<Option<Instant>>>,
}

impl WorkerMetrics {
    fn new(num_workers: usize) -> Self {
        Self {
            tasks_per_worker: (0..num_workers).map(|_| AtomicUsize::new(0)).collect(),
            idle_time_per_worker: (0..num_workers).map(|_| AtomicUsize::new(0)).collect(),
            last_idle_timestamp: (0..num_workers).map(|_| Mutex::new(None)).collect(),
        }
    }

    fn record_task_start(&self, worker_idx: usize) {
        // Worker transitioning from idle to busy
        if let Some(idle_start) = self.last_idle_timestamp[worker_idx].lock().take() {
            let idle_duration = idle_start.elapsed().as_nanos() as usize;
            self.idle_time_per_worker[worker_idx].fetch_add(idle_duration, Ordering::Relaxed);
        }
    }

    fn record_task_complete(&self, worker_idx: usize) {
        self.tasks_per_worker[worker_idx].fetch_add(1, Ordering::Relaxed);
        // Worker now idle
        *self.last_idle_timestamp[worker_idx].lock() = Some(Instant::now());
    }

    pub fn print_stats(&self) {
        println!("\n=== Worker Utilization Statistics ===");
        for (idx, (tasks, idle_ns)) in self
            .tasks_per_worker
            .iter()
            .zip(self.idle_time_per_worker.iter())
            .enumerate()
        {
            let task_count = tasks.load(Ordering::Relaxed);
            let idle_us = idle_ns.load(Ordering::Relaxed) / 1000;
            println!("Worker {}: {} tasks, {}µs idle", idx, task_count, idle_us);
        }

        // Calculate load imbalance
        let max_tasks = self
            .tasks_per_worker
            .iter()
            .map(|a| a.load(Ordering::Relaxed))
            .max()
            .unwrap_or(0);
        let min_tasks = self
            .tasks_per_worker
            .iter()
            .map(|a| a.load(Ordering::Relaxed))
            .min()
            .unwrap_or(0);

        if max_tasks > 0 {
            let imbalance = ((max_tasks - min_tasks) as f64 / max_tasks as f64) * 100.0;
            println!("Load imbalance: {:.2}%", imbalance);
        }
        println!("=====================================\n");
    }
}

/// Shared base for schedulers with common state and logic.
#[derive(Debug)]
struct SchedulerBase {
    threadpool: ThreadPool,
    system_core_offset: usize,
    system_threads: usize,
    receiver_core_offset: usize,
    receiver_threads: usize,
    worker_core_offset: usize,
    // Optional reserved core for main/orchestrator thread
    main_core: Option<core_affinity::CoreId>,
    pending_jobs: Arc<AtomicUsize>,
    total_spawned: Arc<AtomicUsize>,
    total_completed: Arc<AtomicUsize>,
    async_recorder: Option<Arc<AsyncRecorder>>,
    base_instant: Arc<Instant>,
    // Batch queue for lock-free task completion delivery
    batch_queue_tx: BatchSender<(NodeInfo, CmTypes)>,
    batch_queue_rx: Arc<BatchReceiver<(NodeInfo, CmTypes)>>,
    target_batch_size: usize,
    batch_timeout_us: u64,
    // Stream-specific recording filter
    record_stream: Option<usize>,
    available_stream_slots: Arc<parking_lot::RwLock<Vec<usize>>>,
    // Phase 4: Worker utilization metrics (optional)
    worker_metrics: Option<Arc<WorkerMetrics>>,
}

impl SchedulerBase {
    fn new(
        core_offset: usize,
        workers: usize,
        record: bool,
        external_recorder: Option<Arc<AsyncRecorder>>,
        base_instant: Instant,
        system_threads: usize,
        receiver_threads: usize,
        target_batch_size: usize,
        batch_timeout_us: u64,
        record_stream: Option<usize>,
        available_stream_slots: Arc<parking_lot::RwLock<Vec<usize>>>,
    ) -> Self {
        let total_recorders = workers + receiver_threads + system_threads;
        let async_recorder = if record {
            match external_recorder {
                Some(r) => Some(r),
                None => Some(Arc::new(AsyncRecorder::new(total_recorders, 100))),
            }
        } else {
            None
        };

        let (
            worker_threadpool,
            system_core_offset,
            system_threads,
            receiver_core_offset,
            receiver_threads,
            worker_core_offset,
            main_core,
        ) = create_threadpool(
            core_offset,
            workers,
            receiver_threads,
            system_threads,
            async_recorder.clone(),
        );

        // Create batch_queue for lock-free task completion delivery
        let (batch_queue_tx, batch_queue_rx) = crate::batch_queue::unbounded();

        // Phase 4: Initialize worker metrics (only when recording enabled)
        let worker_metrics = if record {
            Some(Arc::new(WorkerMetrics::new(workers)))
        } else {
            None
        };

        Self {
            threadpool: worker_threadpool,
            system_core_offset,
            system_threads,
            receiver_core_offset,
            receiver_threads,
            worker_core_offset,
            main_core,
            pending_jobs: Arc::new(AtomicUsize::new(0)),
            total_spawned: Arc::new(AtomicUsize::new(0)),
            total_completed: Arc::new(AtomicUsize::new(0)),
            async_recorder,
            base_instant: Arc::new(base_instant),
            batch_queue_tx,
            batch_queue_rx: Arc::new(batch_queue_rx),
            target_batch_size,
            batch_timeout_us,
            record_stream,
            available_stream_slots,
            worker_metrics,
        }
    }

    fn write_records_to_csv(&self, path: &str) {
        if let Some(recorder) = &self.async_recorder {
            match recorder.write_to_csv(path) {
                Ok(()) => {}
                Err(e) => eprintln!("Failed to write records: {}", e),
            }
        } else {
            println!("SchedulerBase: recorder not enabled");
        }
    }

    fn workers(&self) -> usize {
        self.threadpool.current_num_threads()
    }

    fn core_offset(&self) -> usize {
        self.system_core_offset
    }

    fn system_threads(&self) -> usize {
        self.system_threads
    }

    fn receiver_core_offset(&self) -> usize {
        self.receiver_core_offset
    }

    fn receiver_threads(&self) -> usize {
        self.receiver_threads
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

    fn get_batch_queue_tx(&self) -> BatchSender<(NodeInfo, CmTypes)> {
        self.batch_queue_tx.clone()
    }

    fn get_batch_queue_rx(&self) -> Arc<BatchReceiver<(NodeInfo, CmTypes)>> {
        Arc::clone(&self.batch_queue_rx)
    }

    fn get_target_batch_size(&self) -> usize {
        self.target_batch_size
    }

    fn get_batch_timeout_us(&self) -> u64 {
        self.batch_timeout_us
    }

    /// Common task spawning logic. `spawn_fn` handles the specific spawning (e.g., FIFO or work-stealing).
    fn spawn_task_common<F, S>(&self, meta: Option<(IdType, usize, usize)>, task: F, spawn_fn: S)
    where
        F: FnOnce() + Send + 'static,
        S: FnOnce(Box<dyn FnOnce() + Send + 'static>),
    {
        let job_id = self.total_spawned.fetch_add(1, Ordering::SeqCst);
        self.pending_jobs.fetch_add(1, Ordering::SeqCst);

        let pending = Arc::clone(&self.pending_jobs);
        let completed = Arc::clone(&self.total_completed);
        let base = Arc::clone(&self.base_instant);
        let recorder_enabled = self.async_recorder.is_some();
        let record_stream = self.record_stream;
        let available_stream_slots = Arc::clone(&self.available_stream_slots);
        let metrics = self.worker_metrics.clone(); // Phase 4

        let (task_id, slot, index) = meta.unwrap_or((IdType::MIN, usize::MIN, usize::MIN));

        let wrapped_task = move || {
            let worker = get_current_worker_id().unwrap_or(usize::MAX);
            let worker_idx = get_current_worker_index().unwrap_or(usize::MAX);

            // Phase 4: Record task start (worker becomes busy)
            if let Some(ref m) = metrics {
                if worker_idx != usize::MAX {
                    m.record_task_start(worker_idx);
                }
            }

            let start = (*base).elapsed().as_nanos();
            task();
            let end = (*base).elapsed().as_nanos();

            // Phase 4: Record task completion (worker becomes idle)
            if let Some(ref m) = metrics {
                if worker_idx != usize::MAX {
                    m.record_task_complete(worker_idx);
                }
            }

            // Lock-free recording via per-worker channel
            // Check if we should record this slot based on stream filter
            let should_record = match record_stream {
                None => true, // Record all streams
                Some(target_stream) => {
                    // Get current stream for this slot
                    let slots_read = available_stream_slots.read();
                    let current_stream = slots_read.get(slot).copied().unwrap_or(usize::MAX);
                    current_stream == target_stream
                }
            };

            if recorder_enabled && should_record {
                submit_record(Record {
                    slot,
                    job_id,
                    start_ns: start,
                    end_ns: end,
                    worker,
                    task_id,
                    index,
                });
            }

            pending.fetch_sub(1, Ordering::SeqCst);
            completed.fetch_add(1, Ordering::SeqCst);
        };

        spawn_fn(Box::new(wrapped_task));
    }

    fn get_async_recorder(&self) -> Option<Arc<AsyncRecorder>> {
        self.async_recorder.clone()
    }

    fn get_main_core(&self) -> Option<core_affinity::CoreId> {
        self.main_core.clone()
    }
}

#[derive(Debug)]
pub struct FifoScheduler {
    base: SchedulerBase,
}

impl FifoScheduler {
    fn new(
        core_offset: usize,
        workers: usize,
        record: bool,
        external_recorder: Option<Arc<AsyncRecorder>>,
        base_instant: Instant,
        system_threads: usize,
        receiver_threads: usize,
        target_batch_size: usize,
        batch_timeout_us: u64,
        record_stream: Option<usize>,
        available_stream_slots: Arc<parking_lot::RwLock<Vec<usize>>>,
    ) -> Self {
        Self {
            base: SchedulerBase::new(
                core_offset,
                workers,
                record,
                external_recorder,
                base_instant,
                system_threads,
                receiver_threads,
                target_batch_size,
                batch_timeout_us,
                record_stream,
                available_stream_slots,
            ),
        }
    }

    fn spawn_task<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.spawn_task_with_meta(None, task)
    }

    fn spawn_task_with_meta<F>(&self, meta: Option<(IdType, usize, usize)>, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.base
            .spawn_task_common(meta, task, |t| self.base.threadpool.spawn_fifo(t));
    }
}

#[derive(Debug)]
pub struct WorkStealScheduler {
    base: SchedulerBase,
}

impl WorkStealScheduler {
    fn new(
        core_offset: usize,
        workers: usize,
        record: bool,
        external_recorder: Option<Arc<AsyncRecorder>>,
        base_instant: Instant,
        system_threads: usize,
        receiver_threads: usize,
        target_batch_size: usize,
        batch_timeout_us: u64,
        record_stream: Option<usize>,
        available_stream_slots: Arc<parking_lot::RwLock<Vec<usize>>>,
    ) -> Self {
        Self {
            base: SchedulerBase::new(
                core_offset,
                workers,
                record,
                external_recorder,
                base_instant,
                system_threads,
                receiver_threads,
                target_batch_size,
                batch_timeout_us,
                record_stream,
                available_stream_slots,
            ),
        }
    }

    fn spawn_task<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.spawn_task_with_meta(None, task)
    }

    fn spawn_task_with_meta<F>(&self, meta: Option<(IdType, usize, usize)>, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.base
            .spawn_task_common(meta, task, |t| self.base.threadpool.spawn(t));
    }
}

pub enum SchedulerImpl {
    Fifo(FifoScheduler),
    WorkStealing(WorkStealScheduler),
}

impl SchedulerImpl {
    pub fn spawn_task<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        match self {
            SchedulerImpl::Fifo(scheduler) => scheduler.spawn_task(task),
            SchedulerImpl::WorkStealing(scheduler) => scheduler.spawn_task(task),
        }
    }

    pub fn spawn_task_with_meta<F>(&self, meta: Option<(IdType, usize, usize)>, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        match self {
            SchedulerImpl::Fifo(scheduler) => scheduler.spawn_task_with_meta(meta, task),
            SchedulerImpl::WorkStealing(scheduler) => scheduler.spawn_task_with_meta(meta, task),
        }
    }

    pub fn workers(&self) -> usize {
        match self {
            SchedulerImpl::Fifo(scheduler) => scheduler.base.workers(),
            SchedulerImpl::WorkStealing(scheduler) => scheduler.base.workers(),
        }
    }

    pub fn pending_jobs(&self) -> usize {
        match self {
            SchedulerImpl::Fifo(scheduler) => scheduler.base.pending_jobs(),
            SchedulerImpl::WorkStealing(scheduler) => scheduler.base.pending_jobs(),
        }
    }

    pub fn total_jobs_spawned(&self) -> usize {
        match self {
            SchedulerImpl::Fifo(scheduler) => scheduler.base.total_jobs_spawned(),
            SchedulerImpl::WorkStealing(scheduler) => scheduler.base.total_jobs_spawned(),
        }
    }

    pub fn total_jobs_completed(&self) -> usize {
        match self {
            SchedulerImpl::Fifo(scheduler) => scheduler.base.total_jobs_completed(),
            SchedulerImpl::WorkStealing(scheduler) => scheduler.base.total_jobs_completed(),
        }
    }

    pub fn core_offset(&self) -> usize {
        match self {
            SchedulerImpl::Fifo(scheduler) => scheduler.base.core_offset(),
            SchedulerImpl::WorkStealing(scheduler) => scheduler.base.core_offset(),
        }
    }

    pub fn system_threads(&self) -> usize {
        match self {
            SchedulerImpl::Fifo(scheduler) => scheduler.base.system_threads(),
            SchedulerImpl::WorkStealing(scheduler) => scheduler.base.system_threads(),
        }
    }

    pub fn receiver_core_offset(&self) -> usize {
        match self {
            SchedulerImpl::Fifo(scheduler) => scheduler.base.receiver_core_offset(),
            SchedulerImpl::WorkStealing(scheduler) => scheduler.base.receiver_core_offset(),
        }
    }

    pub fn receiver_threads(&self) -> usize {
        match self {
            SchedulerImpl::Fifo(scheduler) => scheduler.base.receiver_threads(),
            SchedulerImpl::WorkStealing(scheduler) => scheduler.base.receiver_threads(),
        }
    }

    /// Dump recorded schedule to CSV at `path` (slot,job_id,start_ns,end_ns,worker,task_name)
    pub fn write_record(&self, path: &str) {
        match self {
            SchedulerImpl::Fifo(s) => s.base.write_records_to_csv(path),
            SchedulerImpl::WorkStealing(s) => s.base.write_records_to_csv(path),
        }
    }

    pub fn get_batch_queue_tx(&self) -> BatchSender<(NodeInfo, CmTypes)> {
        match self {
            SchedulerImpl::Fifo(s) => s.base.get_batch_queue_tx(),
            SchedulerImpl::WorkStealing(s) => s.base.get_batch_queue_tx(),
        }
    }

    pub fn get_batch_queue_rx(&self) -> Arc<BatchReceiver<(NodeInfo, CmTypes)>> {
        match self {
            SchedulerImpl::Fifo(s) => s.base.get_batch_queue_rx(),
            SchedulerImpl::WorkStealing(s) => s.base.get_batch_queue_rx(),
        }
    }

    pub fn get_target_batch_size(&self) -> usize {
        match self {
            SchedulerImpl::Fifo(s) => s.base.get_target_batch_size(),
            SchedulerImpl::WorkStealing(s) => s.base.get_target_batch_size(),
        }
    }

    pub fn get_batch_timeout_us(&self) -> u64 {
        match self {
            SchedulerImpl::Fifo(s) => s.base.get_batch_timeout_us(),
            SchedulerImpl::WorkStealing(s) => s.base.get_batch_timeout_us(),
        }
    }

    pub fn get_async_recorder(&self) -> Option<Arc<AsyncRecorder>> {
        match self {
            SchedulerImpl::Fifo(s) => s.base.get_async_recorder(),
            SchedulerImpl::WorkStealing(s) => s.base.get_async_recorder(),
        }
    }

    pub fn main_core(&self) -> Option<core_affinity::CoreId> {
        match self {
            SchedulerImpl::Fifo(s) => s.base.get_main_core(),
            SchedulerImpl::WorkStealing(s) => s.base.get_main_core(),
        }
    }

    /// Phase 4: Print worker utilization statistics
    pub fn print_worker_stats(&self) {
        match self {
            SchedulerImpl::Fifo(s) => {
                if let Some(ref m) = s.base.worker_metrics {
                    m.print_stats();
                }
            }
            SchedulerImpl::WorkStealing(s) => {
                if let Some(ref m) = s.base.worker_metrics {
                    m.print_stats();
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum SchedulerType {
    Fifo,
    WorkStealing,
}

pub fn create_scheduler(
    scheduler_type: SchedulerType,
    core_offset: usize,
    num_workers: usize,
    record: bool,
    external_recorder: Option<Arc<AsyncRecorder>>,
    base_instant: Instant,
    system_threads: usize,
    receiver_threads: usize,
    target_batch_size: usize,
    batch_timeout_us: u64,
    record_stream: Option<usize>,
    available_stream_slots: Arc<parking_lot::RwLock<Vec<usize>>>,
) -> SchedulerImpl {
    match scheduler_type {
        SchedulerType::Fifo => SchedulerImpl::Fifo(FifoScheduler::new(
            core_offset,
            num_workers,
            record,
            external_recorder,
            base_instant,
            system_threads,
            receiver_threads,
            target_batch_size,
            batch_timeout_us,
            record_stream,
            available_stream_slots,
        )),
        SchedulerType::WorkStealing => SchedulerImpl::WorkStealing(WorkStealScheduler::new(
            core_offset,
            num_workers,
            record,
            external_recorder,
            base_instant,
            system_threads,
            receiver_threads,
            target_batch_size,
            batch_timeout_us,
            record_stream,
            available_stream_slots,
        )),
    }
}
