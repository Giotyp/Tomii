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
use crate::buffers::NodeInfo;
use crate::{IdType, Record};
use synstream_types::CmTypes;

thread_local! {
    // Physical core ID where this thread is pinned. usize::MAX means unassigned.
    static WORKER_ID: Cell<usize> = Cell::new(usize::MAX);
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

/// Set the current thread's physical core ID
pub fn set_current_worker_id(core_id: usize) {
    WORKER_ID.with(|c| c.set(core_id));
}

/// Create Threadpool with Rayon
/// Returns: (ThreadPool, system_core_offset, worker_core_offset, network_core_offset)
pub fn create_threadpool(
    core_offset: usize,
    workers: usize,
    network_workers: usize,
    system_threads: usize,
    async_recorder: Option<Arc<AsyncRecorder>>,
) -> (ThreadPool, Option<ThreadPool>, usize, usize) {
    // Create threadpool and pin workers to cores
    let mut core_ids = core_affinity::get_core_ids().unwrap();
    core_ids.sort();

    let available_cores = core_ids.len();

    // CRITICAL: Allocate cores in sequential order: [system][network][workers]
    // With core_offset=1, system=1, network=2, workers=10:
    //   System: core 1
    //   Network: cores 2-3
    //   Workers: cores 4-13

    let total_needed = system_threads + network_workers + workers;

    let (
        system_core_offset,
        network_offset,
        worker_offset,
        actual_workers,
        actual_network_workers,
        actual_system_threads,
    ) = if available_cores < 2 {
        panic!(
            "Insufficient cores: need minimum 2 cores (1 system + 1 worker), found {}",
            available_cores
        );
    } else if core_offset + total_needed <= available_cores {
        // Ideal case: can honor offset and allocate all threads
        let sys_start = core_offset;
        let net_start = core_offset + system_threads;
        let worker_start = core_offset + system_threads + network_workers;
        (
            sys_start,
            net_start,
            worker_start,
            workers,
            network_workers,
            system_threads,
        )
    } else if total_needed <= available_cores {
        // Can fit all threads but not with requested offset
        eprintln!(
            "Warning: Cannot honor core_offset {}. Using offset 0 instead.",
            core_offset
        );
        let sys_start = 0;
        let net_start = system_threads;
        let worker_start = system_threads + network_workers;
        (
            sys_start,
            net_start,
            worker_start,
            workers,
            network_workers,
            system_threads,
        )
    } else {
        // Not enough cores for all threads - reduce proportionally
        let max_system = 1; // Always need at least 1 system thread
        let remaining = available_cores.saturating_sub(max_system);
        let max_network = network_workers.min(remaining / 2).max(0);
        let max_workers = remaining.saturating_sub(max_network).max(1);
        eprintln!(
                "Warning: Requested {} system + {} network + {} workers = {} total exceeds {} available cores.\n\
                Using {} system at core 0, {} network starting at core {}, {} workers starting at core {}.",
                system_threads, network_workers, workers, total_needed, available_cores,
                max_system, max_network, max_system, max_workers, max_system + max_network
            );
        (
            0,
            max_system,
            max_system + max_network,
            max_workers,
            max_network,
            max_system,
        )
    };

    // VERIFICATION: Ensure proper sequential allocation with no overlaps
    assert!(
        system_core_offset + actual_system_threads <= network_offset,
        "Core allocation bug: system cores [{}..{}) overlap with network cores [{}..{})",
        system_core_offset,
        system_core_offset + actual_system_threads,
        network_offset,
        network_offset + actual_network_workers
    );
    assert!(
        network_offset + actual_network_workers <= worker_offset,
        "Core allocation bug: network cores [{}..{}) overlap with worker cores [{}..{})",
        network_offset,
        network_offset + actual_network_workers,
        worker_offset,
        worker_offset + actual_workers
    );

    let worker_cores_to_use: Vec<core_affinity::CoreId> =
        core_ids[worker_offset..worker_offset + actual_workers].to_vec();

    let network_cores_to_use: Vec<core_affinity::CoreId> =
        core_ids[network_offset..network_offset + actual_network_workers].to_vec();

    // Print core allocation
    println!("========== Core Allocation ==========");
    println!("Available cores: {}", available_cores);
    println!(
        "System threads: {} at cores {}..{}",
        actual_system_threads,
        system_core_offset,
        system_core_offset + actual_system_threads - 1
    );
    println!(
        "Network threads: {} at cores {}..{}",
        actual_network_workers,
        network_offset,
        network_offset + actual_network_workers - 1
    );
    println!(
        "Worker threads: {} at cores {}..{}",
        actual_workers,
        worker_offset,
        worker_offset + actual_workers - 1
    );
    println!("Main Worker -> Core Mapping:");
    for (idx, core_id) in worker_cores_to_use.iter().enumerate() {
        println!("  Worker {}: Core {:?}", idx, core_id);
    }
    println!("======================================");

    let recorder_clone = async_recorder.clone();
    let worker_threadpool = ThreadPoolBuilder::new()
        .num_threads(actual_workers)
        .start_handler(move |thread_index| {
            // Pin to core and store core ID for CSV recording
            let core_id = worker_cores_to_use[thread_index];
            WORKER_ID.with(|c| c.set(core_id.id));

            core_affinity::set_for_current(core_id);

            // Initialize per-worker recording channel
            if let Some(ref recorder) = recorder_clone {
                if let Some(tx) =
                    recorder.get_worker_sender(thread_index + worker_offset - system_core_offset)
                {
                    set_worker_recorder(tx);
                }
            }
        })
        .build()
        .unwrap();

    let recorder_clone = async_recorder.clone();
    let network_threadpool: Option<ThreadPool> = if network_workers > 0 {
        ThreadPoolBuilder::new()
            .num_threads(actual_network_workers)
            .start_handler(move |thread_index| {
                // Pin to core and store core ID for CSV recording
                let core_id = network_cores_to_use[thread_index];
                WORKER_ID.with(|c| c.set(core_id.id));

                core_affinity::set_for_current(core_id);

                // Initialize per-worker recording channel
                if let Some(ref recorder) = recorder_clone {
                    if let Some(tx) = recorder
                        .get_worker_sender(thread_index + network_offset - system_core_offset)
                    {
                        set_worker_recorder(tx);
                    }
                }
            })
            .build()
            .ok()
    } else {
        None
    };

    (
        worker_threadpool,
        network_threadpool,
        system_core_offset,
        worker_offset,
    )
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

    /// Spawn task on network pool if available, otherwise falls back to main pool
    fn spawn_task_with_meta_network<F>(&self, _meta: Option<(IdType, usize, usize)>, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        // Default implementation: delegate to main pool
        self.spawn_task_with_meta(_meta, task)
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

    fn core_offset(&self) -> Option<usize> {
        None // Default implementation
    }
}

/// Shared base for schedulers with common state and logic.
#[derive(Debug)]
struct SchedulerBase {
    threadpool: ThreadPool,
    system_core_offset: usize,
    worker_core_offset: usize,
    pending_jobs: Arc<AtomicUsize>,
    total_spawned: Arc<AtomicUsize>,
    total_completed: Arc<AtomicUsize>,
    async_recorder: Option<Arc<AsyncRecorder>>,
    base_instant: Arc<Instant>,
    // Batching fields
    batch_buffer: Arc<Mutex<Vec<(NodeInfo, CmTypes)>>>,
    batch_last_sent: Arc<Mutex<Instant>>,
    batching_size: usize,
    batching_limit: u64,
    completed_tx: Arc<Mutex<Option<Sender<Vec<(NodeInfo, CmTypes)>>>>>,
    flusher_handle: Arc<Mutex<Option<std::thread::JoinHandle<()>>>>,
    flusher_shutdown: Arc<AtomicUsize>,
}

impl SchedulerBase {
    fn new(
        core_offset: usize,
        workers: usize,
        network_workers: usize,
        record: bool,
        external_recorder: Option<Arc<AsyncRecorder>>,
        base_instant: Instant,
        system_threads: usize,
        batching_size: usize,
        batching_limit: u64,
    ) -> Self {
        let total_recorders = workers + network_workers + system_threads;
        let async_recorder = if record {
            match external_recorder {
                Some(r) => Some(r),
                None => Some(Arc::new(AsyncRecorder::new(total_recorders, 100))),
            }
        } else {
            None
        };

        let (worker_threadpool, _network_threadpool_opt, system_core_offset, worker_core_offset) =
            create_threadpool(
                core_offset,
                workers,
                network_workers,
                system_threads,
                async_recorder.clone(),
            );

        let batch_buffer = Arc::new(Mutex::new(Vec::with_capacity(batching_size)));
        let batch_last_sent = Arc::new(Mutex::new(Instant::now()));
        let completed_tx = Arc::new(Mutex::new(None));

        Self {
            threadpool: worker_threadpool,
            system_core_offset,
            worker_core_offset,
            pending_jobs: Arc::new(AtomicUsize::new(0)),
            total_spawned: Arc::new(AtomicUsize::new(0)),
            total_completed: Arc::new(AtomicUsize::new(0)),
            async_recorder,
            base_instant: Arc::new(base_instant),
            batch_buffer,
            batch_last_sent,
            batching_size,
            batching_limit,
            completed_tx,
            flusher_handle: Arc::new(Mutex::new(None)),
            flusher_shutdown: Arc::new(AtomicUsize::new(0)),
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

    fn core_offset(&self) -> Option<usize> {
        Some(self.system_core_offset)
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

    fn set_completed_tx(&self, tx: Sender<Vec<(NodeInfo, CmTypes)>>) {
        let mut completed_tx_lock = self.completed_tx.lock();
        *completed_tx_lock = Some(tx);
    }

    fn start_flusher_thread(&self) {
        let batch_buffer = Arc::clone(&self.batch_buffer);
        let batch_last_sent = Arc::clone(&self.batch_last_sent);
        let completed_tx = Arc::clone(&self.completed_tx);
        let shutdown = Arc::clone(&self.flusher_shutdown);
        let batching_size = self.batching_size;
        let batching_limit = self.batching_limit;
        let batch_timeout = Duration::from_micros(batching_limit);

        let handle = std::thread::spawn(move || loop {
            // Check for shutdown signal
            if shutdown.load(Ordering::SeqCst) == 1 {
                // Final flush before exit
                let mut batch = batch_buffer.lock();
                if !batch.is_empty() {
                    let batch_to_send = std::mem::take(&mut *batch);
                    drop(batch);
                    if let Some(tx) = completed_tx.lock().as_ref() {
                        let _ = tx.send(batch_to_send);
                    }
                }
                break;
            }

            std::thread::sleep(Duration::from_micros(batching_limit / 2));

            let should_flush = {
                let last_sent = batch_last_sent.lock();
                last_sent.elapsed() >= batch_timeout
            };

            if should_flush {
                let mut batch = batch_buffer.lock();
                if !batch.is_empty() {
                    let batch_to_send =
                        std::mem::replace(&mut *batch, Vec::with_capacity(batching_size));
                    drop(batch);
                    *batch_last_sent.lock() = Instant::now();

                    if let Some(tx) = completed_tx.lock().as_ref() {
                        let _ = tx.send(batch_to_send);
                    }
                }
            }
        });

        let mut flusher_lock = self.flusher_handle.lock();
        *flusher_lock = Some(handle);
    }

    fn flush_batch(&self) {
        let mut batch = self.batch_buffer.lock();
        if !batch.is_empty() {
            let batch_to_send =
                std::mem::replace(&mut *batch, Vec::with_capacity(self.batching_size));
            drop(batch);
            *self.batch_last_sent.lock() = Instant::now();

            if let Some(tx) = self.completed_tx.lock().as_ref() {
                let _ = tx.send(batch_to_send);
            }
        }
    }

    fn shutdown_flusher(&self) {
        // Signal shutdown
        self.flusher_shutdown.store(1, Ordering::SeqCst);

        // Wait for flusher thread to finish
        let mut handle_lock = self.flusher_handle.lock();
        if let Some(handle) = handle_lock.take() {
            drop(handle_lock); // Release lock before joining
            let _ = handle.join();
        }
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

        let (task_id, slot, index) = meta.unwrap_or((IdType::MIN, usize::MIN, usize::MIN));

        let wrapped_task = move || {
            let worker = get_current_worker_id().unwrap_or(usize::MAX);
            let start = (*base).elapsed().as_nanos();
            task();
            let end = (*base).elapsed().as_nanos();

            // Lock-free recording via per-worker channel
            if recorder_enabled {
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

    fn get_batch_buffer(&self) -> Arc<Mutex<Vec<(NodeInfo, CmTypes)>>> {
        Arc::clone(&self.batch_buffer)
    }

    fn get_batch_last_sent(&self) -> Arc<Mutex<Instant>> {
        Arc::clone(&self.batch_last_sent)
    }

    fn get_batching_size(&self) -> usize {
        self.batching_size
    }

    fn get_completed_tx_ref(&self) -> Arc<Mutex<Option<Sender<Vec<(NodeInfo, CmTypes)>>>>> {
        Arc::clone(&self.completed_tx)
    }

    fn get_async_recorder(&self) -> Option<Arc<AsyncRecorder>> {
        self.async_recorder.clone()
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
        batching_size: usize,
        batching_limit: u64,
    ) -> Self {
        Self {
            base: SchedulerBase::new(
                core_offset,
                workers,
                0,
                record,
                external_recorder,
                base_instant,
                system_threads,
                batching_size,
                batching_limit,
            ),
        }
    }

    fn set_completed_tx(&self, tx: Sender<Vec<(NodeInfo, CmTypes)>>) {
        self.base.set_completed_tx(tx);
        self.base.start_flusher_thread();
    }

    fn flush_batch(&self) {
        self.base.flush_batch();
    }

    fn get_batch_buffer(&self) -> Arc<Mutex<Vec<(NodeInfo, CmTypes)>>> {
        self.base.get_batch_buffer()
    }

    fn get_batch_last_sent(&self) -> Arc<Mutex<Instant>> {
        self.base.get_batch_last_sent()
    }

    fn get_batching_size(&self) -> usize {
        self.base.get_batching_size()
    }

    fn get_completed_tx_ref(&self) -> Arc<Mutex<Option<Sender<Vec<(NodeInfo, CmTypes)>>>>> {
        self.base.get_completed_tx_ref()
    }
    fn shutdown_flusher(&self) {
        self.base.shutdown_flusher();
    }
    fn write_records_to_csv(&self, path: &str) {
        self.base.write_records_to_csv(path);
    }

    fn core_offset(&self) -> Option<usize> {
        self.base.core_offset()
    }
}

impl Scheduler for FifoScheduler {
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

    fn workers(&self) -> usize {
        self.base.workers()
    }

    fn pending_jobs(&self) -> usize {
        self.base.pending_jobs()
    }

    fn total_jobs_spawned(&self) -> usize {
        self.base.total_jobs_spawned()
    }

    fn total_jobs_completed(&self) -> usize {
        self.base.total_jobs_completed()
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
        batching_size: usize,
        batching_limit: u64,
    ) -> Self {
        Self {
            base: SchedulerBase::new(
                core_offset,
                workers,
                0,
                record,
                external_recorder,
                base_instant,
                system_threads,
                batching_size,
                batching_limit,
            ),
        }
    }

    fn set_completed_tx(&self, tx: Sender<Vec<(NodeInfo, CmTypes)>>) {
        self.base.set_completed_tx(tx);
        self.base.start_flusher_thread();
    }

    fn flush_batch(&self) {
        self.base.flush_batch();
    }

    fn get_batch_buffer(&self) -> Arc<Mutex<Vec<(NodeInfo, CmTypes)>>> {
        self.base.get_batch_buffer()
    }

    fn get_batch_last_sent(&self) -> Arc<Mutex<Instant>> {
        self.base.get_batch_last_sent()
    }

    fn get_batching_size(&self) -> usize {
        self.base.get_batching_size()
    }

    fn get_completed_tx_ref(&self) -> Arc<Mutex<Option<Sender<Vec<(NodeInfo, CmTypes)>>>>> {
        self.base.get_completed_tx_ref()
    }

    fn shutdown_flusher(&self) {
        self.base.shutdown_flusher();
    }

    fn write_records_to_csv(&self, path: &str) {
        self.base.write_records_to_csv(path);
    }

    fn core_offset(&self) -> Option<usize> {
        self.base.core_offset()
    }
}

impl Scheduler for WorkStealScheduler {
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

    fn workers(&self) -> usize {
        self.base.workers()
    }

    fn pending_jobs(&self) -> usize {
        self.base.pending_jobs()
    }

    fn total_jobs_spawned(&self) -> usize {
        self.base.total_jobs_spawned()
    }

    fn total_jobs_completed(&self) -> usize {
        self.base.total_jobs_completed()
    }
}

pub enum SchedulerImpl {
    Fifo(FifoScheduler),
    WorkStealing(WorkStealScheduler),
    Unified(UnifiedScheduler),
}

impl Scheduler for SchedulerImpl {
    fn spawn_task<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        match self {
            SchedulerImpl::Fifo(scheduler) => scheduler.spawn_task(task),
            SchedulerImpl::WorkStealing(scheduler) => scheduler.spawn_task(task),
            SchedulerImpl::Unified(scheduler) => scheduler.spawn_task(task),
        }
    }

    fn spawn_task_with_meta<F>(&self, meta: Option<(IdType, usize, usize)>, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        match self {
            SchedulerImpl::Fifo(scheduler) => scheduler.spawn_task_with_meta(meta, task),
            SchedulerImpl::WorkStealing(scheduler) => scheduler.spawn_task_with_meta(meta, task),
            SchedulerImpl::Unified(scheduler) => scheduler.spawn_task_with_meta(meta, task),
        }
    }

    fn workers(&self) -> usize {
        match self {
            SchedulerImpl::Fifo(scheduler) => scheduler.workers(),
            SchedulerImpl::WorkStealing(scheduler) => scheduler.workers(),
            SchedulerImpl::Unified(scheduler) => scheduler.workers(),
        }
    }

    fn pending_jobs(&self) -> usize {
        match self {
            SchedulerImpl::Fifo(scheduler) => scheduler.pending_jobs(),
            SchedulerImpl::WorkStealing(scheduler) => scheduler.pending_jobs(),
            SchedulerImpl::Unified(scheduler) => scheduler.pending_jobs(),
        }
    }

    fn total_jobs_spawned(&self) -> usize {
        match self {
            SchedulerImpl::Fifo(scheduler) => scheduler.total_jobs_spawned(),
            SchedulerImpl::WorkStealing(scheduler) => scheduler.total_jobs_spawned(),
            SchedulerImpl::Unified(scheduler) => scheduler.total_jobs_spawned(),
        }
    }

    fn total_jobs_completed(&self) -> usize {
        match self {
            SchedulerImpl::Fifo(scheduler) => scheduler.total_jobs_completed(),
            SchedulerImpl::WorkStealing(scheduler) => scheduler.total_jobs_completed(),
            SchedulerImpl::Unified(scheduler) => scheduler.total_jobs_completed(),
        }
    }

    fn core_offset(&self) -> Option<usize> {
        match self {
            SchedulerImpl::Fifo(scheduler) => scheduler.core_offset(),
            SchedulerImpl::WorkStealing(scheduler) => scheduler.core_offset(),
            SchedulerImpl::Unified(scheduler) => scheduler.core_offset(),
        }
    }
}

impl SchedulerImpl {
    /// Dump recorded schedule to CSV at `path` (slot,job_id,start_ns,end_ns,worker,task_name)
    pub fn write_record(&self, path: &str) {
        match self {
            SchedulerImpl::Fifo(s) => s.write_records_to_csv(path),
            SchedulerImpl::WorkStealing(s) => s.write_records_to_csv(path),
            SchedulerImpl::Unified(s) => s.write_record(path),
        }
    }

    pub fn set_completed_tx(&self, tx: Sender<Vec<(NodeInfo, CmTypes)>>) {
        match self {
            SchedulerImpl::Fifo(s) => s.set_completed_tx(tx),
            SchedulerImpl::WorkStealing(s) => s.set_completed_tx(tx),
            SchedulerImpl::Unified(s) => s.set_completed_tx(tx),
        }
    }

    pub fn flush_batch(&self) {
        match self {
            SchedulerImpl::Fifo(s) => s.flush_batch(),
            SchedulerImpl::WorkStealing(s) => s.flush_batch(),
            SchedulerImpl::Unified(s) => s.flush_batch(),
        }
    }

    pub fn get_batch_buffer(&self) -> Arc<Mutex<Vec<(NodeInfo, CmTypes)>>> {
        match self {
            SchedulerImpl::Fifo(s) => s.get_batch_buffer(),
            SchedulerImpl::WorkStealing(s) => s.get_batch_buffer(),
            SchedulerImpl::Unified(s) => s.get_batch_buffer(),
        }
    }

    pub fn get_batch_last_sent(&self) -> Arc<Mutex<Instant>> {
        match self {
            SchedulerImpl::Fifo(s) => s.get_batch_last_sent(),
            SchedulerImpl::WorkStealing(s) => s.get_batch_last_sent(),
            SchedulerImpl::Unified(s) => s.get_batch_last_sent(),
        }
    }

    pub fn get_batching_size(&self) -> usize {
        match self {
            SchedulerImpl::Fifo(s) => s.get_batching_size(),
            SchedulerImpl::WorkStealing(s) => s.get_batching_size(),
            SchedulerImpl::Unified(s) => s.get_batching_size(),
        }
    }

    pub fn get_completed_tx_ref(&self) -> Arc<Mutex<Option<Sender<Vec<(NodeInfo, CmTypes)>>>>> {
        match self {
            SchedulerImpl::Fifo(s) => s.get_completed_tx_ref(),
            SchedulerImpl::WorkStealing(s) => s.get_completed_tx_ref(),
            SchedulerImpl::Unified(s) => s.get_completed_tx_ref(),
        }
    }

    pub fn shutdown_flusher(&self) {
        match self {
            SchedulerImpl::Fifo(s) => s.shutdown_flusher(),
            SchedulerImpl::WorkStealing(s) => s.shutdown_flusher(),
            SchedulerImpl::Unified(s) => s.shutdown_flusher(),
        }
    }

    pub fn get_async_recorder(&self) -> Option<Arc<AsyncRecorder>> {
        match self {
            SchedulerImpl::Fifo(s) => s.base.get_async_recorder(),
            SchedulerImpl::WorkStealing(s) => s.base.get_async_recorder(),
            SchedulerImpl::Unified(s) => s.get_async_recorder(),
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
    batching_size: usize,
    batching_limit: u64,
) -> SchedulerImpl {
    match scheduler_type {
        SchedulerType::Fifo => SchedulerImpl::Fifo(FifoScheduler::new(
            core_offset,
            num_workers,
            record,
            external_recorder,
            base_instant,
            system_threads,
            batching_size,
            batching_limit,
        )),
        SchedulerType::WorkStealing => SchedulerImpl::WorkStealing(WorkStealScheduler::new(
            core_offset,
            num_workers,
            record,
            external_recorder,
            base_instant,
            system_threads,
            batching_size,
            batching_limit,
        )),
    }
}

/// Unified Scheduler with separate main and network threadpools
/// Maintains sequential worker IDs: main workers 0..N, network workers N..(N+M)
#[derive(Debug)]
pub struct UnifiedScheduler {
    worker_pool: ThreadPool,
    network_pool: Option<ThreadPool>,
    main_workers: usize,
    network_workers: usize,
    system_core_offset: usize,
    worker_core_offset: usize,
    pending_jobs: Arc<AtomicUsize>,
    total_spawned: Arc<AtomicUsize>,
    total_completed: Arc<AtomicUsize>,
    async_recorder: Option<Arc<AsyncRecorder>>,
    base_instant: Arc<Instant>,
    batch_buffer: Arc<Mutex<Vec<(NodeInfo, CmTypes)>>>,
    batch_last_sent: Arc<Mutex<Instant>>,
    batching_size: usize,
    batching_limit: u64,
    completed_tx: Arc<Mutex<Option<Sender<Vec<(NodeInfo, CmTypes)>>>>>,
    flusher_handle: Arc<Mutex<Option<std::thread::JoinHandle<()>>>>,
    flusher_shutdown: Arc<AtomicUsize>,
}

impl UnifiedScheduler {
    pub fn new(
        core_offset: usize,
        main_workers: usize,
        network_workers: usize,
        record: bool,
        external_recorder: Option<Arc<AsyncRecorder>>,
        base_instant: Instant,
        system_threads: usize,
        batching_size: usize,
        batching_limit: u64,
    ) -> Self {
        let total_workers = main_workers + network_workers + system_threads;
        let async_recorder = if record {
            match external_recorder {
                Some(r) => Some(r),
                None => Some(Arc::new(AsyncRecorder::new(total_workers, 100))),
            }
        } else {
            None
        };

        // Create main threadpool - returns offsets for system, workers, and network
        let (worker_pool, network_pool, system_core_offset, worker_core_offset) = create_threadpool(
            core_offset,
            main_workers,
            network_workers,
            system_threads,
            async_recorder.clone(),
        );

        let batch_buffer = Arc::new(Mutex::new(Vec::with_capacity(batching_size)));
        let batch_last_sent = Arc::new(Mutex::new(Instant::now()));
        let completed_tx = Arc::new(Mutex::new(None));

        Self {
            worker_pool,
            network_pool,
            main_workers,
            network_workers,
            system_core_offset,
            worker_core_offset,
            pending_jobs: Arc::new(AtomicUsize::new(0)),
            total_spawned: Arc::new(AtomicUsize::new(0)),
            total_completed: Arc::new(AtomicUsize::new(0)),
            async_recorder,
            base_instant: Arc::new(base_instant),
            batch_buffer,
            batch_last_sent,
            batching_size,
            batching_limit,
            completed_tx,
            flusher_handle: Arc::new(Mutex::new(None)),
            flusher_shutdown: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Common task spawning logic for unified scheduler
    fn spawn_task_common<F, S>(
        &self,
        meta: Option<(IdType, usize, usize)>,
        task: F,
        spawn_fn: S,
        _is_network: bool,
    ) where
        F: FnOnce() + Send + 'static,
        S: FnOnce(Box<dyn FnOnce() + Send + 'static>),
    {
        let job_id = self.total_spawned.fetch_add(1, Ordering::SeqCst);
        self.pending_jobs.fetch_add(1, Ordering::SeqCst);

        let pending = Arc::clone(&self.pending_jobs);
        let completed = Arc::clone(&self.total_completed);
        let base = Arc::clone(&self.base_instant);
        let recorder_enabled = self.async_recorder.is_some();

        let (task_id, slot, index) = meta.unwrap_or((IdType::MIN, usize::MIN, usize::MIN));

        let wrapped_task = move || {
            let worker = get_current_worker_id().unwrap_or(usize::MAX);
            let start = (*base).elapsed().as_nanos();
            task();
            let end = (*base).elapsed().as_nanos();

            // Lock-free recording via per-worker channel
            if recorder_enabled {
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

    fn write_records_to_csv(&self, path: &str) {
        if let Some(recorder) = &self.async_recorder {
            match recorder.write_to_csv(path) {
                Ok(()) => {}
                Err(e) => eprintln!("Failed to write records: {}", e),
            }
        } else {
            println!("UnifiedScheduler: recorder not enabled");
        }
    }

    pub fn set_completed_tx(&self, tx: Sender<Vec<(NodeInfo, CmTypes)>>) {
        let mut completed_tx_lock = self.completed_tx.lock();
        *completed_tx_lock = Some(tx);
        drop(completed_tx_lock);
        self.start_flusher_thread();
    }

    pub fn start_flusher_thread(&self) {
        let batch_buffer = Arc::clone(&self.batch_buffer);
        let batch_last_sent = Arc::clone(&self.batch_last_sent);
        let completed_tx = Arc::clone(&self.completed_tx);
        let shutdown = Arc::clone(&self.flusher_shutdown);
        let batching_size = self.batching_size;
        let batching_limit = self.batching_limit;
        let batch_timeout = Duration::from_micros(batching_limit);

        let handle = std::thread::spawn(move || loop {
            if shutdown.load(Ordering::SeqCst) == 1 {
                let mut batch = batch_buffer.lock();
                if !batch.is_empty() {
                    let batch_to_send = std::mem::take(&mut *batch);
                    drop(batch);
                    if let Some(tx) = completed_tx.lock().as_ref() {
                        let _ = tx.send(batch_to_send);
                    }
                }
                break;
            }

            std::thread::sleep(Duration::from_micros(batching_limit / 2));

            let should_flush = {
                let last_sent = batch_last_sent.lock();
                last_sent.elapsed() >= batch_timeout
            };

            if should_flush {
                let mut batch = batch_buffer.lock();
                if !batch.is_empty() {
                    let batch_to_send =
                        std::mem::replace(&mut *batch, Vec::with_capacity(batching_size));
                    drop(batch);
                    *batch_last_sent.lock() = Instant::now();

                    if let Some(tx) = completed_tx.lock().as_ref() {
                        let _ = tx.send(batch_to_send);
                    }
                }
            }
        });

        let mut flusher_lock = self.flusher_handle.lock();
        *flusher_lock = Some(handle);
    }

    pub fn flush_batch(&self) {
        let mut batch = self.batch_buffer.lock();
        if !batch.is_empty() {
            let batch_to_send =
                std::mem::replace(&mut *batch, Vec::with_capacity(self.batching_size));
            drop(batch);
            *self.batch_last_sent.lock() = Instant::now();

            if let Some(tx) = self.completed_tx.lock().as_ref() {
                let _ = tx.send(batch_to_send);
            }
        }
    }

    pub fn shutdown_flusher(&self) {
        self.flusher_shutdown.store(1, Ordering::SeqCst);
        let mut handle_lock = self.flusher_handle.lock();
        if let Some(handle) = handle_lock.take() {
            drop(handle_lock);
            let _ = handle.join();
        }
    }

    pub fn get_batch_buffer(&self) -> Arc<Mutex<Vec<(NodeInfo, CmTypes)>>> {
        Arc::clone(&self.batch_buffer)
    }

    pub fn get_batch_last_sent(&self) -> Arc<Mutex<Instant>> {
        Arc::clone(&self.batch_last_sent)
    }

    pub fn get_batching_size(&self) -> usize {
        self.batching_size
    }

    pub fn get_completed_tx_ref(&self) -> Arc<Mutex<Option<Sender<Vec<(NodeInfo, CmTypes)>>>>> {
        Arc::clone(&self.completed_tx)
    }

    pub fn get_async_recorder(&self) -> Option<Arc<AsyncRecorder>> {
        self.async_recorder.clone()
    }

    pub fn write_record(&self, path: &str) {
        self.write_records_to_csv(path);
    }
}

impl Scheduler for UnifiedScheduler {
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
        self.spawn_task_common(meta, task, |t| self.worker_pool.spawn(t), false);
    }

    fn spawn_task_with_meta_network<F>(&self, meta: Option<(IdType, usize, usize)>, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if let Some(ref net_pool) = self.network_pool {
            self.spawn_task_common(meta, task, |t| net_pool.spawn(t), true);
        } else {
            // Fallback to main pool if network pool not available
            self.spawn_task_with_meta(meta, task);
        }
    }

    fn workers(&self) -> usize {
        self.main_workers + self.network_workers
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

    fn core_offset(&self) -> Option<usize> {
        Some(self.system_core_offset)
    }
}

/// Factory function to create UnifiedScheduler wrapped in SchedulerImpl
pub fn create_unified_scheduler(
    _scheduler_type: SchedulerType,
    core_offset: usize,
    main_workers: usize,
    network_workers: usize,
    record: bool,
    external_recorder: Option<Arc<AsyncRecorder>>,
    base_instant: Instant,
    system_threads: usize,
    batching_size: usize,
    batching_limit: u64,
) -> SchedulerImpl {
    // Note: scheduler_type is currently ignored for UnifiedScheduler
    // Could be extended to use different strategies for main/network pools
    SchedulerImpl::Unified(UnifiedScheduler::new(
        core_offset,
        main_workers,
        network_workers,
        record,
        external_recorder,
        base_instant,
        system_threads,
        batching_size,
        batching_limit,
    ))
}
