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
use crate::debug::print_debug;
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
/// Returns: (ThreadPool, system_core_offset, worker_core_offset)
pub fn create_threadpool(
    core_offset: usize,
    workers: usize,
    receiver_threads: usize,
    system_threads: usize,
    async_recorder: Option<Arc<AsyncRecorder>>,
) -> (ThreadPool, usize, usize, usize, usize, usize) {
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
    ) = if available_cores < 2 {
        panic!(
            "Insufficient cores: need minimum 2 cores (1 system + 1 worker), found {}",
            available_cores
        );
    } else if core_offset + total_needed <= available_cores {
        // Ideal case: can honor offset and allocate all threads
        let sys_start = core_offset;
        let recv_start = core_offset + system_threads;
        let worker_start = core_offset + system_threads + receiver_threads;
        (
            sys_start,
            recv_start,
            worker_start,
            workers,
            receiver_threads,
            system_threads,
        )
    } else if total_needed <= available_cores {
        // Can fit all threads but not with requested offset
        eprintln!(
            "Warning: Cannot honor core_offset {}. Using offset 0 instead.",
            core_offset
        );
        let sys_start = 0;
        let recv_start = system_threads;
        let worker_start = system_threads + receiver_threads;
        (
            sys_start,
            recv_start,
            worker_start,
            workers,
            receiver_threads,
            system_threads,
        )
    } else {
        // Not enough cores for all threads - reduce proportionally
        let max_system = 1; // Always need at least 1 system thread
        let remaining = available_cores.saturating_sub(max_system);
        let max_receivers = receiver_threads.min(remaining / 2).max(0);
        let max_workers = remaining.saturating_sub(max_receivers).max(1);
        eprintln!(
                "Warning: Requested {} system + {} receivers + {} workers = {} total exceeds {} available cores.\n\
                Using {} system at core 0, {} receivers starting at core {}, {} workers starting at core {}.",
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
    )
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
    // Flush notification channel to eliminate polling sleep
    flush_notify_tx: crossbeam_channel::Sender<()>,
    flush_notify_rx: Arc<Mutex<crossbeam_channel::Receiver<()>>>,
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
        batching_size: usize,
        batching_limit: u64,
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
        ) = create_threadpool(
            core_offset,
            workers,
            receiver_threads,
            system_threads,
            async_recorder.clone(),
        );

        let batch_buffer = Arc::new(Mutex::new(Vec::with_capacity(batching_size)));
        let batch_last_sent = Arc::new(Mutex::new(Instant::now()));
        let completed_tx = Arc::new(Mutex::new(None));
        let (flush_notify_tx, flush_notify_rx) = crossbeam_channel::unbounded();

        Self {
            threadpool: worker_threadpool,
            system_core_offset,
            system_threads,
            receiver_core_offset,
            receiver_threads,
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
            flush_notify_tx,
            flush_notify_rx: Arc::new(Mutex::new(flush_notify_rx)),
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
        let flush_notify_rx = Arc::clone(&self.flush_notify_rx);

        let handle = std::thread::spawn(move || {
            let rx = flush_notify_rx.lock();
            loop {
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

                // Wait for notification or timeout - NO POLLING SLEEP
                let _ = rx.recv_timeout(batch_timeout);

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

    fn get_flush_notify(&self) -> crossbeam_channel::Sender<()> {
        self.flush_notify_tx.clone()
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
        receiver_threads: usize,
        batching_size: usize,
        batching_limit: u64,
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
                batching_size,
                batching_limit,
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
        batching_size: usize,
        batching_limit: u64,
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
                batching_size,
                batching_limit,
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

    pub fn set_completed_tx(&self, tx: Sender<Vec<(NodeInfo, CmTypes)>>) {
        match self {
            SchedulerImpl::Fifo(s) => s.base.set_completed_tx(tx),
            SchedulerImpl::WorkStealing(s) => s.base.set_completed_tx(tx),
        }
    }

    pub fn flush_batch(&self) {
        match self {
            SchedulerImpl::Fifo(s) => s.base.flush_batch(),
            SchedulerImpl::WorkStealing(s) => s.base.flush_batch(),
        }
    }

    pub fn get_batch_buffer(&self) -> Arc<Mutex<Vec<(NodeInfo, CmTypes)>>> {
        match self {
            SchedulerImpl::Fifo(s) => s.base.get_batch_buffer(),
            SchedulerImpl::WorkStealing(s) => s.base.get_batch_buffer(),
        }
    }

    pub fn get_batch_last_sent(&self) -> Arc<Mutex<Instant>> {
        match self {
            SchedulerImpl::Fifo(s) => s.base.get_batch_last_sent(),
            SchedulerImpl::WorkStealing(s) => s.base.get_batch_last_sent(),
        }
    }

    pub fn get_batching_size(&self) -> usize {
        match self {
            SchedulerImpl::Fifo(s) => s.base.get_batching_size(),
            SchedulerImpl::WorkStealing(s) => s.base.get_batching_size(),
        }
    }

    pub fn get_flush_notify(&self) -> crossbeam_channel::Sender<()> {
        match self {
            SchedulerImpl::Fifo(s) => s.base.get_flush_notify(),
            SchedulerImpl::WorkStealing(s) => s.base.get_flush_notify(),
        }
    }

    pub fn get_completed_tx_ref(&self) -> Arc<Mutex<Option<Sender<Vec<(NodeInfo, CmTypes)>>>>> {
        match self {
            SchedulerImpl::Fifo(s) => s.base.get_completed_tx_ref(),
            SchedulerImpl::WorkStealing(s) => s.base.get_completed_tx_ref(),
        }
    }

    pub fn shutdown_flusher(&self) {
        match self {
            SchedulerImpl::Fifo(s) => s.base.shutdown_flusher(),
            SchedulerImpl::WorkStealing(s) => s.base.shutdown_flusher(),
        }
    }

    pub fn get_async_recorder(&self) -> Option<Arc<AsyncRecorder>> {
        match self {
            SchedulerImpl::Fifo(s) => s.base.get_async_recorder(),
            SchedulerImpl::WorkStealing(s) => s.base.get_async_recorder(),
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
            receiver_threads,
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
            receiver_threads,
            batching_size,
            batching_limit,
        )),
    }
}
