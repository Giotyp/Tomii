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
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::async_recorder::{set_worker_recorder, submit_record, AsyncRecorder};
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

pub struct ThreadPoolResult {
    pub threadpool: ThreadPool,
    pub system_core_offset: usize,
    pub system_threads: usize,
    pub receiver_core_offset: usize,
    pub receiver_threads: usize,
    pub worker_core_offset: usize,
    pub main_core: Option<core_affinity::CoreId>,
}

/// Create Threadpool with Rayon, pinning workers to allocated cores.
pub fn create_threadpool(
    core_offset: usize,
    workers: usize,
    receiver_threads: usize,
    system_threads: usize,
    async_recorder: Option<Arc<AsyncRecorder>>,
) -> ThreadPoolResult {
    // Use core allocation algorithm
    let alloc =
        crate::core_alloc::allocate_cores(core_offset, system_threads, receiver_threads, workers);

    let system_core_offset = alloc.system_core_offset;
    let receiver_offset = alloc.receiver_offset;
    let worker_offset = alloc.worker_offset;
    let actual_workers = alloc.worker_count;
    let actual_receivers = alloc.receiver_threads;
    let actual_system_threads = alloc.system_threads;
    let main_core_opt = alloc.main_core.clone();

    let worker_cores_to_use: Vec<core_affinity::CoreId> =
        alloc.all_core_ids[worker_offset..worker_offset + actual_workers].to_vec();

    // Print core allocation
    println!("========== Core Allocation ==========");
    println!("Available cores: {}", alloc.all_core_ids.len());
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

    ThreadPoolResult {
        threadpool: worker_threadpool,
        system_core_offset,
        system_threads: actual_system_threads,
        receiver_core_offset: receiver_offset,
        receiver_threads: actual_receivers,
        worker_core_offset: worker_offset,
        main_core: main_core_opt,
    }
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
    target_batch_size: usize,
    batch_timeout_us: u64,
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

        let tp = create_threadpool(
            core_offset,
            workers,
            receiver_threads,
            system_threads,
            async_recorder.clone(),
        );

        // Phase 4: Initialize worker metrics (only when recording enabled)
        let worker_metrics = if record {
            Some(Arc::new(WorkerMetrics::new(workers)))
        } else {
            None
        };

        Self {
            threadpool: tp.threadpool,
            system_core_offset: tp.system_core_offset,
            system_threads: tp.system_threads,
            receiver_core_offset: tp.receiver_core_offset,
            receiver_threads: tp.receiver_threads,
            worker_core_offset: tp.worker_core_offset,
            main_core: tp.main_core,
            pending_jobs: Arc::new(AtomicUsize::new(0)),
            total_spawned: Arc::new(AtomicUsize::new(0)),
            total_completed: Arc::new(AtomicUsize::new(0)),
            async_recorder,
            base_instant: Arc::new(base_instant),
            target_batch_size,
            batch_timeout_us,
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

    fn get_target_batch_size(&self) -> usize {
        self.target_batch_size
    }

    fn get_batch_timeout_us(&self) -> u64 {
        self.batch_timeout_us
    }

    /// Common task spawning logic. `spawn_fn` handles the specific spawning (e.g., FIFO or work-stealing).
    fn spawn_task_common<F, S>(&self, meta: Option<crate::TaskMeta>, task: F, spawn_fn: S)
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
        let metrics = self.worker_metrics.clone(); // Phase 4

        let crate::TaskMeta { task_id, slot, index, should_record } =
            meta.unwrap_or(crate::TaskMeta { task_id: IdType::MIN, slot: usize::MIN, index: usize::MIN, should_record: false });

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
            // should_record flag was pre-computed at spawn time based on stream filter
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

#[derive(Debug, Clone, Copy)]
enum SpawnMode {
    Fifo,
    WorkStealing,
}

#[derive(Debug)]
pub struct RayonScheduler {
    base: SchedulerBase,
    mode: SpawnMode,
}

impl RayonScheduler {
    fn new(
        mode: SpawnMode,
        core_offset: usize,
        workers: usize,
        record: bool,
        external_recorder: Option<Arc<AsyncRecorder>>,
        base_instant: Instant,
        system_threads: usize,
        receiver_threads: usize,
        target_batch_size: usize,
        batch_timeout_us: u64,
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
            ),
            mode,
        }
    }

    fn spawn_task<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.spawn_task_with_meta(None, task);
    }

    fn spawn_task_with_meta<F>(&self, meta: Option<crate::TaskMeta>, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        match self.mode {
            SpawnMode::Fifo => self
                .base
                .spawn_task_common(meta, task, |t| self.base.threadpool.spawn_fifo(t)),
            SpawnMode::WorkStealing => self
                .base
                .spawn_task_common(meta, task, |t| self.base.threadpool.spawn(t)),
        }
    }
}

pub enum SchedulerImpl {
    Rayon(RayonScheduler),
    Custom(crate::custom_scheduler::CustomScheduler),
}

impl SchedulerImpl {
    pub fn spawn_task<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        match self {
            SchedulerImpl::Rayon(scheduler) => scheduler.spawn_task(task),
            SchedulerImpl::Custom(scheduler) => scheduler.spawn(task),
        }
    }

    pub fn spawn_task_with_meta<F>(&self, meta: Option<crate::TaskMeta>, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        match self {
            SchedulerImpl::Rayon(scheduler) => scheduler.spawn_task_with_meta(meta, task),
            SchedulerImpl::Custom(scheduler) => scheduler.spawn_with_meta(meta, task),
        }
    }

    /// Spawn task with metadata and priority (Custom scheduler respects priority, others ignore it)
    pub fn spawn_task_with_meta_priority<F>(
        &self,
        priority: crate::custom_scheduler::Priority,
        meta: Option<crate::TaskMeta>,
        task: F,
    ) where
        F: FnOnce() + Send + 'static,
    {
        match self {
            SchedulerImpl::Rayon(scheduler) => scheduler.spawn_task_with_meta(meta, task),
            SchedulerImpl::Custom(scheduler) => {
                scheduler.spawn_with_meta_priority(priority, meta, task)
            }
        }
    }

    /// Spawn task to specific worker group (Custom scheduler only, others fallback to normal spawn)
    pub fn spawn_to_group_with_meta<F>(
        &self,
        group_id: usize,
        priority: crate::custom_scheduler::Priority,
        meta: Option<crate::TaskMeta>,
        task: F,
    ) where
        F: FnOnce() + Send + 'static,
    {
        match self {
            SchedulerImpl::Rayon(scheduler) => scheduler.spawn_task_with_meta(meta, task),
            SchedulerImpl::Custom(scheduler) => {
                scheduler.spawn_to_group_with_meta(group_id, priority, meta, task)
            }
        }
    }

    /// Get the affinity group for a given use_workers spec
    /// Returns:
    /// - 0 for None (no affinity), Count specs, or non-Custom schedulers
    /// - group_id for Range specs in Custom scheduler
    pub fn get_affinity_group(&self, use_workers: Option<&crate::WorkerRangeSpec>) -> usize {
        match self {
            SchedulerImpl::Rayon(_) => {
                // Non-custom schedulers don't support affinity groups
                0
            }
            SchedulerImpl::Custom(scheduler) => {
                // Delegate to CustomScheduler's affinity logic
                scheduler.get_affinity_group(use_workers)
            }
        }
    }

    pub fn workers(&self) -> usize {
        match self {
            SchedulerImpl::Rayon(scheduler) => scheduler.base.workers(),
            SchedulerImpl::Custom(scheduler) => scheduler.workers(),
        }
    }

    pub fn pending_jobs(&self) -> usize {
        match self {
            SchedulerImpl::Rayon(scheduler) => scheduler.base.pending_jobs(),
            SchedulerImpl::Custom(scheduler) => scheduler.pending_tasks(),
        }
    }

    pub fn total_jobs_spawned(&self) -> usize {
        match self {
            SchedulerImpl::Rayon(scheduler) => scheduler.base.total_jobs_spawned(),
            SchedulerImpl::Custom(scheduler) => scheduler.total_spawned(),
        }
    }

    pub fn total_jobs_completed(&self) -> usize {
        match self {
            SchedulerImpl::Rayon(scheduler) => scheduler.base.total_jobs_completed(),
            SchedulerImpl::Custom(scheduler) => scheduler.total_completed(),
        }
    }

    pub fn core_offset(&self) -> usize {
        match self {
            SchedulerImpl::Rayon(scheduler) => scheduler.base.core_offset(),
            SchedulerImpl::Custom(scheduler) => scheduler.core_offset(),
        }
    }

    pub fn system_threads(&self) -> usize {
        match self {
            SchedulerImpl::Rayon(scheduler) => scheduler.base.system_threads(),
            SchedulerImpl::Custom(scheduler) => scheduler.system_threads(),
        }
    }

    pub fn receiver_core_offset(&self) -> usize {
        match self {
            SchedulerImpl::Rayon(scheduler) => scheduler.base.receiver_core_offset(),
            SchedulerImpl::Custom(scheduler) => scheduler.receiver_core_offset(),
        }
    }

    pub fn receiver_threads(&self) -> usize {
        match self {
            SchedulerImpl::Rayon(scheduler) => scheduler.base.receiver_threads(),
            SchedulerImpl::Custom(scheduler) => scheduler.receiver_threads(),
        }
    }

    /// Dump recorded schedule to CSV at `path` (slot,job_id,start_ns,end_ns,worker,task_name)
    pub fn write_record(&self, path: &str) {
        match self {
            SchedulerImpl::Rayon(s) => s.base.write_records_to_csv(path),
            SchedulerImpl::Custom(s) => s.write_record(path),
        }
    }

    pub fn get_async_recorder(&self) -> Option<Arc<AsyncRecorder>> {
        match self {
            SchedulerImpl::Rayon(s) => s.base.get_async_recorder(),
            SchedulerImpl::Custom(s) => s.get_async_recorder(),
        }
    }

    pub fn main_core(&self) -> Option<core_affinity::CoreId> {
        match self {
            SchedulerImpl::Rayon(s) => s.base.get_main_core(),
            SchedulerImpl::Custom(s) => s.main_core(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum SchedulerType {
    Fifo,
    WorkStealing,
    Custom,
}

/// Worker affinity configuration for nodes with use_workers
/// Maps WorkerRange -> group_id for routing tasks to specific worker ranges
#[derive(Debug, Clone, Default)]
pub struct WorkerAffinityConfig {
    /// Maps WorkerRange -> group_id (0 is always global/all workers)
    pub range_to_group: std::collections::HashMap<crate::WorkerRange, usize>,
    /// List of (group_id, WorkerRange) for affinity groups
    pub affinity_groups: Vec<(usize, crate::WorkerRange)>,
    /// Maps worker_index -> Vec<group_id> for multi-group membership
    /// A worker can belong to multiple groups if ranges overlap
    pub worker_to_groups: std::collections::HashMap<usize, Vec<usize>>,
}

impl WorkerAffinityConfig {
    /// Create affinity config from a set of unique WorkerRangeSpec values
    /// Only processes Range specs - Count specs always use the global queue (group 0)
    pub fn from_worker_specs(
        specs: &std::collections::HashSet<crate::WorkerRangeSpec>,
        total_workers: usize,
    ) -> Self {
        // Extract only range-based specs - count-based specs use global queue
        let mut ranges: std::collections::HashSet<crate::WorkerRange> =
            std::collections::HashSet::new();

        // Filter and sort range-based specs for deterministic ordering
        let mut range_specs: Vec<_> = specs
            .iter()
            .filter_map(|s| match s {
                crate::WorkerRangeSpec::Range(r) => Some(r),
                crate::WorkerRangeSpec::Count(_) => None, // Ignore count specs
            })
            .collect();

        range_specs.sort();

        // Add range-based specs, validating bounds
        for range in range_specs {
            if range.end > total_workers {
                panic!(
                    "Worker range {:?} exceeds total workers {}",
                    range, total_workers
                );
            }
            ranges.insert(range.clone());
        }

        // Build affinity config from concrete ranges only
        Self::from_worker_ranges(&ranges, total_workers)
    }

    /// Create affinity config from a set of unique WorkerRange values
    pub fn from_worker_ranges(
        ranges: &std::collections::HashSet<crate::WorkerRange>,
        total_workers: usize,
    ) -> Self {
        let mut range_to_group = std::collections::HashMap::new();
        let mut affinity_groups = Vec::new();
        let mut worker_to_groups: std::collections::HashMap<usize, Vec<usize>> =
            std::collections::HashMap::new();

        // Initialize all workers with empty group lists
        for worker_idx in 0..total_workers {
            worker_to_groups.insert(worker_idx, Vec::new());
        }

        let mut group_id = 1;

        // Sort ranges for deterministic assignment
        let mut sorted_ranges: Vec<&crate::WorkerRange> = ranges.iter().collect();
        sorted_ranges.sort();

        for range in sorted_ranges {
            // Validate range bounds
            if range.end > total_workers {
                panic!(
                    "Worker range {:?} exceeds total workers {}",
                    range, total_workers
                );
            }

            // Map range to group
            range_to_group.insert(range.clone(), group_id);
            affinity_groups.push((group_id, range.clone()));

            // Add this group to all workers in the range
            for worker_idx in range.start..range.end {
                worker_to_groups
                    .get_mut(&worker_idx)
                    .unwrap()
                    .push(group_id);
            }

            group_id += 1;
        }

        Self {
            range_to_group,
            affinity_groups,
            worker_to_groups,
        }
    }

    /// Get group_id for a given WorkerRangeSpec value
    /// Returns:
    /// - 0 for None (no affinity - use global queue)
    /// - 0 for Count specs (use global queue with any workers)
    /// - group_id for Range specs (use dedicated worker group)
    pub fn get_group(&self, use_workers: Option<&crate::WorkerRangeSpec>) -> usize {
        match use_workers {
            None => 0, // No spec → global queue
            Some(spec) => match spec {
                crate::WorkerRangeSpec::Range(r) => {
                    // Range spec → dedicated group (if mapped)
                    *self.range_to_group.get(r).unwrap_or(&0)
                }
                crate::WorkerRangeSpec::Count(_) => {
                    // Count spec → always use global queue
                    0
                }
            },
        }
    }

    /// Get list of group IDs for a specific worker index
    pub fn get_worker_groups(&self, worker_idx: usize) -> &[usize] {
        self.worker_to_groups
            .get(&worker_idx)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }
}

/// Configuration for creating a scheduler instance.
///
/// Used by [`create_scheduler`] to avoid a 11-parameter function signature.
pub struct SchedulerConfig {
    pub scheduler_type: SchedulerType,
    pub core_offset: usize,
    pub num_workers: usize,
    pub record: bool,
    pub external_recorder: Option<Arc<AsyncRecorder>>,
    pub base_instant: Instant,
    pub system_threads: usize,
    pub receiver_threads: usize,
    pub target_batch_size: usize,
    pub batch_timeout_us: u64,
    pub worker_affinity: Option<WorkerAffinityConfig>,
}

pub fn create_scheduler(cfg: SchedulerConfig) -> SchedulerImpl {
    let SchedulerConfig {
        scheduler_type,
        core_offset,
        num_workers,
        record,
        external_recorder,
        base_instant,
        system_threads,
        receiver_threads,
        target_batch_size,
        batch_timeout_us,
        worker_affinity,
    } = cfg;
    match scheduler_type {
        SchedulerType::Fifo => SchedulerImpl::Rayon(RayonScheduler::new(
            SpawnMode::Fifo,
            core_offset,
            num_workers,
            record,
            external_recorder,
            base_instant,
            system_threads,
            receiver_threads,
            target_batch_size,
            batch_timeout_us,
        )),
        SchedulerType::WorkStealing => SchedulerImpl::Rayon(RayonScheduler::new(
            SpawnMode::WorkStealing,
            core_offset,
            num_workers,
            record,
            external_recorder,
            base_instant,
            system_threads,
            receiver_threads,
            target_batch_size,
            batch_timeout_us,
        )),
        SchedulerType::Custom => {
            let mut builder = crate::custom_scheduler::CustomScheduler::builder()
                .core_offset(core_offset)
                .system_threads(system_threads)
                .receiver_threads(receiver_threads)
                .record(record)
                .base_instant(base_instant);

            // Build worker groups based on affinity configuration
            //
            // New Architecture:
            // - Range-based specs (e.g., "0-7") create EXCLUSIVE worker groups
            //   These workers ONLY handle tasks with their specific range spec
            // - Count-based specs (e.g., "3") and unspecified tasks use the GLOBAL pool
            //   Remaining workers (not in any range) handle these tasks
            //
            // Example with 16 workers:
            //   - use_workers "0-7" → Group 1: workers 0-7 (exclusive, no global steal)
            //   - use_workers "8-12" → Group 2: workers 8-12 (exclusive, no global steal)
            //   - use_workers "3" → Global pool (any of remaining 3 workers)
            //   - no use_workers → Global pool (any of remaining 3 workers)
            if let Some(ref affinity) = worker_affinity {
                if !affinity.affinity_groups.is_empty() {
                    // Use new with_affinity_groups to automatically create proper groups
                    // Note: with_affinity_groups also calls worker_affinity() internally
                    builder = builder.with_affinity_groups(affinity.clone(), num_workers);
                } else {
                    // No affinity groups - single global group
                    builder = builder
                        .add_workers(num_workers, 64)
                        .worker_affinity(worker_affinity);
                }
            } else {
                // No affinity config - single global group
                builder = builder.add_workers(num_workers, 64);
            }

            if let Some(rec) = external_recorder {
                builder = builder.external_recorder(rec);
            }
            SchedulerImpl::Custom(builder.build())
        }
    }
}
