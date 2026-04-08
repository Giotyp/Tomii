//! # High-Performance Custom Scheduler (Channel-Based)
//!
//! A custom thread pool designed for low-latency task execution with:
//! 1. **MPMC channels** - Even 1-task-per-recv distribution, no batch imbalance
//! 2. **Priority queues** - Urgent tasks are processed first (High > Normal > Low)
//! 3. **Worker scoping** - Workers can be bound to specific queue groups
//! 4. **Efficient blocking** - crossbeam select! with built-in park/wake
//!
//! ## Architecture
//!
//! ```text
//! +---------------------------------------------------------------+
//! |                         Scheduler                             |
//! +---------------------------------------------------------------+
//! |  WorkerGroup 0 (cores 4-8)         WorkerGroup 1 (cores 9-13) |
//! |  +-------------------------+       +-------------------------+ |
//! |  | Worker 0                |       | Worker 5                | |
//! |  | Worker 1                |       | Worker 6                | |
//! |  | Worker 2                |       | Worker 7                | |
//! |  | Worker 3                |       | Worker 8                | |
//! |  | Worker 4                |       | Worker 9                | |
//! |  |    recv from group chans|       |    recv from group chans| |
//! |  |  Group Channels [H/N/L] |       |  Group Channels [H/N/L] | |
//! |  +-------------------------+       +-------------------------+ |
//! |                                                               |
//! |  Global Channels (fallback when group channels empty):        |
//! |  +---------+ +---------+ +---------+                         |
//! |  |  High   | | Normal  | |   Low   |                         |
//! |  +---------+ +---------+ +---------+                         |
//! +---------------------------------------------------------------+
//! ```

use core_affinity::{self, CoreId};
use crossbeam_channel::{Receiver, Sender};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::async_recorder::{set_worker_recorder, submit_record, AsyncRecorder};
use crate::{IdType, Record};

// ============================================================================
// SECTION 1: Priority Types and Task Definition
// ============================================================================

/// A boxed task that can be sent across threads
pub type BoxedTask = Box<dyn FnOnce() + Send + 'static>;

/// Priority levels for task scheduling
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Priority {
    High = 0,   // Checked first
    Normal = 1, // Default priority
    Low = 2,    // Background tasks
}

impl Default for Priority {
    fn default() -> Self {
        Priority::Normal
    }
}

// ============================================================================
// SECTION 2: Channel-Based Queue Types
// ============================================================================

/// Recording metadata carried alongside task.
/// Eliminates Arc::clone per spawn - worker loop handles metrics directly.
struct RecordMeta {
    job_id: usize,
    task_id: IdType,
    slot: usize,
    index: usize,
}

/// Task + optional recording metadata. The item type for all channels.
struct ScheduledTask {
    task: BoxedTask,
    meta: Option<RecordMeta>,
}

/// 3 priority-level MPMC channels (High/Normal/Low).
/// Used for both global and per-group task distribution.
/// crossbeam_channel provides efficient MPMC with built-in park/wake.
struct ChannelSet {
    high_tx: Sender<ScheduledTask>,
    high_rx: Receiver<ScheduledTask>,
    normal_tx: Sender<ScheduledTask>,
    normal_rx: Receiver<ScheduledTask>,
    low_tx: Sender<ScheduledTask>,
    low_rx: Receiver<ScheduledTask>,
}

impl ChannelSet {
    fn new() -> Self {
        let (high_tx, high_rx) = crossbeam_channel::unbounded();
        let (normal_tx, normal_rx) = crossbeam_channel::unbounded();
        let (low_tx, low_rx) = crossbeam_channel::unbounded();
        Self {
            high_tx,
            high_rx,
            normal_tx,
            normal_rx,
            low_tx,
            low_rx,
        }
    }

    #[inline]
    fn send(&self, priority: Priority, task: ScheduledTask) {
        let _ = match priority {
            Priority::High => self.high_tx.send(task),
            Priority::Normal => self.normal_tx.send(task),
            Priority::Low => self.low_tx.send(task),
        };
    }

    /// Non-blocking priority-ordered receive.
    /// Checks High first, then Normal, then Low.
    #[allow(dead_code)] // used by future work-stealing / load-balancing path
    #[inline]
    fn try_recv_prioritized(&self) -> Option<ScheduledTask> {
        self.high_rx
            .try_recv()
            .ok()
            .or_else(|| self.normal_rx.try_recv().ok())
            .or_else(|| self.low_rx.try_recv().ok())
    }

    #[allow(dead_code)] // used by future load-balancing / backpressure path
    #[inline]
    fn is_empty(&self) -> bool {
        self.high_rx.is_empty() && self.normal_rx.is_empty() && self.low_rx.is_empty()
    }
}

/// Non-blocking priority-ordered receive across group and global channels.
/// Order: group.high -> group.normal -> global.high -> global.normal -> group.low -> global.low
#[inline]
fn try_recv_all(
    group: &ChannelSet,
    global: &ChannelSet,
    allow_global: bool,
) -> Option<ScheduledTask> {
    // Group high priority
    if let Ok(t) = group.high_rx.try_recv() {
        return Some(t);
    }
    // Group normal priority
    if let Ok(t) = group.normal_rx.try_recv() {
        return Some(t);
    }
    // Global high/normal (if allowed)
    if allow_global {
        if let Ok(t) = global.high_rx.try_recv() {
            return Some(t);
        }
        if let Ok(t) = global.normal_rx.try_recv() {
            return Some(t);
        }
    }
    // Group low priority
    if let Ok(t) = group.low_rx.try_recv() {
        return Some(t);
    }
    // Global low (if allowed)
    if allow_global {
        if let Ok(t) = global.low_rx.try_recv() {
            return Some(t);
        }
    }
    None
}

// ============================================================================
// SECTION 3: Worker Group Configuration
// ============================================================================

/// Configuration for a group of workers
#[derive(Debug, Clone)]
pub struct WorkerGroupConfig {
    /// Number of workers in this group
    pub num_workers: usize,
    /// Core IDs to pin workers to (if provided)
    pub core_ids: Option<Vec<CoreId>>,
    /// Group identifier
    pub group_id: usize,
    /// Whether this group can steal from global queues
    pub allow_global_steal: bool,
    /// Spin iterations before parking (0 = always park immediately)
    pub spin_iterations: usize,
}

impl Default for WorkerGroupConfig {
    fn default() -> Self {
        Self {
            num_workers: 1,
            core_ids: None,
            group_id: 0,
            allow_global_steal: true,
            spin_iterations: 64,
        }
    }
}

/// Internal state for a worker group
struct WorkerGroup {
    #[allow(dead_code)] // retained for future per-group config queries
    config: WorkerGroupConfig,
    /// Worker thread handles
    handles: Vec<JoinHandle<()>>,
}

// ============================================================================
// SECTION 4: Worker Thread Implementation
// ============================================================================

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

/// Shared state for all workers
struct SharedWorkerState {
    /// Global channels (fallback when group channels empty)
    global_channels: ChannelSet,
    /// Per-group channels
    group_channels: Vec<Arc<ChannelSet>>,
    /// Shutdown signal
    shutdown: AtomicBool,
    /// Total tasks spawned (for metrics)
    total_spawned: AtomicUsize,
    /// Total tasks completed (for metrics)
    total_completed: AtomicUsize,
    /// Pending tasks (spawned - completed)
    pending_tasks: AtomicUsize,
    /// Optional async recorder
    async_recorder: Option<Arc<AsyncRecorder>>,
    /// Base instant for timing
    base_instant: Arc<Instant>,
    /// System core offset for recorder channel indexing
    system_core_offset: usize,
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
fn worker_loop(
    worker_id: usize,
    group_id: usize,
    core_id: CoreId,
    shared: Arc<SharedWorkerState>,
    group_channels: Arc<ChannelSet>,
    allow_global_steal: bool,
    spin_iterations: usize,
) {
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
fn execute_task(shared: &SharedWorkerState, st: ScheduledTask, has_recorder: bool) {
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

// ============================================================================
// SECTION 5: Main Scheduler Implementation
// ============================================================================

/// High-performance custom scheduler with channel-based task distribution
pub struct CustomScheduler {
    shared: Arc<SharedWorkerState>,
    groups: Vec<WorkerGroup>,
    /// System core offset for recording
    system_core_offset: usize,
    /// Number of system threads
    system_threads: usize,
    /// Receiver core offset
    receiver_core_offset: usize,
    /// Number of receiver threads
    receiver_threads: usize,
    /// Total workers across all groups
    total_workers: usize,
    /// Optional reserved core for main/orchestrator thread
    main_core: Option<CoreId>,
    /// Worker affinity configuration for use_workers routing
    worker_affinity: Option<crate::scheduler::WorkerAffinityConfig>,
}

/// Builder for CustomScheduler
pub struct CustomSchedulerBuilder {
    groups: Vec<WorkerGroupConfig>,
    core_offset: usize,
    system_threads: usize,
    receiver_threads: usize,
    record: bool,
    external_recorder: Option<Arc<AsyncRecorder>>,
    base_instant: Instant,
    worker_affinity: Option<crate::scheduler::WorkerAffinityConfig>,
}

impl CustomSchedulerBuilder {
    pub fn new() -> Self {
        Self {
            groups: Vec::new(),
            core_offset: 0,
            system_threads: 1,
            receiver_threads: 0,
            record: false,
            external_recorder: None,
            base_instant: Instant::now(),
            worker_affinity: None,
        }
    }

    /// Add a worker group with configuration
    pub fn add_group(mut self, config: WorkerGroupConfig) -> Self {
        self.groups.push(config);
        self
    }

    /// Add a simple worker group with N workers
    pub fn add_workers(mut self, num_workers: usize, spin_iterations: usize) -> Self {
        let group_id = self.groups.len();
        self.groups.push(WorkerGroupConfig {
            num_workers,
            core_ids: None,
            group_id,
            allow_global_steal: true,
            spin_iterations,
        });
        self
    }

    /// Set core offset for thread pinning
    pub fn core_offset(mut self, offset: usize) -> Self {
        self.core_offset = offset;
        self
    }

    /// Set system threads count (for core allocation)
    pub fn system_threads(mut self, count: usize) -> Self {
        self.system_threads = count;
        self
    }

    /// Set receiver threads count (for core allocation)
    pub fn receiver_threads(mut self, count: usize) -> Self {
        self.receiver_threads = count;
        self
    }

    /// Enable recording
    pub fn record(mut self, enable: bool) -> Self {
        self.record = enable;
        self
    }

    /// Set external async recorder
    pub fn external_recorder(mut self, recorder: Arc<AsyncRecorder>) -> Self {
        self.external_recorder = Some(recorder);
        self
    }

    /// Set base instant for timing
    pub fn base_instant(mut self, instant: Instant) -> Self {
        self.base_instant = instant;
        self
    }

    /// Set worker affinity configuration for use_workers routing
    pub fn worker_affinity(
        mut self,
        affinity: Option<crate::scheduler::WorkerAffinityConfig>,
    ) -> Self {
        self.worker_affinity = affinity;
        self
    }

    /// Automatically configure worker groups from WorkerAffinityConfig
    /// This creates:
    /// - Dedicated groups for each range-based spec (exclusive workers)
    /// - A global group for remaining workers (handles count-based and unspecified tasks)
    ///
    /// IMPORTANT: Groups are added such that self.groups[group_id] matches the group_id
    /// - self.groups[0] = global group (or dummy if no global workers)
    /// - self.groups[1] = first range group (group_id 1)
    /// - self.groups[2] = second range group (group_id 2)
    /// - etc.
    pub fn with_affinity_groups(
        mut self,
        affinity: crate::scheduler::WorkerAffinityConfig,
        total_workers: usize,
    ) -> Self {
        use std::collections::HashSet;

        // Track which worker indices are assigned to range groups
        let mut assigned_workers = HashSet::new();

        println!("========== Configuring Worker Affinity Groups ==========");

        // Calculate remaining workers for global group first
        for (_, range) in &affinity.affinity_groups {
            for worker_idx in range.start..range.end {
                assigned_workers.insert(worker_idx);
            }
        }

        let global_worker_count = total_workers.saturating_sub(assigned_workers.len());
        let has_global_workers = global_worker_count > 0;

        // Add global group at index 0 FIRST (even if 0 workers)
        if has_global_workers {
            println!(
                "  Global Group 0: {} workers (handles count-based and unspecified tasks)",
                global_worker_count
            );
            self = self.add_group(WorkerGroupConfig {
                num_workers: global_worker_count,
                core_ids: None,
                group_id: 0,
                allow_global_steal: true,
                spin_iterations: 64,
            });
        } else {
            println!("  Warning: All workers assigned to ranges!");
            println!("  Global tasks (count-based/unspecified) will be handled by range workers");
            // Add dummy group with 0 workers to maintain indexing
            self = self.add_group(WorkerGroupConfig {
                num_workers: 0,
                core_ids: None,
                group_id: 0,
                allow_global_steal: true,
                spin_iterations: 64,
            });
        }

        // Now add range groups in order of group_id
        let mut sorted_groups = affinity.affinity_groups.clone();
        sorted_groups.sort_by_key(|(gid, _)| *gid);

        for (group_id, range) in sorted_groups {
            println!(
                "  Range Group {}: workers {}-{} ({} workers)",
                group_id,
                range.start,
                range.end - 1,
                range.len()
            );

            let allow_steal = true;

            self = self.add_group(WorkerGroupConfig {
                num_workers: range.len(),
                core_ids: None,
                group_id,
                allow_global_steal: allow_steal,
                spin_iterations: 64,
            });
        }

        println!("========================================================");

        // Store the affinity config for routing
        self.worker_affinity(Some(affinity))
    }

    /// Build the scheduler
    pub fn build(self) -> CustomScheduler {
        // Calculate total workers needed
        let total_workers: usize = self.groups.iter().map(|g| g.num_workers).sum();

        // Use core allocation algorithm
        let alloc = crate::core_alloc::allocate_cores(
            self.core_offset,
            self.system_threads,
            self.receiver_threads,
            total_workers,
        );

        let system_core_offset = alloc.system_core_offset;
        let receiver_core_offset = alloc.receiver_offset;
        let worker_core_offset = alloc.worker_offset;
        let main_core = alloc.main_core.clone();

        println!("========== Custom Scheduler Core Allocation ==========");
        println!("Available cores: {}", alloc.all_core_ids.len());
        if let Some(ref mc) = main_core {
            println!("Main thread: pinned at core {:?}", mc);
        }
        println!(
            "System threads: {} at cores {}..{}",
            alloc.system_threads,
            system_core_offset,
            system_core_offset + alloc.system_threads - 1
        );
        println!(
            "Receiver threads: {} at cores {}..{}",
            alloc.receiver_threads,
            receiver_core_offset,
            receiver_core_offset + alloc.receiver_threads - 1
        );
        println!(
            "Worker threads: {} at cores {}..{}",
            total_workers,
            worker_core_offset,
            worker_core_offset + total_workers - 1
        );

        let num_groups = self.groups.len();
        let total_recorders = total_workers + alloc.receiver_threads + alloc.system_threads;
        let (shared, group_channels) = create_channels_and_state(
            num_groups,
            self.record,
            self.external_recorder,
            self.base_instant,
            system_core_offset,
            total_recorders,
        );

        let group_configs: Vec<WorkerGroupConfig> = self.groups;
        let group_worker_handles = spawn_worker_threads(
            total_workers,
            &group_configs,
            self.worker_affinity.as_ref(),
            &alloc.all_core_ids,
            worker_core_offset,
            &shared,
            &group_channels,
        );

        let groups: Vec<WorkerGroup> = group_configs
            .into_iter()
            .zip(group_worker_handles)
            .map(|(config, handles)| WorkerGroup { config, handles })
            .collect();

        let total_assigned: usize = groups.iter().map(|g| g.handles.len()).sum();
        assert_eq!(
            total_assigned, total_workers,
            "Worker assignment mismatch: {} assigned, {} expected",
            total_assigned, total_workers
        );

        println!("======================================================");

        CustomScheduler {
            shared,
            groups,
            system_core_offset,
            system_threads: alloc.system_threads,
            receiver_core_offset,
            receiver_threads: alloc.receiver_threads,
            total_workers,
            main_core,
            worker_affinity: self.worker_affinity,
        }
    }
}

/// Create the shared worker state and per-group channel sets.
fn create_channels_and_state(
    num_groups: usize,
    record: bool,
    external_recorder: Option<Arc<AsyncRecorder>>,
    base_instant: Instant,
    system_core_offset: usize,
    total_recorders: usize,
) -> (Arc<SharedWorkerState>, Vec<Arc<ChannelSet>>) {
    let async_recorder = if record {
        external_recorder.or_else(|| Some(Arc::new(AsyncRecorder::new(total_recorders, 100))))
    } else {
        None
    };

    let global_channels = ChannelSet::new();
    let group_channels: Vec<Arc<ChannelSet>> = (0..num_groups)
        .map(|_| Arc::new(ChannelSet::new()))
        .collect();

    let shared = Arc::new(SharedWorkerState {
        global_channels,
        group_channels: group_channels.clone(),
        shutdown: AtomicBool::new(false),
        total_spawned: AtomicUsize::new(0),
        total_completed: AtomicUsize::new(0),
        pending_tasks: AtomicUsize::new(0),
        async_recorder,
        base_instant: Arc::new(base_instant),
        system_core_offset,
    });

    (shared, group_channels)
}

/// Build the worker_id→group mapping, emit diagnostics, and spawn worker threads.
///
/// Returns a `Vec<Vec<JoinHandle<()>>>` indexed by group, matching `group_configs`.
fn spawn_worker_threads(
    total_workers: usize,
    group_configs: &[WorkerGroupConfig],
    worker_affinity: Option<&crate::scheduler::WorkerAffinityConfig>,
    all_core_ids: &[CoreId],
    worker_core_offset: usize,
    shared: &Arc<SharedWorkerState>,
    group_channels: &[Arc<ChannelSet>],
) -> Vec<Vec<JoinHandle<()>>> {
    let num_groups = group_configs.len();

    // Build worker_id -> group_idx mapping
    let mut worker_to_group_idx: Vec<usize> = vec![0; total_workers];
    if let Some(affinity) = worker_affinity {
        for worker_id in 0..total_workers {
            let group_ids = affinity.get_worker_groups(worker_id);
            if !group_ids.is_empty() {
                worker_to_group_idx[worker_id] = group_ids[0];
            }
        }
    }

    // Diagnostic output
    println!("========== Worker to Group Assignment ==========");
    for worker_id in 0..total_workers {
        let group_idx = worker_to_group_idx[worker_id];
        let core_id = all_core_ids[worker_core_offset + worker_id];
        println!(
            "  Worker {}: Group {} -> Core {}",
            worker_id, group_idx, core_id.id
        );
    }
    println!("================================================");

    // Spawn workers
    let mut group_worker_handles: Vec<Vec<JoinHandle<()>>> =
        (0..num_groups).map(|_| Vec::new()).collect();

    for worker_id in 0..total_workers {
        let group_idx = worker_to_group_idx[worker_id];
        let core_id = all_core_ids[worker_core_offset + worker_id];
        let shared_clone = Arc::clone(shared);
        let group_chans = Arc::clone(&group_channels[group_idx]);
        let config = &group_configs[group_idx];
        let allow_global_steal = config.allow_global_steal;
        let spin_iters = config.spin_iterations;

        let handle = thread::Builder::new()
            .name(format!("worker-{}", worker_id))
            .spawn(move || {
                worker_loop(
                    worker_id,
                    group_idx,
                    core_id,
                    shared_clone,
                    group_chans,
                    allow_global_steal,
                    spin_iters,
                );
            })
            .expect("Failed to spawn worker thread");

        group_worker_handles[group_idx].push(handle);
    }

    // Validate and report group assignments
    for (group_idx, config) in group_configs.iter().enumerate() {
        let actual_workers = group_worker_handles[group_idx].len();
        if actual_workers != config.num_workers {
            println!(
                "Warning: Group {} expected {} workers but got {}",
                group_idx, config.num_workers, actual_workers
            );
        }
        let worker_ids: Vec<usize> = worker_to_group_idx
            .iter()
            .enumerate()
            .filter(|(_, &gid)| gid == group_idx)
            .map(|(wid, _)| wid)
            .collect();
        let core_ids: Vec<usize> = worker_ids
            .iter()
            .map(|&wid| all_core_ids[worker_core_offset + wid].id)
            .collect();
        println!(
            "Worker Group {}: {} workers (indices: {:?}) on cores {:?}",
            group_idx, actual_workers, worker_ids, core_ids
        );
    }

    group_worker_handles
}

impl CustomScheduler {
    /// Create a builder for the scheduler
    pub fn builder() -> CustomSchedulerBuilder {
        CustomSchedulerBuilder::new()
    }

    /// Spawn a task with default priority to global queue
    pub fn spawn<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.spawn_with_priority(Priority::Normal, task);
    }

    /// Spawn a task with specified priority to global queue
    pub fn spawn_with_priority<F>(&self, priority: Priority, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.shared.total_spawned.fetch_add(1, Ordering::Relaxed);
        self.shared.pending_tasks.fetch_add(1, Ordering::Relaxed);

        self.shared.global_channels.send(
            priority,
            ScheduledTask {
                task: Box::new(task),
                meta: None,
            },
        );
    }

    /// Spawn a task to a specific worker group's channel
    pub fn spawn_to_group<F>(&self, group_id: usize, priority: Priority, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if group_id < self.shared.group_channels.len() {
            self.shared.total_spawned.fetch_add(1, Ordering::Relaxed);
            self.shared.pending_tasks.fetch_add(1, Ordering::Relaxed);

            self.shared.group_channels[group_id].send(
                priority,
                ScheduledTask {
                    task: Box::new(task),
                    meta: None,
                },
            );
        } else {
            // Fallback to global queue
            self.spawn_with_priority(priority, task);
        }
    }

    /// Spawn a task with metadata for recording
    pub fn spawn_with_meta<F>(&self, meta: Option<crate::TaskMeta>, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job_id = self.shared.total_spawned.fetch_add(1, Ordering::Relaxed);
        self.shared.pending_tasks.fetch_add(1, Ordering::Relaxed);

        let record_meta = meta.and_then(
            |crate::TaskMeta {
                 task_id,
                 slot,
                 index,
                 should_record,
             }| {
                if should_record {
                    Some(RecordMeta {
                        job_id,
                        task_id,
                        slot,
                        index,
                    })
                } else {
                    None
                }
            },
        );

        self.shared.global_channels.send(
            Priority::Normal,
            ScheduledTask {
                task: Box::new(task),
                meta: record_meta,
            },
        );
    }

    /// Spawn a task with metadata and priority
    pub fn spawn_with_meta_priority<F>(
        &self,
        priority: Priority,
        meta: Option<crate::TaskMeta>,
        task: F,
    ) where
        F: FnOnce() + Send + 'static,
    {
        let job_id = self.shared.total_spawned.fetch_add(1, Ordering::Relaxed);
        self.shared.pending_tasks.fetch_add(1, Ordering::Relaxed);

        let record_meta = meta.and_then(
            |crate::TaskMeta {
                 task_id,
                 slot,
                 index,
                 should_record,
             }| {
                if should_record {
                    Some(RecordMeta {
                        job_id,
                        task_id,
                        slot,
                        index,
                    })
                } else {
                    None
                }
            },
        );

        self.shared.global_channels.send(
            priority,
            ScheduledTask {
                task: Box::new(task),
                meta: record_meta,
            },
        );
    }

    /// Spawn a task to a specific worker group with metadata and priority
    pub fn spawn_to_group_with_meta<F>(
        &self,
        group_id: usize,
        priority: Priority,
        meta: Option<crate::TaskMeta>,
        task: F,
    ) where
        F: FnOnce() + Send + 'static,
    {
        if group_id < self.shared.group_channels.len() {
            let job_id = self.shared.total_spawned.fetch_add(1, Ordering::Relaxed);
            self.shared.pending_tasks.fetch_add(1, Ordering::Relaxed);

            let record_meta = meta.and_then(
                |crate::TaskMeta {
                     task_id,
                     slot,
                     index,
                     should_record,
                 }| {
                    if should_record {
                        Some(RecordMeta {
                            job_id,
                            task_id,
                            slot,
                            index,
                        })
                    } else {
                        None
                    }
                },
            );

            self.shared.group_channels[group_id].send(
                priority,
                ScheduledTask {
                    task: Box::new(task),
                    meta: record_meta,
                },
            );
        } else {
            // Fallback to global queue with priority
            self.spawn_with_meta_priority(priority, meta, task);
        }
    }

    /// Get number of pending tasks
    pub fn pending_tasks(&self) -> usize {
        self.shared.pending_tasks.load(Ordering::Relaxed)
    }

    /// Get total tasks spawned
    pub fn total_spawned(&self) -> usize {
        self.shared.total_spawned.load(Ordering::Relaxed)
    }

    /// Get total tasks completed
    pub fn total_completed(&self) -> usize {
        self.shared.total_completed.load(Ordering::Relaxed)
    }

    /// Get number of workers
    pub fn workers(&self) -> usize {
        self.total_workers
    }

    /// Get number of worker groups
    pub fn num_groups(&self) -> usize {
        self.groups.len()
    }

    /// Get system core offset
    pub fn core_offset(&self) -> usize {
        self.system_core_offset
    }

    /// Get system threads count
    pub fn system_threads(&self) -> usize {
        self.system_threads
    }

    /// Get receiver core offset
    pub fn receiver_core_offset(&self) -> usize {
        self.receiver_core_offset
    }

    /// Get receiver threads count
    pub fn receiver_threads(&self) -> usize {
        self.receiver_threads
    }

    /// Get async recorder reference
    pub fn get_async_recorder(&self) -> Option<Arc<AsyncRecorder>> {
        self.shared.async_recorder.clone()
    }

    /// Get main/orchestrator core if reserved
    pub fn main_core(&self) -> Option<CoreId> {
        self.main_core.clone()
    }

    /// Get worker affinity configuration
    pub fn get_worker_affinity(&self) -> &Option<crate::scheduler::WorkerAffinityConfig> {
        &self.worker_affinity
    }

    /// Get group_id for a given WorkerRangeSpec
    pub fn get_affinity_group(&self, use_workers: Option<&crate::WorkerRangeSpec>) -> usize {
        match &self.worker_affinity {
            Some(affinity) => affinity.get_group(use_workers),
            None => 0,
        }
    }

    /// Write records to CSV
    pub fn write_record(&self, path: &str) {
        if let Some(ref recorder) = self.shared.async_recorder {
            if let Err(e) = recorder.write_to_csv(path) {
                eprintln!("Failed to write scheduler records: {}", e);
            }
        }
    }

    /// Shutdown the scheduler and wait for all workers
    pub fn shutdown(&mut self) {
        // Signal shutdown
        self.shared.shutdown.store(true, Ordering::Release);

        // Workers will see shutdown on next select! timeout (<=100us)
        // Join all worker threads
        for group in &mut self.groups {
            for handle in group.handles.drain(..) {
                let _ = handle.join();
            }
        }
    }

    /// Wait for all pending tasks to complete (with timeout)
    pub fn wait_idle(&self, timeout: Duration) -> bool {
        let start = Instant::now();
        while self.pending_tasks() > 0 {
            if start.elapsed() > timeout {
                return false;
            }
            std::hint::spin_loop();
        }
        true
    }
}

impl Drop for CustomScheduler {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;

    #[test]
    fn test_channel_set_priority_ordering() {
        let channels = ChannelSet::new();
        let order = Arc::new(Mutex::new(Vec::new()));

        // Push in reverse priority order
        let order_low = Arc::clone(&order);
        channels.send(
            Priority::Low,
            ScheduledTask {
                task: Box::new(move || {
                    order_low.lock().unwrap().push("low");
                }),
                meta: None,
            },
        );

        let order_normal = Arc::clone(&order);
        channels.send(
            Priority::Normal,
            ScheduledTask {
                task: Box::new(move || {
                    order_normal.lock().unwrap().push("normal");
                }),
                meta: None,
            },
        );

        let order_high = Arc::clone(&order);
        channels.send(
            Priority::High,
            ScheduledTask {
                task: Box::new(move || {
                    order_high.lock().unwrap().push("high");
                }),
                meta: None,
            },
        );

        // Receive in priority order: high first
        if let Ok(st) = channels.high_rx.try_recv() {
            (st.task)();
        }
        if let Ok(st) = channels.normal_rx.try_recv() {
            (st.task)();
        }
        if let Ok(st) = channels.low_rx.try_recv() {
            (st.task)();
        }

        let result = order.lock().unwrap();
        assert_eq!(*result, vec!["high", "normal", "low"]);
    }

    #[test]
    fn test_scheduler_basic() {
        let scheduler = CustomScheduler::builder()
            .add_workers(2, 64)
            .core_offset(0)
            .system_threads(1)
            .receiver_threads(0)
            .record(false)
            .base_instant(Instant::now())
            .build();

        let counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..100 {
            let counter_clone = Arc::clone(&counter);
            scheduler.spawn(move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            });
        }

        // Wait for completion
        assert!(scheduler.wait_idle(Duration::from_secs(5)));
        assert_eq!(counter.load(Ordering::SeqCst), 100);
    }
}
