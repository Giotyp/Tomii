//! # High-Performance Custom Scheduler
//!
//! A custom thread pool designed for low-latency task execution with:
//! 1. **Spin-then-park workers** - Avoid kernel sleep overhead for idle workers
//! 2. **Priority queues** - Urgent tasks are processed first
//! 3. **Worker scoping** - Workers can be bound to specific queue groups
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                         Scheduler                               │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  WorkerGroup 0 (cores 4-8)         WorkerGroup 1 (cores 9-13)   │
//! │  ┌─────────────────────────┐       ┌─────────────────────────┐  │
//! │  │ Worker 0 ─┐             │       │ Worker 5 ─┐             │  │
//! │  │ Worker 1 ─┼─► Queue Set │       │ Worker 6 ─┼─► Queue Set │  │
//! │  │ Worker 2 ─┤   [High]    │       │ Worker 7 ─┤   [High]    │  │
//! │  │ Worker 3 ─┤   [Normal]  │       │ Worker 8 ─┤   [Normal]  │  │
//! │  │ Worker 4 ─┘   [Low]     │       │ Worker 9 ─┘   [Low]     │  │
//! │  └─────────────────────────┘       └─────────────────────────┘  │
//! │                                                                 │
//! │  Global Queues (fallback when group queues empty):              │
//! │  ┌─────────┐ ┌─────────┐ ┌─────────┐                           │
//! │  │  High   │ │ Normal  │ │   Low   │                           │
//! │  └─────────┘ └─────────┘ └─────────┘                           │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

#![allow(unused_imports)]
#![allow(dead_code)]

use core_affinity::{self, CoreId};
use crossbeam_utils::Backoff;
use parking_lot::{Condvar, Mutex, RwLock};
use std::cell::Cell;
use std::collections::HashMap;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::async_recorder::{set_worker_recorder, submit_record, AsyncRecorder};
use crate::buffers::NodeInfo;
use crate::{IdType, Record};
use synstream_types::CmTypes;

// ============================================================================
// SECTION 1: Lock-Free Task Queue (Treiber Stack with Priority)
// ============================================================================

/// A boxed task that can be sent across threads
pub type BoxedTask = Box<dyn FnOnce() + Send + 'static>;

/// Node in the lock-free task queue
struct TaskNode {
    task: BoxedTask,
    next: *mut TaskNode,
}

// Safety: TaskNode contains a Send task and raw pointer used only within atomic ops
unsafe impl Send for TaskNode {}

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

/// Lock-free MPMC task queue using Treiber stack
/// Optimized for low contention with adaptive backoff
#[derive(Debug)]
pub struct TaskQueue {
    head: AtomicPtr<TaskNode>,
    len: AtomicUsize,
    // Notification mechanism for sleeping workers
    notify_mutex: Mutex<()>,
    notify_condvar: Condvar,
}

impl TaskQueue {
    pub fn new() -> Self {
        Self {
            head: AtomicPtr::new(ptr::null_mut()),
            len: AtomicUsize::new(0),
            notify_mutex: Mutex::new(()),
            notify_condvar: Condvar::new(),
        }
    }

    /// Push a task onto the queue (lock-free) with notification
    #[inline]
    pub fn push(&self, task: BoxedTask) {
        self.push_no_notify(task);
        // Wake one waiting worker
        self.notify_condvar.notify_one();
    }

    /// Push a task onto the queue (lock-free) WITHOUT notification
    /// Used when caller will handle notification (e.g., PriorityQueueSet)
    #[inline]
    pub fn push_no_notify(&self, task: BoxedTask) {
        let new_node = Box::into_raw(Box::new(TaskNode {
            task,
            next: ptr::null_mut(),
        }));

        loop {
            let current_head = self.head.load(Ordering::Relaxed);
            unsafe {
                (*new_node).next = current_head;
            }

            if self
                .head
                .compare_exchange_weak(current_head, new_node, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                self.len.fetch_add(1, Ordering::Relaxed);
                return;
            }
            // CAS failed, retry (no backoff needed for push - rare contention)
        }
    }

    /// Try to pop a task from the queue (lock-free, non-blocking)
    #[inline]
    pub fn try_pop(&self) -> Option<BoxedTask> {
        let backoff = Backoff::new();

        loop {
            let current_head = self.head.load(Ordering::Acquire);
            if current_head.is_null() {
                return None;
            }

            let next = unsafe { (*current_head).next };

            if self
                .head
                .compare_exchange_weak(current_head, next, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                self.len.fetch_sub(1, Ordering::Relaxed);
                let node = unsafe { Box::from_raw(current_head) };
                return Some(node.task);
            }

            // Backoff on contention
            if backoff.is_completed() {
                return None; // Give up after too many retries
            }
            backoff.spin();
        }
    }

    /// Check if queue is empty (relaxed, may be stale)
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.head.load(Ordering::Relaxed).is_null()
    }

    /// Approximate length (relaxed ordering)
    #[inline]
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    /// Wait for a task with timeout (for idle workers)
    pub fn wait_for_task(&self, timeout: Duration) -> bool {
        let lock = self.notify_mutex.lock();
        if !self.is_empty() {
            return true; // Task available, no need to wait
        }
        self.notify_condvar.wait_for(&mut { lock }, timeout);
        !self.is_empty()
    }

    /// Wake all waiting workers (for shutdown)
    pub fn wake_all(&self) {
        self.notify_condvar.notify_all();
    }
}

impl Drop for TaskQueue {
    fn drop(&mut self) {
        // Drain remaining tasks
        while let Some(_task) = self.try_pop() {
            // Tasks are dropped here
        }
    }
}

// ============================================================================
// SECTION 2: Priority Queue Set (Multiple Priorities)
// ============================================================================

/// A set of queues with different priority levels
/// Workers check High → Normal → Low in order
pub struct PriorityQueueSet {
    queues: [TaskQueue; 3], // High, Normal, Low
    // Shared notification mechanism for ALL priorities
    // This ensures workers wake up regardless of which priority queue receives a task
    notify_mutex: Mutex<()>,
    notify_condvar: Condvar,
}

impl PriorityQueueSet {
    pub fn new() -> Self {
        Self {
            queues: [TaskQueue::new(), TaskQueue::new(), TaskQueue::new()],
            notify_mutex: Mutex::new(()),
            notify_condvar: Condvar::new(),
        }
    }

    /// Push a task with specified priority
    /// Uses shared notification to wake ANY waiting worker
    #[inline]
    pub fn push(&self, priority: Priority, task: BoxedTask) {
        self.queues[priority as usize].push_no_notify(task);
        // Notify shared condvar - wakes workers waiting on ANY priority
        self.notify_condvar.notify_one();
    }

    /// Try to pop from highest priority queue first
    #[inline]
    pub fn try_pop(&self) -> Option<BoxedTask> {
        // Check High priority first
        if let Some(task) = self.queues[Priority::High as usize].try_pop() {
            return Some(task);
        }
        // Then Normal
        if let Some(task) = self.queues[Priority::Normal as usize].try_pop() {
            return Some(task);
        }
        // Finally Low
        self.queues[Priority::Low as usize].try_pop()
    }

    /// Check if all queues are empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.queues.iter().all(|q| q.is_empty())
    }

    /// Total pending tasks across all priorities
    pub fn total_len(&self) -> usize {
        self.queues.iter().map(|q| q.len()).sum()
    }

    /// Wake workers waiting on any queue
    pub fn wake_all(&self) {
        // Wake shared condvar
        self.notify_condvar.notify_all();
        // Also wake individual queue condvars for backwards compatibility
        for q in &self.queues {
            q.wake_all();
        }
    }

    /// Get reference to specific priority queue (for direct waiting)
    pub fn get_queue(&self, priority: Priority) -> &TaskQueue {
        &self.queues[priority as usize]
    }

    /// Wait for a task on ANY priority queue with timeout
    /// This is the preferred method for workers to wait for tasks
    pub fn wait_for_any_task(&self, timeout: Duration) -> bool {
        let guard = self.notify_mutex.lock();
        if !self.is_empty() {
            return true; // Task available, no need to wait
        }
        self.notify_condvar.wait_for(&mut { guard }, timeout);
        !self.is_empty()
    }
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
            spin_iterations: 64, // ~1-2µs of spinning on modern CPUs
        }
    }
}

/// Internal state for a worker group
struct WorkerGroup {
    config: WorkerGroupConfig,
    /// Local priority queues for this group
    local_queues: Arc<PriorityQueueSet>,
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
    worker_id: usize,      // Global worker index
    group_id: usize,       // Which group this worker belongs to
    core_id: usize,        // Physical core ID
    tasks_executed: usize, // Counter for metrics
}

/// Shared state for all workers
struct SharedWorkerState {
    /// Global queues (fallback when group queues empty)
    global_queues: Arc<PriorityQueueSet>,
    /// Per-group local queues
    group_queues: Vec<Arc<PriorityQueueSet>>,
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
    /// Stream filter for recording
    record_stream: Option<usize>,
    /// Available stream slots for recording filter
    available_stream_slots: Arc<RwLock<Vec<usize>>>,
    /// System core offset for recorder channel indexing (Bug 2 fix)
    system_core_offset: usize,
}

/// Worker thread main loop
fn worker_loop(
    worker_id: usize,
    group_id: usize,
    core_id: CoreId,
    shared: Arc<SharedWorkerState>,
    local_queues: Arc<PriorityQueueSet>,
    spin_iterations: usize,
    allow_global_steal: bool,
) {
    // Pin to core
    core_affinity::set_for_current(core_id);

    // Set thread-local state (Bug 3 fix: set both WORKER_ID and WORKER_INDEX)
    crate::scheduler::set_current_worker_id(core_id.id);
    crate::scheduler::set_current_worker_index(worker_id);

    // Set internal thread-local state for group membership
    WORKER_STATE.with(|s| {
        s.set(WorkerState {
            worker_id,
            group_id,
            core_id: core_id.id,
            tasks_executed: 0,
        });
    });

    // Initialize async recorder channel if enabled (Bug 2 fix: use correct channel index)
    if let Some(ref recorder) = shared.async_recorder {
        let channel_index = core_id.id - shared.system_core_offset;
        if let Some(tx) = recorder.get_worker_sender(channel_index) {
            set_worker_recorder(tx);
        }
    }

    let park_timeout = Duration::from_micros(100); // 100µs park timeout

    loop {
        // Check shutdown first
        if shared.shutdown.load(Ordering::Acquire) {
            break;
        }

        // Phase 1: Try local group queues (no spinning yet)
        if let Some(task) = local_queues.try_pop() {
            execute_task_wrapper(&shared, task);
            continue;
        }

        // Phase 2: Try global queues if allowed
        if allow_global_steal {
            if let Some(task) = shared.global_queues.try_pop() {
                execute_task_wrapper(&shared, task);
                continue;
            }
        }

        // Phase 3: Adaptive spinning (stay in user-space)
        let mut found_task = false;
        for _ in 0..spin_iterations {
            // Check shutdown during spin
            if shared.shutdown.load(Ordering::Relaxed) {
                return;
            }

            // Spin-wait hint (PAUSE instruction on x86)
            std::hint::spin_loop();

            // Re-check local queues
            if let Some(task) = local_queues.try_pop() {
                execute_task_wrapper(&shared, task);
                found_task = true;
                break;
            }

            // Re-check global queues
            if allow_global_steal {
                if let Some(task) = shared.global_queues.try_pop() {
                    execute_task_wrapper(&shared, task);
                    found_task = true;
                    break;
                }
            }
        }

        if found_task {
            continue;
        }

        // Phase 4: Park with short timeout (kernel-space, but bounded)
        // This is the key optimization: we park briefly instead of sleeping indefinitely
        // Use wait_for_any_task to wake on ANY priority queue task arrival
        let _has_task = local_queues.wait_for_any_task(park_timeout);

        // After waking, immediately loop back to check queues
        // No explicit task extraction here - the loop handles it
    }
}

#[inline]
fn execute_task_wrapper(_shared: &Arc<SharedWorkerState>, task: BoxedTask) {
    // Pure pass-through: all metrics handling is in the task wrappers
    // (spawn_with_priority, spawn_to_group, spawn_with_meta)
    task();
}

// ============================================================================
// SECTION 5: Main Scheduler Implementation
// ============================================================================

/// High-performance custom scheduler with priority queues and worker scoping
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
    /// Optional reserved core for main/orchestrator thread (Bug 4 fix)
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
    record_stream: Option<usize>,
    available_stream_slots: Arc<RwLock<Vec<usize>>>,
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
            record_stream: None,
            available_stream_slots: Arc::new(RwLock::new(Vec::new())),
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
            core_ids: None, // Will be auto-assigned
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

    /// Set stream filter for recording
    pub fn record_stream(mut self, stream: Option<usize>) -> Self {
        self.record_stream = stream;
        self
    }

    /// Set available stream slots reference
    pub fn available_stream_slots(mut self, slots: Arc<RwLock<Vec<usize>>>) -> Self {
        self.available_stream_slots = slots;
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
        // This ensures self.groups[0] is the global group
        if has_global_workers {
            println!(
                "  Global Group 0: {} workers (handles count-based and unspecified tasks)",
                global_worker_count
            );
            self = self.add_group(WorkerGroupConfig {
                num_workers: global_worker_count,
                core_ids: None,
                group_id: 0,
                allow_global_steal: true, // Can handle global tasks
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
        // This ensures self.groups[group_id] = correct group
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

            // Range workers should always be able to steal from global queue when idle
            // This ensures tasks without use_workers can utilize ALL workers, not just global group
            // Semantics: "use_workers: 0-7" means "task MUST run on 0-7"
            //            NOT "workers 0-7 can ONLY run these tasks"
            let allow_steal = true;

            self = self.add_group(WorkerGroupConfig {
                num_workers: range.len(),
                core_ids: None, // Auto-assign during build()
                group_id,
                allow_global_steal: allow_steal, // Always allow stealing from global queue
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

        // Use core allocation algorithm (Bug 4 fix)
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

        // Create async recorder if needed
        let total_recorders = total_workers + alloc.receiver_threads + alloc.system_threads;
        let async_recorder = if self.record {
            self.external_recorder
                .or_else(|| Some(Arc::new(AsyncRecorder::new(total_recorders, 100))))
        } else {
            None
        };

        // Create global queues
        let global_queues = Arc::new(PriorityQueueSet::new());

        // Create per-group queues
        let group_queues: Vec<Arc<PriorityQueueSet>> = (0..self.groups.len())
            .map(|_| Arc::new(PriorityQueueSet::new()))
            .collect();

        // Create shared state (Bug 2 fix: add system_core_offset for recorder channel indexing)
        let shared = Arc::new(SharedWorkerState {
            global_queues: Arc::clone(&global_queues),
            group_queues: group_queues.clone(),
            shutdown: AtomicBool::new(false),
            total_spawned: AtomicUsize::new(0),
            total_completed: AtomicUsize::new(0),
            pending_tasks: AtomicUsize::new(0),
            async_recorder,
            base_instant: Arc::new(self.base_instant),
            record_stream: self.record_stream,
            available_stream_slots: self.available_stream_slots,
            system_core_offset,
        });

        // ===================================================================
        // WORKER AFFINITY BUG FIX: Index-Based Worker Assignment
        // ===================================================================
        // The fix changes from sequential worker creation (per-group) to
        // index-based assignment where workers are created 0..total_workers
        // and each is assigned to the correct group based on WorkerAffinityConfig
        // ===================================================================

        // Step 1: Pre-allocate group structures
        let group_configs: Vec<WorkerGroupConfig> = self.groups;
        let num_groups = group_configs.len();
        let mut group_worker_handles: Vec<Vec<JoinHandle<()>>> =
            (0..num_groups).map(|_| Vec::new()).collect();

        // Step 2: Build worker_id → group_idx mapping from WorkerAffinityConfig
        // Default: all workers belong to global group (group 0)
        let mut worker_to_group_idx: Vec<usize> = vec![0; total_workers];

        if let Some(ref affinity) = self.worker_affinity {
            for worker_id in 0..total_workers {
                let group_ids = affinity.get_worker_groups(worker_id);
                if !group_ids.is_empty() {
                    // Worker belongs to range group (use first group_id if overlapping ranges)
                    worker_to_group_idx[worker_id] = group_ids[0];
                }
                // else: worker stays in global group (group_idx 0, already defaulted)
            }
        }

        // Step 3: Diagnostic output - show worker assignment before creation
        println!("========== Worker to Group Assignment ==========");
        for worker_id in 0..total_workers {
            let group_idx = worker_to_group_idx[worker_id];
            let core_id = alloc.all_core_ids[worker_core_offset + worker_id];
            println!(
                "  Worker {}: Group {} → Core {}",
                worker_id, group_idx, core_id.id
            );
        }
        println!("================================================");

        // Step 4: Create workers by index (0..total_workers), assigning to correct groups
        for worker_id in 0..total_workers {
            let group_idx = worker_to_group_idx[worker_id];
            let core_id = alloc.all_core_ids[worker_core_offset + worker_id];

            // Clone shared state for this worker
            let shared_clone = Arc::clone(&shared);
            let local_queues_clone = Arc::clone(&group_queues[group_idx]);

            // Get group configuration
            let config = &group_configs[group_idx];
            let spin_iterations = config.spin_iterations;
            let allow_global_steal = config.allow_global_steal;

            // Spawn worker thread
            let handle = thread::Builder::new()
                .name(format!("worker-{}", worker_id))
                .spawn(move || {
                    worker_loop(
                        worker_id, // Global worker index (0..total_workers)
                        group_idx, // Group this worker belongs to
                        core_id,   // Physical core to pin to
                        shared_clone,
                        local_queues_clone,
                        spin_iterations,
                        allow_global_steal,
                    );
                })
                .expect("Failed to spawn worker thread");

            // Add handle to the correct group's handle vector
            group_worker_handles[group_idx].push(handle);
        }

        // Step 5: Diagnostic output and validation
        for (group_idx, config) in group_configs.iter().enumerate() {
            let actual_workers = group_worker_handles[group_idx].len();

            // Validate: actual worker count matches config expectation
            if actual_workers != config.num_workers {
                println!(
                    "Warning: Group {} expected {} workers but got {}",
                    group_idx, config.num_workers, actual_workers
                );
            }

            // Collect worker IDs and core IDs for this group (for diagnostic output)
            let worker_ids: Vec<usize> = worker_to_group_idx
                .iter()
                .enumerate()
                .filter(|(_, &gid)| gid == group_idx)
                .map(|(wid, _)| wid)
                .collect();

            let core_ids: Vec<usize> = worker_ids
                .iter()
                .map(|&worker_id| alloc.all_core_ids[worker_core_offset + worker_id].id)
                .collect();

            println!(
                "Worker Group {}: {} workers (indices: {:?}) on cores {:?}",
                group_idx, actual_workers, worker_ids, core_ids
            );
        }

        // Step 6: Assemble WorkerGroup structs by pairing configs with handles
        let mut groups = Vec::new();
        for (group_idx, (config, handles)) in group_configs
            .into_iter()
            .zip(group_worker_handles.into_iter())
            .enumerate()
        {
            groups.push(WorkerGroup {
                config,
                local_queues: group_queues[group_idx].clone(),
                handles,
            });
        }

        // Step 7: Validate total worker assignment
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
        let shared = Arc::clone(&self.shared);
        self.shared.total_spawned.fetch_add(1, Ordering::Relaxed);
        self.shared.pending_tasks.fetch_add(1, Ordering::Relaxed);

        // Wrap task with metrics (all tasks must be wrapped)
        let wrapped_task = move || {
            task();
            shared.pending_tasks.fetch_sub(1, Ordering::Relaxed);
            shared.total_completed.fetch_add(1, Ordering::Relaxed);
        };

        self.shared
            .global_queues
            .push(priority, Box::new(wrapped_task));
    }

    /// Spawn a task to a specific worker group's local queue
    pub fn spawn_to_group<F>(&self, group_id: usize, priority: Priority, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if group_id < self.groups.len() {
            let shared = Arc::clone(&self.shared);
            self.shared.total_spawned.fetch_add(1, Ordering::Relaxed);
            self.shared.pending_tasks.fetch_add(1, Ordering::Relaxed);

            // Wrap task with metrics
            let wrapped_task = move || {
                task();
                shared.pending_tasks.fetch_sub(1, Ordering::Relaxed);
                shared.total_completed.fetch_add(1, Ordering::Relaxed);
            };

            self.groups[group_id]
                .local_queues
                .push(priority, Box::new(wrapped_task));
        } else {
            // Fallback to global queue
            self.spawn_with_priority(priority, task);
        }
    }

    /// Spawn a task with metadata for recording
    pub fn spawn_with_meta<F>(&self, meta: Option<(IdType, usize, usize)>, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let shared = Arc::clone(&self.shared);
        let job_id = self.shared.total_spawned.fetch_add(1, Ordering::Relaxed);
        self.shared.pending_tasks.fetch_add(1, Ordering::Relaxed);

        let (task_id, slot, index) = meta.unwrap_or((IdType::MIN, usize::MIN, usize::MIN));

        let wrapped_task = move || {
            let start = shared.base_instant.elapsed().as_nanos();
            task();
            let end = shared.base_instant.elapsed().as_nanos();

            // Record if enabled
            if shared.async_recorder.is_some() {
                let should_record = match shared.record_stream {
                    None => true,
                    Some(target_stream) => {
                        let slots_read = shared.available_stream_slots.read();
                        let current_stream = slots_read.get(slot).copied().unwrap_or(usize::MAX);
                        current_stream == target_stream
                    }
                };

                if should_record {
                    let worker = WORKER_STATE.with(|s| s.get().core_id);
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
            }

            shared.pending_tasks.fetch_sub(1, Ordering::Relaxed);
            shared.total_completed.fetch_add(1, Ordering::Relaxed);
        };

        self.shared
            .global_queues
            .push(Priority::Normal, Box::new(wrapped_task));
    }

    /// Spawn a task with metadata and priority
    pub fn spawn_with_meta_priority<F>(
        &self,
        priority: Priority,
        meta: Option<(IdType, usize, usize)>,
        task: F,
    ) where
        F: FnOnce() + Send + 'static,
    {
        let shared = Arc::clone(&self.shared);
        let job_id = self.shared.total_spawned.fetch_add(1, Ordering::Relaxed);
        self.shared.pending_tasks.fetch_add(1, Ordering::Relaxed);

        let (task_id, slot, index) = meta.unwrap_or((IdType::MIN, usize::MIN, usize::MIN));

        let wrapped_task = move || {
            let start = shared.base_instant.elapsed().as_nanos();
            task();
            let end = shared.base_instant.elapsed().as_nanos();

            // Record if enabled
            if shared.async_recorder.is_some() {
                let should_record = match shared.record_stream {
                    None => true,
                    Some(target_stream) => {
                        let slots_read = shared.available_stream_slots.read();
                        let current_stream = slots_read.get(slot).copied().unwrap_or(usize::MAX);
                        current_stream == target_stream
                    }
                };

                if should_record {
                    let worker = WORKER_STATE.with(|s| s.get().core_id);
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
            }

            shared.pending_tasks.fetch_sub(1, Ordering::Relaxed);
            shared.total_completed.fetch_add(1, Ordering::Relaxed);
        };

        self.shared
            .global_queues
            .push(priority, Box::new(wrapped_task));
    }

    /// Spawn a task to a specific worker group with metadata and priority
    pub fn spawn_to_group_with_meta<F>(
        &self,
        group_id: usize,
        priority: Priority,
        meta: Option<(IdType, usize, usize)>,
        task: F,
    ) where
        F: FnOnce() + Send + 'static,
    {
        if group_id < self.groups.len() {
            let shared = Arc::clone(&self.shared);
            let job_id = self.shared.total_spawned.fetch_add(1, Ordering::Relaxed);
            self.shared.pending_tasks.fetch_add(1, Ordering::Relaxed);

            let (task_id, slot, index) = meta.unwrap_or((IdType::MIN, usize::MIN, usize::MIN));

            let wrapped_task = move || {
                let start = shared.base_instant.elapsed().as_nanos();
                task();
                let end = shared.base_instant.elapsed().as_nanos();

                // Record if enabled
                if shared.async_recorder.is_some() {
                    let should_record = match shared.record_stream {
                        None => true,
                        Some(target_stream) => {
                            let slots_read = shared.available_stream_slots.read();
                            let current_stream =
                                slots_read.get(slot).copied().unwrap_or(usize::MAX);
                            current_stream == target_stream
                        }
                    };

                    if should_record {
                        let worker = WORKER_STATE.with(|s| s.get().core_id);
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
                }

                shared.pending_tasks.fetch_sub(1, Ordering::Relaxed);
                shared.total_completed.fetch_add(1, Ordering::Relaxed);
            };

            self.groups[group_id]
                .local_queues
                .push(priority, Box::new(wrapped_task));
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

    /// Get main/orchestrator core if reserved (Bug 4 fix)
    pub fn main_core(&self) -> Option<CoreId> {
        self.main_core.clone()
    }

    /// Get worker affinity configuration
    pub fn get_worker_affinity(&self) -> &Option<crate::scheduler::WorkerAffinityConfig> {
        &self.worker_affinity
    }

    /// Get group_id for a given WorkerRangeSpec
    /// Returns 0 for None (global) or the mapped group_id for specific worker specs
    pub fn get_affinity_group(&self, use_workers: Option<&crate::WorkerRangeSpec>) -> usize {
        match &self.worker_affinity {
            Some(affinity) => affinity.get_group(use_workers),
            None => 0, // No affinity config - always use global
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

        // Wake all workers
        self.shared.global_queues.wake_all();
        for group in &self.groups {
            group.local_queues.wake_all();
        }

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

    #[test]
    fn test_task_queue_basic() {
        let queue = TaskQueue::new();
        assert!(queue.is_empty());

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        queue.push(Box::new(move || {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        }));

        assert!(!queue.is_empty());
        assert_eq!(queue.len(), 1);

        let task = queue.try_pop().unwrap();
        task();

        assert_eq!(counter.load(Ordering::SeqCst), 1);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_priority_queue_ordering() {
        let pq = PriorityQueueSet::new();

        let order = Arc::new(Mutex::new(Vec::new()));

        // Push in reverse priority order
        let order_low = Arc::clone(&order);
        pq.push(
            Priority::Low,
            Box::new(move || {
                order_low.lock().push("low");
            }),
        );

        let order_normal = Arc::clone(&order);
        pq.push(
            Priority::Normal,
            Box::new(move || {
                order_normal.lock().push("normal");
            }),
        );

        let order_high = Arc::clone(&order);
        pq.push(
            Priority::High,
            Box::new(move || {
                order_high.lock().push("high");
            }),
        );

        // Pop should return High first
        pq.try_pop().unwrap()();
        pq.try_pop().unwrap()();
        pq.try_pop().unwrap()();

        let result = order.lock();
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
