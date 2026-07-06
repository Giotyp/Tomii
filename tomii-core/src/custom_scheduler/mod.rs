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

pub mod builder;
mod channels;
mod worker;

pub use builder::CustomSchedulerBuilder;

use builder::WorkerGroup;
use channels::{Job, RecordMeta, ScheduledTask};
use core_affinity::CoreId;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use worker::SharedWorkerState;

// ============================================================================
// Public Types
// ============================================================================

/// A boxed task that can be sent across threads
pub type BoxedTask = Box<dyn FnOnce() + Send + 'static>;

/// POD descriptor for a graph-node task — the zero-allocation typed spawn path.
///
/// Instead of boxing a closure per task (heap alloc + `Arc<SharedData>` clone +
/// vtable call), the runtime sends this descriptor through the scheduler
/// channels by value; workers execute it via the node-executor hook installed
/// once at runtime init ([`CustomScheduler::set_node_executor`]).
pub struct NodeTaskDesc {
    /// Node identity: id, slot, index, and the slot generation stamped at spawn.
    pub node: crate::buffers::NodeInfo,
    /// Plugin function pointer resolved from the node cache at spawn time.
    pub func: tomii_types::CmPtr,
    /// Spawn timestamp (ns since the scheduler base instant) for latency metrics.
    pub spawn_ns: u128,
    /// Scheduler-assigned job id (set inside [`CustomScheduler::spawn_node`];
    /// callers pass 0).
    pub job_id: usize,
    /// Whether this task's execution should be submitted to the async recorder.
    pub should_record: bool,
}

/// Priority levels for task scheduling
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(u8)]
pub enum Priority {
    High = 0, // Checked first
    #[default]
    Normal = 1, // Default priority
    Low = 2,  // Background tasks
}

// ============================================================================
// Main Scheduler Struct
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
            Job::Boxed(ScheduledTask {
                task: Box::new(task),
                meta: None,
            }),
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
                Job::Boxed(ScheduledTask {
                    task: Box::new(task),
                    meta: None,
                }),
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
            Job::Boxed(ScheduledTask {
                task: Box::new(task),
                meta: record_meta,
            }),
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
            Job::Boxed(ScheduledTask {
                task: Box::new(task),
                meta: record_meta,
            }),
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
                Job::Boxed(ScheduledTask {
                    task: Box::new(task),
                    meta: record_meta,
                }),
            );
        } else {
            // Fallback to global queue with priority
            self.spawn_with_meta_priority(priority, meta, task);
        }
    }

    /// Install the executor for typed node tasks (P2 zero-alloc spawn path).
    ///
    /// Must be called before any [`Self::spawn_node`]; the runtime installs it
    /// right after `SharedData` is assembled. The hook should hold a `Weak`
    /// reference to runtime state — the scheduler is owned by that state, so an
    /// `Arc` here would form a reference cycle and leak the worker pool.
    pub fn set_node_executor(&self, exec: Box<dyn Fn(NodeTaskDesc) + Send + Sync>) {
        if self.shared.node_exec.set(exec).is_err() {
            tracing::warn!("node executor already installed; ignoring");
        }
    }

    /// Zero-allocation spawn of a graph-node task (P2 typed spawn path).
    ///
    /// Assigns the job id, then sends the POD descriptor by value — no `Box`,
    /// no closure capture. `group_id > 0` routes to that group's channels
    /// (falling back to global if out of range), matching the boxed path's
    /// `spawn_to_group_with_meta` routing.
    pub fn spawn_node(&self, group_id: usize, priority: Priority, mut desc: NodeTaskDesc) {
        desc.job_id = self.shared.total_spawned.fetch_add(1, Ordering::Relaxed);
        self.shared.pending_tasks.fetch_add(1, Ordering::Relaxed);

        if group_id > 0 && group_id < self.shared.group_channels.len() {
            self.shared.group_channels[group_id].send(priority, Job::Node(desc));
        } else {
            self.shared.global_channels.send(priority, Job::Node(desc));
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
    pub fn get_async_recorder(&self) -> Option<Arc<crate::async_recorder::AsyncRecorder>> {
        self.shared.async_recorder.clone()
    }

    /// Get main/orchestrator core if reserved
    pub fn main_core(&self) -> Option<CoreId> {
        self.main_core
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
                tracing::warn!(error = %e, "failed to write scheduler records");
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
        let start = std::time::Instant::now();
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
    use channels::{ChannelSet, ScheduledTask};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    #[test]
    fn test_channel_set_priority_ordering() {
        let channels = ChannelSet::new();
        let order = Arc::new(Mutex::new(Vec::new()));

        // Push in reverse priority order
        let order_low = Arc::clone(&order);
        channels.send(
            Priority::Low,
            Job::Boxed(ScheduledTask {
                task: Box::new(move || {
                    order_low.lock().unwrap().push("low");
                }),
                meta: None,
            }),
        );

        let order_normal = Arc::clone(&order);
        channels.send(
            Priority::Normal,
            Job::Boxed(ScheduledTask {
                task: Box::new(move || {
                    order_normal.lock().unwrap().push("normal");
                }),
                meta: None,
            }),
        );

        let order_high = Arc::clone(&order);
        channels.send(
            Priority::High,
            Job::Boxed(ScheduledTask {
                task: Box::new(move || {
                    order_high.lock().unwrap().push("high");
                }),
                meta: None,
            }),
        );

        // Receive in priority order: high first
        if let Ok(Job::Boxed(st)) = channels.high_rx.try_recv() {
            (st.task)();
        }
        if let Ok(Job::Boxed(st)) = channels.normal_rx.try_recv() {
            (st.task)();
        }
        if let Ok(Job::Boxed(st)) = channels.low_rx.try_recv() {
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

    #[test]
    fn test_typed_spawn_node_executes_via_hook() {
        let scheduler = CustomScheduler::builder()
            .add_workers(2, 64)
            .core_offset(0)
            .system_threads(1)
            .receiver_threads(0)
            .record(false)
            .base_instant(Instant::now())
            .build();

        let counter = Arc::new(AtomicUsize::new(0));
        let hook_counter = Arc::clone(&counter);
        scheduler.set_node_executor(Box::new(move |desc: NodeTaskDesc| {
            assert_eq!(desc.node.id, 7);
            assert_eq!(desc.node.index, 3);
            hook_counter.fetch_add(1, Ordering::SeqCst);
        }));

        fn dummy_func(_: &[tomii_types::CmTypes]) -> tomii_types::CmTypes {
            tomii_types::CmTypes::None
        }

        for _ in 0..100 {
            scheduler.spawn_node(
                0,
                Priority::Normal,
                NodeTaskDesc {
                    node: crate::buffers::NodeInfo::new(7, 0, 3, 0),
                    func: dummy_func,
                    spawn_ns: 0,
                    job_id: 0,
                    should_record: false,
                },
            );
        }

        assert!(scheduler.wait_idle(Duration::from_secs(5)));
        assert_eq!(counter.load(Ordering::SeqCst), 100);
        assert_eq!(scheduler.total_completed(), 100);
    }
}
