use super::channels::ChannelSet;
use super::worker::{worker_loop, SharedWorkerState};
use crate::async_recorder::AsyncRecorder;
use core_affinity::CoreId;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Instant;

// ============================================================================
// Worker Group Configuration
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
pub(super) struct WorkerGroup {
    #[allow(dead_code)] // retained for future per-group config queries
    pub(super) config: WorkerGroupConfig,
    /// Worker thread handles
    pub(super) handles: Vec<JoinHandle<()>>,
}

// ============================================================================
// Builder
// ============================================================================

/// Builder for CustomScheduler
pub struct CustomSchedulerBuilder {
    pub(super) groups: Vec<WorkerGroupConfig>,
    pub(super) core_offset: usize,
    pub(super) system_threads: usize,
    pub(super) receiver_threads: usize,
    pub(super) record: bool,
    pub(super) external_recorder: Option<Arc<AsyncRecorder>>,
    pub(super) base_instant: Instant,
    pub(super) worker_affinity: Option<crate::scheduler::WorkerAffinityConfig>,
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
            tracing::info!(global_worker_count, "configuring global worker group");
            self = self.add_group(WorkerGroupConfig {
                num_workers: global_worker_count,
                core_ids: None,
                group_id: 0,
                allow_global_steal: true,
                spin_iterations: 64,
            });
        } else {
            tracing::warn!("all workers assigned to ranges; global tasks handled by range workers");
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
            tracing::info!(
                group_id,
                start = range.start,
                end = range.end,
                "configuring range group"
            );

            self = self.add_group(WorkerGroupConfig {
                num_workers: range.len(),
                core_ids: None,
                group_id,
                allow_global_steal: true,
                spin_iterations: 64,
            });
        }

        // Store the affinity config for routing
        self.worker_affinity(Some(affinity))
    }

    /// Build the scheduler
    pub fn build(self) -> super::CustomScheduler {
        // Calculate total workers needed
        let requested_workers: usize = self.groups.iter().map(|g| g.num_workers).sum();

        // Use core allocation algorithm; may scale down on over-subscribed machines
        let alloc = crate::core_alloc::allocate_cores(
            self.core_offset,
            self.system_threads,
            self.receiver_threads,
            requested_workers,
        );
        let total_workers = alloc.worker_count;

        let system_core_offset = alloc.system_core_offset;
        let receiver_core_offset = alloc.receiver_offset;
        let worker_core_offset = alloc.worker_offset;
        let main_core = alloc.main_core;

        tracing::info!(
            available_cores = alloc.all_core_ids.len(),
            system_threads = alloc.system_threads,
            system_core_start = system_core_offset,
            receiver_threads = alloc.receiver_threads,
            receiver_core_start = receiver_core_offset,
            worker_threads = total_workers,
            worker_core_start = worker_core_offset,
            main_core = ?main_core,
            "custom scheduler core allocation"
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

        super::CustomScheduler {
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
pub(super) fn create_channels_and_state(
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
pub(super) fn spawn_worker_threads(
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
        for (worker_id, slot) in worker_to_group_idx.iter_mut().enumerate() {
            let group_ids = affinity.get_worker_groups(worker_id);
            if !group_ids.is_empty() {
                *slot = group_ids[0];
            }
        }
    }

    for worker_id in 0..total_workers {
        let group_idx = worker_to_group_idx[worker_id];
        let core_id = all_core_ids[worker_core_offset + worker_id];
        tracing::debug!(
            worker_id,
            group_idx,
            core = core_id.id,
            "worker-to-group assignment"
        );
    }

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
            tracing::warn!(
                group_idx,
                expected = config.num_workers,
                actual = actual_workers,
                "worker count mismatch"
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
        tracing::info!(
            group_idx,
            actual_workers,
            ?worker_ids,
            ?core_ids,
            "worker group ready"
        );
    }

    group_worker_handles
}
