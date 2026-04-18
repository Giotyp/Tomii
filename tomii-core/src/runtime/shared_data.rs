//! All shared-state structs passed between runtime threads.
//!
//! [`SharedData`] is an intentional "god-object": every sub-system that runs concurrently
//! needs access to overlapping sets of fields, and threading individual sub-structs
//! through every function signature would be unwieldy.  The design compromise is that
//! hot-path functions receive narrow borrows (`&SlotData`, `&ExecCtx`, etc.) rather than
//! `&SharedData` directly, keeping coupling at the type level even when the root
//! allocation is shared.
//!
//! This module owns **only** struct definitions and their small inherent helpers
//! (`Telemetry::record_timing` etc.).  No threading, scheduling, or slot logic lives
//! here — those belong to `threading`, `scheduling`, and `slot_lifecycle` respectively.

#[cfg(feature = "network")]
use crate::network::{NetworkSocket, PacketMessage};
use crate::resolution_state::ResolutionState;
use crate::time_buffer::TimeBufferManager;
use crate::{buffers::*, graph::*, scheduler::*};
#[cfg(feature = "network")]
use flume::{Receiver, Sender};
#[cfg(feature = "network")]
use parking_lot::Mutex;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use std::sync::Arc;
use std::time::Instant;
use tomii_types::*;

/// Type aliases for the crossbeam batch_queue channel used by workers → resolution threads.
pub type BatchQueueTx = crossbeam_channel::Sender<crate::buffers::NodeInfo>;
pub type BatchQueueRx = crossbeam_channel::Receiver<crate::buffers::NodeInfo>;

/// Slot state for priority-based processing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    Active,    // Slot is actively processing and sending tasks to scheduler
    Buffering, // Slot is buffering with tasks
    Inactive,  // Slot is inactive with no tasks
}

/// Precomputed node cache and predecessor routing tables.
pub struct GraphCache {
    pub node_cache: Vec<super::node_cache::NodeCacheEntry>,
    #[allow(clippy::type_complexity)]
    pub pred_index_filter: Arc<Vec<Vec<Option<(usize, usize)>>>>,
    pub pred_group_by: Arc<Vec<Vec<Option<usize>>>>,
    pub pred_succ_1to1_offset: Arc<Vec<Vec<Option<isize>>>>,
    pub total_tasks: usize,
    pub total_cond_tasks: usize,
    /// Materialized initialization objects, indexed by `$ref` IDs in `Arg` values.
    /// Moved here from `Graph` so the graph remains a pure topological description.
    pub init_objects: Vec<Vec<CmTypes>>,
}

/// Tuning parameters for the worker spin-wait loop.
///
/// Controls the three-phase back-off when a worker is waiting on a
/// predecessor result: spin → yield → park.
#[derive(Debug, Clone, Copy)]
pub struct SpinWaitConfig {
    /// Iterations of `spin_loop()` before switching to `yield_now()`.
    pub spin_iters: u32,
    /// Iterations of `yield_now()` before switching to `park_timeout()`.
    pub yield_iters: u32,
    /// Duration (nanoseconds) per `park_timeout()` call.
    pub park_ns: u64,
}

impl Default for SpinWaitConfig {
    fn default() -> Self {
        Self {
            spin_iters: 64,
            yield_iters: 256,
            park_ns: 100,
        }
    }
}

/// Tuning parameters for the resolution-loop batch processor.
#[derive(Debug, Clone, Copy)]
pub struct BatchConfig {
    /// Maximum tasks drained from the batch queue per loop iteration.
    pub target_size: usize,
    /// Microseconds to wait on an empty queue before moving on.
    pub timeout_us: u64,
    /// Spin iterations when the queue is initially empty (catches burst completions).
    pub poll_spin_iters: u32,
    /// Flush accumulated successors to workers every this many items.
    pub flush_threshold: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            target_size: 1,
            timeout_us: 10,
            poll_spin_iters: 32,
            flush_threshold: 32,
        }
    }
}

/// All immutable configuration and tuning knobs.
#[derive(Clone)]
pub struct RuntimeConfig {
    pub slots: usize,
    pub max_streams: usize,
    pub max_runtime: Option<u64>,
    pub system_threads: usize,
    pub receiver_threads: usize,
    pub workers: usize,
    pub core_offset: usize,
    pub receiver_core_offset: usize,
    pub slot_priority_enabled: bool,
    pub coalesce_barriers: bool,
    pub inline_continuation: bool,
    pub single_slot_mode: bool,
    pub record_stream: Option<usize>,
    pub recv_pool_size: usize,
    pub spin_wait: SpinWaitConfig,
    pub batch: BatchConfig,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            slots: 1,
            max_streams: 1,
            max_runtime: None,
            system_threads: 1,
            receiver_threads: 0,
            workers: 1,
            core_offset: 0,
            receiver_core_offset: 0,
            slot_priority_enabled: false,
            coalesce_barriers: false,
            inline_continuation: false,
            single_slot_mode: true,
            record_stream: None,
            recv_pool_size: 1024,
            spin_wait: SpinWaitConfig::default(),
            batch: BatchConfig::default(),
        }
    }
}

/// All per-slot atomics and lock-protected slot state.
pub struct SlotData {
    /// Slot generation counters — incremented on slot completion for lazy reinit.
    pub generation: Arc<Vec<AtomicU64>>,
    pub pending_tasks: Arc<Vec<AtomicUsize>>,
    pub pending_cond_tasks: Arc<Vec<AtomicUsize>>,
    pub processing_count: Arc<Vec<AtomicUsize>>,
    pub needs_check: Arc<Vec<AtomicBool>>,
    pub packet_counters: Arc<Vec<AtomicUsize>>,
    pub packet_complete: Arc<Vec<AtomicBool>>,
    pub stream_id: Arc<Vec<AtomicUsize>>,
    pub active_bitmap: Arc<AtomicU64>,
    /// Condition node spawn tracking per slot.
    pub cond_instances_to_spawn: Arc<Vec<Vec<AtomicU64>>>,
    pub states: Arc<RwLock<Vec<SlotState>>>,
    pub running_streams: Arc<RwLock<Vec<(usize, usize)>>>,
    /// Per-slot buffering: holds ready nodes with packet data waiting for slot activation.
    #[allow(clippy::type_complexity)]
    pub buffers: Arc<RwLock<Vec<Vec<(NodeInfo, Option<CmTypes>)>>>>,
    pub last_assigned: Arc<AtomicUsize>,
}

/// Network receiver infrastructure — present only when the `network` feature is enabled.
#[cfg(feature = "network")]
pub struct NetworkInfra {
    pub receive_finished: Arc<AtomicBool>,
    /// Flume MPSC channel from network receivers to resolution threads.
    pub packet_sender: Sender<PacketMessage>,
    pub packet_receiver: Receiver<PacketMessage>,
    pub receiver_sockets: Arc<Vec<NetworkSocket>>,
    pub packet_drop_counters: Arc<Vec<AtomicUsize>>,
    /// Per-socket buffer return channels: resolution thread → receiver thread.
    pub buffer_return_senders: Vec<Sender<Vec<u8>>>,
    /// Receiver ends taken exactly once when the corresponding receiver thread is spawned.
    pub buffer_return_receivers: Vec<Mutex<Option<Receiver<Vec<u8>>>>>,
    pub streams_receive_counter: Arc<AtomicUsize>,
    /// Counts frames dropped because no slot was available when they arrived.
    pub dropped_streams: Arc<AtomicUsize>,
    /// Per-frame drop bitmap — prevents double-counting of dropped frames.
    pub frame_dropped: Arc<Vec<AtomicBool>>,
}

/// Scheduler, batch queue, resolution state, and result storage.
pub struct ExecCtx {
    pub scheduler: Arc<SchedulerImpl>,
    pub batch_queue_tx: BatchQueueTx,
    pub batch_queue_rx: BatchQueueRx,
    pub resolution_state: Arc<dyn ResolutionState>,
    pub node_results: Arc<crate::buffers::LockFreeResultMap>,
    pub initial_prep_done: Arc<AtomicUsize>,
}

/// Timing, recording, and stream counters.
pub struct Telemetry {
    pub time_buffer: Option<Arc<TimeBufferManager>>,
    pub async_recorder: Option<Arc<crate::async_recorder::AsyncRecorder>>,
    pub base_instant: Arc<Instant>,
    pub job_counter: Arc<AtomicUsize>,
    pub stream_complete_counter: Arc<AtomicUsize>,
}

impl Telemetry {
    /// Capture a start timestamp for a timed section.
    /// Returns `None` when timing is not enabled.
    #[inline]
    pub fn measure_start(&self) -> Option<crate::time_buffer::TimingMethod> {
        self.time_buffer.as_ref().map(|tb| tb.measure_time())
    }

    /// Record a named timing section. No-op when timing is disabled or `start` is `None`.
    #[inline]
    pub fn record_timing(
        &self,
        start: Option<crate::time_buffer::TimingMethod>,
        slot: usize,
        label: &str,
        worker: usize,
    ) {
        if let (Some(tb), Some(start)) = (&self.time_buffer, start) {
            let end = tb.measure_time();
            let dur = tb.measure_duration(start, end);
            tb.add_task_time(slot, label, worker, dur);
        }
    }

    /// Call `f` with the `TimeBufferManager` if timing is enabled; no-op otherwise.
    #[inline]
    pub fn with_timing<F: FnOnce(&TimeBufferManager)>(&self, f: F) {
        if let Some(tb) = &self.time_buffer {
            f(tb);
        }
    }
}

// Shared data across all Τομί threads - immutable or internally synchronized
pub struct SharedData {
    /// Immutable graph definition — kept flat for unchanged access pattern.
    pub graph: Graph,
    pub graph_cache: GraphCache,
    pub config: RuntimeConfig,
    pub slot_data: SlotData,
    /// Runtime shutdown signal — set by the main thread on max_runtime or stream completion.
    /// Read by resolution threads to exit their loops. Always present (not network-specific).
    pub shutdown_flag: Arc<AtomicBool>,
    #[cfg(feature = "network")]
    pub net: NetworkInfra,
    pub exec: ExecCtx,
    pub telemetry: Telemetry,
}

/// Borrowed view of the sub-structs needed by dependency-resolution functions.
/// Constructed cheaply on the stack via [`SharedData::resolve_ctx`].
pub(super) struct ResolveCtx<'a> {
    pub slots: &'a SlotData,
    pub exec: &'a ExecCtx,
    pub cache: &'a GraphCache,
    pub cfg: &'a RuntimeConfig,
}

/// Borrowed view of the sub-structs needed by task-scheduling functions.
/// Constructed cheaply on the stack via [`SharedData::sched_ctx`].
pub(super) struct SchedCtx<'a> {
    pub exec: &'a ExecCtx,
    pub telemetry: &'a Telemetry,
    pub cfg: &'a RuntimeConfig,
    pub slots: &'a SlotData,
    pub cache: &'a GraphCache,
}

impl SharedData {
    #[inline]
    pub(super) fn resolve_ctx(&self) -> ResolveCtx<'_> {
        ResolveCtx {
            slots: &self.slot_data,
            exec: &self.exec,
            cache: &self.graph_cache,
            cfg: &self.config,
        }
    }

    #[inline]
    pub(super) fn sched_ctx(&self) -> SchedCtx<'_> {
        SchedCtx {
            exec: &self.exec,
            telemetry: &self.telemetry,
            cfg: &self.config,
            slots: &self.slot_data,
            cache: &self.graph_cache,
        }
    }
}
