mod arg_resolution;
mod batch_resolution;
mod consts;
mod init;
#[cfg(feature = "network")]
mod network_init;
mod node_cache;
mod ordering;
#[cfg(feature = "network")]
mod packet_processing;
mod reporting;
mod resolution_loop;
mod scheduling;
mod shared_data;
mod slot_lifecycle;
mod slot_management;
mod successor;
mod task_execution;
mod thread_locals;
mod threading;

// build_node_cache and build_predecessor_tables are re-exported pub(crate) so that
// graph_gen::GraphSpec::compile() can call them without going through the runtime builder.
use init::build_slot_counters;
pub(crate) use init::{build_node_cache, build_predecessor_tables};
#[cfg(feature = "network")]
use network_init::prepare_network_infrastructure;
pub(crate) use node_cache::NodeCacheEntry;
use parking_lot::RwLock;
#[cfg(feature = "network")]
pub(crate) use shared_data::NetworkInfra;
// SharedData is crate-internal; only BatchConfig, SpinWaitConfig, and RuntimeConfig are public.
pub(crate) use shared_data::SharedData;
pub use shared_data::{BatchConfig, RuntimeConfig, SpinWaitConfig};
pub(crate) use shared_data::{
    BatchQueueRx, BatchQueueTx, ExecCtx, GraphCache, SlotData, SlotState, Telemetry,
};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

use crate::async_recorder::AsyncRecorder;
use crate::graph_gen::GraphCompiled;
use crate::resolution_state::{MultiThreadedState, ResolutionState};
use crate::scheduler::SchedulerImpl;
use crate::time_buffer::TimeBufferManager;
use crossbeam_channel::bounded as cb_bounded;

pub const RUN_SLEEP: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// TomiiRtBuilder — fluent builder for the Τομί runtime
// ---------------------------------------------------------------------------

/// Builder for [`TomiiRt`]. Holds all configuration with sensible defaults so
/// callers only need to set the parameters that differ from the defaults.
///
/// # Example
/// ```ignore
/// let compiled = spec.compile(&scheduler);
/// let synrt = TomiiRtBuilder::new(compiled, scheduler)
///     .slots(4)
///     .max_streams(100)
///     .max_runtime(Some(60))
///     .timing_enabled(true)
///     .build();
/// ```
pub struct TomiiRtBuilder {
    compiled: GraphCompiled,
    scheduler: SchedulerImpl,
    config: RuntimeConfig,
    // Build-only fields — not part of RuntimeConfig:
    batch_queue_capacity: usize,
    socket_recv_buf_bytes: usize,
    timing_enabled: bool,
    base_instant: Instant,
    async_recorder: Option<Arc<AsyncRecorder>>,
    use_rdtsc: bool,
    record: bool,
}

impl TomiiRtBuilder {
    /// Create a builder from a compiled graph IR and scheduler.
    ///
    /// Obtain the `compiled` argument via [`crate::graph_gen::GraphSpec::compile`].
    pub fn new(compiled: GraphCompiled, scheduler: SchedulerImpl) -> Self {
        Self {
            compiled,
            scheduler,
            config: RuntimeConfig::default(),
            batch_queue_capacity: 65536,
            socket_recv_buf_bytes: 16_777_216,
            timing_enabled: false,
            base_instant: Instant::now(),
            async_recorder: None,
            use_rdtsc: false,
            record: false,
        }
    }

    /// Construct a builder from a pre-built [`RuntimeConfig`].
    /// Useful for embedders loading config from TOML/JSON.
    pub fn with_config(
        compiled: GraphCompiled,
        scheduler: SchedulerImpl,
        config: RuntimeConfig,
    ) -> Self {
        Self {
            compiled,
            scheduler,
            config,
            batch_queue_capacity: 65536,
            socket_recv_buf_bytes: 16_777_216,
            timing_enabled: false,
            base_instant: Instant::now(),
            async_recorder: None,
            use_rdtsc: false,
            record: false,
        }
    }

    pub fn slots(mut self, n: usize) -> Self {
        self.config.slots = n;
        self
    }
    pub fn max_streams(mut self, n: usize) -> Self {
        self.config.max_streams = n;
        self
    }
    pub fn max_runtime(mut self, secs: Option<u64>) -> Self {
        self.config.max_runtime = secs;
        self
    }
    pub fn use_rdtsc(mut self, v: bool) -> Self {
        self.use_rdtsc = v;
        self
    }
    pub fn record(mut self, v: bool) -> Self {
        self.record = v;
        self
    }
    pub fn record_stream(mut self, v: Option<usize>) -> Self {
        self.config.record_stream = v;
        self
    }
    pub fn timing_enabled(mut self, v: bool) -> Self {
        self.timing_enabled = v;
        self
    }
    /// Override the base instant (useful when the scheduler was created with the same instant).
    pub fn base_instant(mut self, t: Instant) -> Self {
        self.base_instant = t;
        self
    }
    pub fn slot_priority_enabled(mut self, v: bool) -> Self {
        self.config.slot_priority_enabled = v;
        self
    }
    /// Attach a pre-created [`AsyncRecorder`] shared with the scheduler.
    pub fn async_recorder(mut self, r: Option<Arc<AsyncRecorder>>) -> Self {
        self.async_recorder = r;
        self
    }
    pub fn coalesce_barriers(mut self, v: bool) -> Self {
        self.config.coalesce_barriers = v;
        self
    }
    pub fn inline_continuation(mut self, v: bool) -> Self {
        self.config.inline_continuation = v;
        self
    }
    pub fn batch_queue_capacity(mut self, n: usize) -> Self {
        self.batch_queue_capacity = n;
        self
    }
    pub fn socket_recv_buf_bytes(mut self, n: usize) -> Self {
        self.socket_recv_buf_bytes = n;
        self
    }
    pub fn recv_pool_size(mut self, n: usize) -> Self {
        self.config.recv_pool_size = n;
        self
    }
    /// Set all worker spin-wait parameters at once.
    pub fn spin_wait(mut self, cfg: SpinWaitConfig) -> Self {
        self.config.spin_wait = cfg;
        self
    }
    /// Set all batch-processing parameters at once.
    pub fn batch(mut self, cfg: BatchConfig) -> Self {
        self.config.batch = cfg;
        self
    }

    /// Construct the runtime. This is cheap — no threads are spawned until [`TomiiRt::run`].
    ///
    /// # Errors
    ///
    /// Returns [`crate::BuildError::InvalidConfig`] if any configuration constraint is violated:
    /// - `slots` (clamped to `max_streams`) must be in the range `[1, 64]`
    /// - `max_streams` must be `>= 1`
    /// - `batch_queue_capacity` must be `> 0`
    pub fn build(mut self) -> Result<TomiiRt, crate::BuildError> {
        // Clamp slots to max_streams and write back into config so the
        // assembled RuntimeConfig carries the resolved value.
        self.config.slots = std::cmp::min(self.config.slots, self.config.max_streams);
        let slots = self.config.slots;

        if slots < 1 {
            return Err(crate::BuildError::InvalidConfig(
                "slots must be >= 1 (got 0)".to_string(),
            ));
        }
        if slots > consts::MAX_SLOTS {
            return Err(crate::BuildError::InvalidConfig(format!(
                "Τομί supports at most {} concurrent slots (got {slots}); \
                 this limit is enforced by the u64 completion bitmaps",
                consts::MAX_SLOTS
            )));
        }
        if self.config.max_streams < 1 {
            return Err(crate::BuildError::InvalidConfig(
                "max_streams must be >= 1 (got 0)".to_string(),
            ));
        }
        if self.batch_queue_capacity == 0 {
            return Err(crate::BuildError::InvalidConfig(
                "batch_queue_capacity must be > 0 (got 0)".to_string(),
            ));
        }

        // --- Destructure compiled graph IR ---
        let GraphCompiled {
            graph,
            node_cache,
            pred_index_filter,
            pred_group_by,
            pred_succ_1to1_offset,
            total_tasks,
            total_cond_tasks,
            init_objects,
            dependency_count_vec,
            max_factor,
            num_nodes,
        } = self.compiled;

        // --- Slot counters & condition tracking (slot-count-dependent) ---
        let (slot_pending_tasks, slot_pending_cond_tasks, cond_instances_to_spawn) =
            build_slot_counters(slots, &node_cache);

        // --- Thread pool parameters from scheduler ---
        self.config.system_threads = self.scheduler.system_threads();
        self.config.core_offset = self.scheduler.core_offset();
        self.config.receiver_threads = self.scheduler.receiver_threads();
        self.config.receiver_core_offset = self.scheduler.receiver_core_offset();
        self.config.workers = self.scheduler.workers();
        // single_slot_mode is derived at build time
        self.config.single_slot_mode = slots == 1;

        let system_threads = self.config.system_threads;
        let receiver_threads = self.config.receiver_threads;

        // --- Telemetry ---
        let time_buffer = if self.timing_enabled {
            Some(Arc::new(TimeBufferManager::new_async(
                slots + system_threads,
                system_threads,
                self.use_rdtsc,
            )))
        } else {
            None
        };

        let async_recorder = if self.record {
            self.async_recorder.or_else(|| {
                Some(Arc::new(AsyncRecorder::new(
                    slots + system_threads + receiver_threads,
                    100,
                )))
            })
        } else {
            None
        };

        // --- Resolution state ---
        let resolution_state: Arc<dyn ResolutionState> = Arc::new(MultiThreadedState::new(
            num_nodes,
            slots,
            max_factor,
            dependency_count_vec,
            &graph.nodes,
        ));
        tracing::debug!(
            "\nResolutionState initialized:\n{}\n",
            resolution_state.debug_info()
        );

        // --- Batch queue ---
        let (batch_queue_tx, batch_queue_rx): (BatchQueueTx, BatchQueueRx) =
            cb_bounded(self.batch_queue_capacity);

        // --- Network infrastructure ---
        #[cfg(feature = "network")]
        let (
            receiver_sockets,
            packet_sender,
            packet_receiver,
            packet_drop_counters,
            buffer_return_senders,
            buffer_return_receivers,
        ) = prepare_network_infrastructure(
            &graph,
            self.socket_recv_buf_bytes,
            self.config.recv_pool_size,
        );

        // --- Assemble SharedData ---
        let node_results = Arc::new(crate::buffers::LockFreeResultMap::new(&graph.nodes, slots));
        let slot_buffers = Arc::new(RwLock::new(vec![Vec::new(); slots]));

        #[cfg(feature = "network")]
        let max_streams_for_frame_drop = self.config.max_streams;

        let shared = Arc::new(SharedData {
            graph,
            graph_cache: GraphCache {
                node_cache,
                pred_index_filter: Arc::new(pred_index_filter),
                pred_group_by: Arc::new(pred_group_by),
                pred_succ_1to1_offset: Arc::new(pred_succ_1to1_offset),
                total_tasks,
                total_cond_tasks,
                init_objects,
            },
            config: self.config,
            slot_data: SlotData {
                generation: Arc::new((0..slots).map(|_| AtomicU64::new(0)).collect()),
                pending_tasks: Arc::new(slot_pending_tasks),
                pending_cond_tasks: Arc::new(slot_pending_cond_tasks),
                processing_count: Arc::new((0..slots).map(|_| AtomicUsize::new(0)).collect()),
                needs_check: Arc::new((0..slots).map(|_| AtomicBool::new(false)).collect()),
                packet_counters: Arc::new((0..slots).map(|_| AtomicUsize::new(0)).collect()),
                packet_complete: Arc::new((0..slots).map(|_| AtomicBool::new(false)).collect()),
                stream_id: Arc::new((0..slots).map(|_| AtomicUsize::new(usize::MAX)).collect()),
                active_bitmap: Arc::new(AtomicU64::new(0)),
                cond_instances_to_spawn: Arc::new(cond_instances_to_spawn),
                states: Arc::new(RwLock::new(vec![SlotState::Inactive; slots])),
                running_streams: Arc::new(RwLock::new(Vec::new())),
                buffers: slot_buffers,
                last_assigned: Arc::new(AtomicUsize::new(0)),
            },
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            #[cfg(feature = "network")]
            net: NetworkInfra {
                receive_finished: Arc::new(AtomicBool::new(false)),
                packet_sender,
                packet_receiver,
                receiver_sockets,
                packet_drop_counters,
                buffer_return_senders,
                buffer_return_receivers,
                streams_receive_counter: Arc::new(AtomicUsize::new(0)),
                dropped_streams: Arc::new(AtomicUsize::new(0)),
                frame_dropped: Arc::new(
                    (0..max_streams_for_frame_drop + slots)
                        .map(|_| AtomicBool::new(false))
                        .collect(),
                ),
            },
            exec: ExecCtx {
                scheduler: Arc::new(self.scheduler),
                batch_queue_tx,
                batch_queue_rx,
                resolution_state,
                node_results,
                initial_prep_done: Arc::new(AtomicUsize::new(0)),
            },
            telemetry: Telemetry {
                time_buffer,
                async_recorder,
                base_instant: Arc::new(self.base_instant),
                job_counter: Arc::new(AtomicUsize::new(0)),
                stream_complete_counter: Arc::new(AtomicUsize::new(0)),
            },
        });

        Ok(TomiiRt { shared })
    }
}

// ---------------------------------------------------------------------------
// TomiiRt — the main runtime handle
// ---------------------------------------------------------------------------

/// Main Τομί runtime handle. Construct via [`TomiiRtBuilder`].
pub struct TomiiRt {
    shared: Arc<SharedData>,
}

impl TomiiRt {
    pub fn base_instant(&self) -> Instant {
        *self.shared.telemetry.base_instant
    }

    /// Spawn all threads and run the graph to completion (or until max_runtime).
    ///
    /// # Errors
    ///
    /// Returns [`crate::RuntimeError::SpawnFailed`] if any worker or receiver thread fails to
    /// spawn. A panicking child thread propagates via the [`std::thread::JoinHandle::join`]
    /// unwrap and is not converted to an error — that indicates a bug in the runtime, not a
    /// recoverable user error.
    pub fn run(&mut self) -> Result<(), crate::RuntimeError> {
        // Start timing for system thread slots
        for thread_id in 0..self.shared.config.system_threads {
            let system_slot = self.shared.config.slots + thread_id;
            self.shared
                .telemetry
                .with_timing(|tb| tb.start_slot_processing(system_slot));
        }

        #[cfg(feature = "network")]
        let receiver_handles = self.spawn_receiver_threads()?;
        let resolution_handles = self.spawn_resolution_threads()?;

        // Wait loop: sleep until max_runtime exceeded or all streams complete
        let start_time = Instant::now();
        tracing::debug!("Max runtime check started");
        if let Some(max_runtime) = self.shared.config.max_runtime {
            sleep(RUN_SLEEP);
            let mut finish = false;
            loop {
                let completed_streams = self
                    .shared
                    .telemetry
                    .stream_complete_counter
                    .load(Ordering::Acquire);
                let completed = completed_streams == self.shared.config.max_streams;

                if start_time.elapsed().as_secs() > max_runtime {
                    tracing::info!("Max runtime reached, exiting");
                    finish = true;
                } else if completed {
                    tracing::info!("All streams completed, exiting");
                    finish = true;
                }

                if finish {
                    self.shared.shutdown_flag.store(true, Ordering::SeqCst);
                    tracing::info!("Shutdown flag set - signaling resolution threads to exit");
                    tracing::debug!("Processing possible post-nodes");
                    self.schedule_post_nodes();
                    break;
                }
                sleep(RUN_SLEEP);
            }
        }

        for handle in resolution_handles {
            handle.join().unwrap();
        }

        #[cfg(feature = "network")]
        self.shutdown_receiver_threads(receiver_handles);

        // Finish timing for system thread slots
        for thread_id in 0..self.shared.config.system_threads {
            let system_slot = self.shared.config.slots + thread_id;
            self.shared.telemetry.with_timing(|tb| {
                let _ = tb.finish_slot_processing(system_slot);
            });
        }

        Ok(())
    }
}
