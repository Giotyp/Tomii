mod arg_resolution;
mod batch_resolution;
mod init;
#[cfg(feature = "network")]
mod network_init;
mod node_cache;
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

// SharedData is pub because network.rs (a non-runtime module) takes &Arc<SharedData>
// in the receiver loop signatures. All other runtime internals are pub(crate).
//
// build_node_cache and build_predecessor_tables are re-exported pub(crate) so that
// graph_gen::GraphSpec::compile() can call them without going through the runtime builder.
pub(crate) use init::{build_node_cache, build_predecessor_tables};
use init::build_slot_counters;
pub(crate) use node_cache::NodeCacheEntry;
#[cfg(feature = "network")]
use network_init::prepare_network_infrastructure;
use parking_lot::RwLock;
#[cfg(feature = "network")]
pub(crate) use shared_data::NetworkInfra;
pub use shared_data::SharedData;
pub use shared_data::{BatchConfig, SpinWaitConfig};
pub(crate) use shared_data::{
    BatchQueueRx, BatchQueueTx, ExecCtx, GraphCache, RuntimeConfig, SlotData, SlotState, Telemetry,
};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

use crate::async_recorder::AsyncRecorder;
use crate::debug::print_debug;
use crate::graph_gen::GraphCompiled;
use crate::resolution_state::{MultiThreadedState, ResolutionState};
use crate::scheduler::SchedulerImpl;
use crate::time_buffer::TimeBufferManager;
use crossbeam_channel::bounded as cb_bounded;

pub const RUN_SLEEP: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// SynRtBuilder — fluent builder for the SynStream runtime
// ---------------------------------------------------------------------------

/// Builder for [`SynRt`]. Holds all configuration with sensible defaults so
/// callers only need to set the parameters that differ from the defaults.
///
/// # Example
/// ```ignore
/// let compiled = spec.compile(&scheduler);
/// let synrt = SynRtBuilder::new(compiled, scheduler)
///     .slots(4)
///     .max_streams(100)
///     .max_runtime(60)
///     .timing_enabled(true)
///     .build();
/// ```
pub struct SynRtBuilder {
    compiled: GraphCompiled,
    scheduler: SchedulerImpl,
    slots: usize,
    max_streams: usize,
    max_runtime: Option<u64>,
    use_rdtsc: bool,
    record: bool,
    record_stream: Option<usize>,
    timing_enabled: bool,
    base_instant: Instant,
    slot_priority_enabled: bool,
    async_recorder: Option<Arc<AsyncRecorder>>,
    coalesce_barriers: bool,
    inline_continuation: bool,
    batch_queue_capacity: usize,
    socket_recv_buf_bytes: usize,
    recv_pool_size: usize,
    spin_wait: SpinWaitConfig,
    batch: BatchConfig,
}

impl SynRtBuilder {
    /// Create a builder from a compiled graph IR and scheduler.
    ///
    /// Obtain the `compiled` argument via [`crate::graph_gen::GraphSpec::compile`].
    pub fn new(compiled: GraphCompiled, scheduler: SchedulerImpl) -> Self {
        Self {
            compiled,
            scheduler,
            slots: 1,
            max_streams: 1,
            max_runtime: None,
            use_rdtsc: false,
            record: false,
            record_stream: None,
            timing_enabled: false,
            base_instant: Instant::now(),
            slot_priority_enabled: false,
            async_recorder: None,
            coalesce_barriers: false,
            inline_continuation: false,
            batch_queue_capacity: 65536,
            socket_recv_buf_bytes: 16_777_216,
            recv_pool_size: 1024,
            spin_wait: SpinWaitConfig::default(),
            batch: BatchConfig::default(),
        }
    }

    pub fn slots(mut self, n: usize) -> Self {
        self.slots = n;
        self
    }
    pub fn max_streams(mut self, n: usize) -> Self {
        self.max_streams = n;
        self
    }
    pub fn max_runtime(mut self, secs: Option<u64>) -> Self {
        self.max_runtime = secs;
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
        self.record_stream = v;
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
        self.slot_priority_enabled = v;
        self
    }
    /// Attach a pre-created [`AsyncRecorder`] shared with the scheduler.
    pub fn async_recorder(mut self, r: Option<Arc<AsyncRecorder>>) -> Self {
        self.async_recorder = r;
        self
    }
    pub fn coalesce_barriers(mut self, v: bool) -> Self {
        self.coalesce_barriers = v;
        self
    }
    pub fn inline_continuation(mut self, v: bool) -> Self {
        self.inline_continuation = v;
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
        self.recv_pool_size = n;
        self
    }
    /// Set all worker spin-wait parameters at once.
    pub fn spin_wait(mut self, cfg: SpinWaitConfig) -> Self {
        self.spin_wait = cfg;
        self
    }
    /// Set all batch-processing parameters at once.
    pub fn batch(mut self, cfg: BatchConfig) -> Self {
        self.batch = cfg;
        self
    }

    /// Construct the runtime. This is cheap — no threads are spawned until [`SynRt::run`].
    pub fn build(self) -> SynRt {
        let slots = std::cmp::min(self.slots, self.max_streams);

        assert!(
            slots <= 64,
            "SynStream supports at most 64 concurrent slots (got {}); this limit is enforced by the u64 completion bitmaps",
            slots
        );

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
        let system_threads = self.scheduler.system_threads();
        let core_offset = self.scheduler.core_offset();
        let receiver_threads = self.scheduler.receiver_threads();
        let receiver_core_offset = self.scheduler.receiver_core_offset();
        let workers = self.scheduler.workers();

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
        let resolution_state: Arc<dyn ResolutionState> = {
            println!("Using multi-threaded resolution state (lock-free atomics)");
            Arc::new(MultiThreadedState::new(
                num_nodes,
                slots,
                max_factor,
                dependency_count_vec,
                &graph.nodes,
            ))
        };
        println!(
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
            self.recv_pool_size,
        );

        // --- Assemble SharedData ---
        let node_results = Arc::new(crate::buffers::LockFreeResultMap::new(&graph.nodes, slots));
        let slot_buffers = Arc::new(RwLock::new(vec![Vec::new(); slots]));

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
            config: RuntimeConfig {
                slots,
                max_streams: self.max_streams,
                max_runtime: self.max_runtime,
                system_threads,
                receiver_threads,
                workers,
                core_offset,
                receiver_core_offset,
                slot_priority_enabled: self.slot_priority_enabled,
                coalesce_barriers: self.coalesce_barriers,
                inline_continuation: self.inline_continuation,
                single_slot_mode: slots == 1,
                record_stream: self.record_stream,
                recv_pool_size: self.recv_pool_size,
                spin_wait: self.spin_wait,
                batch: self.batch,
            },
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
                    (0..self.max_streams + slots)
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

        SynRt { shared }
    }
}

// ---------------------------------------------------------------------------
// SynRt — the main runtime handle
// ---------------------------------------------------------------------------

/// Main SynStream runtime handle. Construct via [`SynRtBuilder`].
pub struct SynRt {
    shared: Arc<SharedData>,
}

impl SynRt {
    pub fn base_instant(&self) -> Instant {
        *self.shared.telemetry.base_instant
    }

    /// Spawn all threads and run the graph to completion (or until max_runtime).
    pub fn run(&mut self) {
        // Start timing for system thread slots
        for thread_id in 0..self.shared.config.system_threads {
            let system_slot = self.shared.config.slots + thread_id;
            self.shared
                .telemetry
                .with_timing(|tb| tb.start_slot_processing(system_slot));
        }

        #[cfg(feature = "network")]
        let receiver_handles = self.spawn_receiver_threads();
        let resolution_handles = self.spawn_resolution_threads();

        // Wait loop: sleep until max_runtime exceeded or all streams complete
        let start_time = Instant::now();
        print_debug(|| "Max runtime check started".to_string());
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
                    println!("Max runtime reached exiting...");
                    finish = true;
                } else if completed {
                    println!("No pending jobs and all jobs completed, exiting...");
                    finish = true;
                }

                if finish {
                    self.shared.shutdown_flag.store(true, Ordering::SeqCst);
                    println!("Shutdown flag set - signaling resolution threads to exit");
                    println!("Processing possible post-nodes...");
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
    }
}
