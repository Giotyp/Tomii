mod arg_resolution;
mod network_init;
mod node_cache;
mod reporting;
mod resolution_loop;
mod scheduling;
mod shared_data;
mod slot_lifecycle;
mod slot_management;
mod successor;
mod task_execution;

// SharedData is pub because network.rs (a non-runtime module) takes &Arc<SharedData>
// in the receiver loop signatures. All other runtime internals are pub(crate).
pub use shared_data::SharedData;
pub(crate) use shared_data::{
    BatchQueueRx, BatchQueueTx, ExecCtx, GraphCache, NetworkInfra, RuntimeConfig,
    SlotData, SlotState, Telemetry,
};
pub(crate) use node_cache::NodeCacheEntry;

use core_affinity;
use node_cache::node_cache_entry;
use network_init::prepare_network_infrastructure;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, sleep, spawn, JoinHandle};
use std::time::{Duration, Instant};

use crate::async_recorder::{set_worker_recorder, AsyncRecorder};
use crate::debug::print_debug;
use crate::graph::*;
use crate::network::multi_socket_receiver_loop;
use crate::resolution_state::{MultiThreadedState, ResolutionState};
use crate::scheduler::SchedulerImpl;
use crate::time_buffer::TimeBufferManager;
use crate::IdType;
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
/// let synrt = SynRtBuilder::new(graph, scheduler)
///     .slots(4)
///     .max_streams(100)
///     .max_runtime(60)
///     .timing_enabled(true)
///     .build();
/// ```
pub struct SynRtBuilder {
    graph: Graph,
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
    target_batch_size: usize,
    batch_timeout_us: u64,
    coalesce_barriers: bool,
    inline_continuation: bool,
    batch_queue_capacity: usize,
    spin_iterations: u32,
    sched_flush_threshold: usize,
    socket_recv_buf_bytes: usize,
    recv_pool_size: usize,
    spin_wait_spin_iters: u32,
    spin_wait_yield_iters: u32,
    spin_wait_park_ns: u64,
}

impl SynRtBuilder {
    /// Create a builder with required fields and defaults for everything else.
    pub fn new(graph: Graph, scheduler: SchedulerImpl) -> Self {
        Self {
            graph,
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
            target_batch_size: 1,
            batch_timeout_us: 10,
            coalesce_barriers: false,
            inline_continuation: false,
            batch_queue_capacity: 65536,
            spin_iterations: 32,
            sched_flush_threshold: 32,
            socket_recv_buf_bytes: 16_777_216,
            recv_pool_size: 1024,
            spin_wait_spin_iters: 64,
            spin_wait_yield_iters: 256,
            spin_wait_park_ns: 100,
        }
    }

    pub fn slots(mut self, n: usize) -> Self { self.slots = n; self }
    pub fn max_streams(mut self, n: usize) -> Self { self.max_streams = n; self }
    pub fn max_runtime(mut self, secs: Option<u64>) -> Self { self.max_runtime = secs; self }
    pub fn use_rdtsc(mut self, v: bool) -> Self { self.use_rdtsc = v; self }
    pub fn record(mut self, v: bool) -> Self { self.record = v; self }
    pub fn record_stream(mut self, v: Option<usize>) -> Self { self.record_stream = v; self }
    pub fn timing_enabled(mut self, v: bool) -> Self { self.timing_enabled = v; self }
    /// Override the base instant (useful when the scheduler was created with the same instant).
    pub fn base_instant(mut self, t: Instant) -> Self { self.base_instant = t; self }
    pub fn slot_priority_enabled(mut self, v: bool) -> Self { self.slot_priority_enabled = v; self }
    /// Attach a pre-created [`AsyncRecorder`] shared with the scheduler.
    pub fn async_recorder(mut self, r: Option<Arc<AsyncRecorder>>) -> Self { self.async_recorder = r; self }
    pub fn target_batch_size(mut self, n: usize) -> Self { self.target_batch_size = n; self }
    pub fn batch_timeout_us(mut self, us: u64) -> Self { self.batch_timeout_us = us; self }
    pub fn coalesce_barriers(mut self, v: bool) -> Self { self.coalesce_barriers = v; self }
    pub fn inline_continuation(mut self, v: bool) -> Self { self.inline_continuation = v; self }
    pub fn batch_queue_capacity(mut self, n: usize) -> Self { self.batch_queue_capacity = n; self }
    pub fn spin_iterations(mut self, n: u32) -> Self { self.spin_iterations = n; self }
    pub fn sched_flush_threshold(mut self, n: usize) -> Self { self.sched_flush_threshold = n; self }
    pub fn socket_recv_buf_bytes(mut self, n: usize) -> Self { self.socket_recv_buf_bytes = n; self }
    pub fn recv_pool_size(mut self, n: usize) -> Self { self.recv_pool_size = n; self }
    pub fn spin_wait_spin_iters(mut self, n: u32) -> Self { self.spin_wait_spin_iters = n; self }
    pub fn spin_wait_yield_iters(mut self, n: u32) -> Self { self.spin_wait_yield_iters = n; self }
    pub fn spin_wait_park_ns(mut self, ns: u64) -> Self { self.spin_wait_park_ns = ns; self }

    /// Construct the runtime. This is cheap — no threads are spawned until [`SynRt::run`].
    pub fn build(self) -> SynRt {
        let slots = std::cmp::min(self.slots, self.max_streams);
        let app_graph = &self.graph;

        // --- Node cache ---
        let node_cache = build_node_cache(app_graph, &self.scheduler);

        // --- Predecessor routing tables ---
        let (pred_index_filter, pred_group_by, pred_succ_1to1_offset) =
            build_predecessor_tables(app_graph);

        // --- Slot counters & condition tracking ---
        let (slot_pending_tasks, slot_pending_cond_tasks, cond_instances_to_spawn) =
            build_slot_counters(slots, &node_cache);

        let total_tasks: usize = node_cache
            .iter()
            .filter(|nc| !nc.is_initial && !nc.is_condition)
            .map(|nc| nc.factor)
            .sum();
        let total_cond_tasks: usize = node_cache
            .iter()
            .filter(|nc| nc.is_condition)
            .map(|nc| nc.factor)
            .sum();

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
        let num_nodes = app_graph.nodes.len();
        let max_factor = node_cache.iter().map(|n| n.factor).max().unwrap_or(1);
        let dependency_count_vec: Vec<usize> = app_graph.dependency_count_vec();

        let resolution_state: Arc<dyn ResolutionState> = {
            println!("Using multi-threaded resolution state (lock-free atomics)");
            Arc::new(MultiThreadedState::new(
                num_nodes,
                slots,
                max_factor,
                dependency_count_vec.clone(),
                &app_graph.nodes,
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
        let (
            receiver_sockets,
            packet_sender,
            packet_receiver,
            packet_drop_counters,
            buffer_return_senders,
            buffer_return_receivers,
        ) = prepare_network_infrastructure(app_graph, self.socket_recv_buf_bytes, self.recv_pool_size);

        // --- Assemble SharedData ---
        // node_results must be created before moving self.graph into SharedData.
        let node_results = Arc::new(crate::buffers::LockFreeResultMap::new(
            &self.graph.nodes,
            slots,
        ));
        let slot_buffers = Arc::new(RwLock::new(vec![Vec::new(); slots]));

        let shared = Arc::new(SharedData {
            graph: self.graph,
            graph_cache: GraphCache {
                node_cache,
                pred_index_filter: Arc::new(pred_index_filter),
                pred_group_by: Arc::new(pred_group_by),
                pred_succ_1to1_offset: Arc::new(pred_succ_1to1_offset),
                total_tasks,
                total_cond_tasks,
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
                target_batch_size: self.target_batch_size,
                batch_timeout_us: self.batch_timeout_us,
                spin_iterations: self.spin_iterations,
                sched_flush_threshold: self.sched_flush_threshold,
                spin_wait_spin_iters: self.spin_wait_spin_iters,
                spin_wait_yield_iters: self.spin_wait_yield_iters,
                spin_wait_park_ns: self.spin_wait_park_ns,
                recv_pool_size: self.recv_pool_size,
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
            net: NetworkInfra {
                shutdown_flag: Arc::new(AtomicBool::new(false)),
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
            self.shared.telemetry.with_timing(|tb| tb.start_slot_processing(system_slot));
        }

        let receiver_handles = self.spawn_receiver_threads();
        let resolution_handles = self.spawn_resolution_threads();

        // Wait loop: sleep until max_runtime exceeded or all streams complete
        let start_time = Instant::now();
        print_debug(|| "Max runtime check started".to_string());
        if let Some(max_runtime) = self.shared.config.max_runtime {
            sleep(RUN_SLEEP);
            let mut finish = false;
            loop {
                let completed_streams = self.shared.telemetry.stream_complete_counter
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
                    self.shared.net.shutdown_flag.store(true, Ordering::SeqCst);
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

        self.shutdown_receiver_threads(receiver_handles);

        // Finish timing for system thread slots
        for thread_id in 0..self.shared.config.system_threads {
            let system_slot = self.shared.config.slots + thread_id;
            self.shared.telemetry.with_timing(|tb| { let _ = tb.finish_slot_processing(system_slot); });
        }
    }

    /// Spawn dedicated network receiver threads (one per socket, or round-robin if fewer threads).
    fn spawn_receiver_threads(&self) -> Vec<JoinHandle<()>> {
        let Some(ref network_config) = self.shared.graph.network_config() else {
            println!("No network_config present - skipping network receiver setup");
            return Vec::new();
        };

        let num_sockets = network_config.num_sockets;
        let buffer_depth = network_config.buffer_depth;

        println!("\n=== Initializing Network Receiver Infrastructure ===");
        println!("Number of sockets: {}", num_sockets);
        println!("Buffer depth: {} packets per socket", buffer_depth);

        assert_eq!(
            self.shared.net.receiver_sockets.len(),
            num_sockets,
            "Network config expected {} sockets but {} were allocated",
            num_sockets,
            self.shared.net.receiver_sockets.len()
        );

        let receiver_threads = self.shared.config.receiver_threads;
        let receiver_offset = self.shared.config.receiver_core_offset;
        let dylib_path =
            std::env::var("DYLIB_PATH").unwrap_or_else(|_| "./libmimolib.so".to_string());

        println!(
            "\nSpawning {} receiver threads starting at core {}",
            receiver_threads, receiver_offset
        );
        println!("Using dylib: {} for frame ID extraction", dylib_path);

        let mut handles = Vec::with_capacity(receiver_threads);

        // Extract shared receiver context once — avoid passing all of SharedData into network.rs.
        let packet_length = self.shared.graph.network_config()
            .expect("Network config must be present for receiver threads")
            .packet_length;
        let recv_pool_size = self.shared.config.recv_pool_size;
        let shutdown = Arc::clone(&self.shared.net.shutdown_flag);
        let tx = self.shared.net.packet_sender.clone();
        let sockets = Arc::clone(&self.shared.net.receiver_sockets);
        let drop_counters = Arc::clone(&self.shared.net.packet_drop_counters);

        if receiver_threads >= num_sockets {
            println!("Using 1:1 thread-to-socket mapping (optimal)");
            for socket_id in 0..num_sockets {
                let core_id = receiver_offset + socket_id;
                let return_rx = self.shared.net.buffer_return_receivers[socket_id]
                    .lock()
                    .take()
                    .expect("buffer_return_receivers already taken");
                let (pl, rps) = (packet_length, recv_pool_size);
                let (sd, tx2, socks, drops) = (
                    Arc::clone(&shutdown), tx.clone(),
                    Arc::clone(&sockets), Arc::clone(&drop_counters),
                );

                let handle = thread::Builder::new()
                    .name(format!("rx-{}", socket_id))
                    .spawn(move || {
                        multi_socket_receiver_loop(
                            pl, rps, sd, tx2, socks, drops,
                            socket_id, socket_id..socket_id + 1, core_id,
                            vec![return_rx],
                        );
                    })
                    .expect("Failed to spawn receiver thread");
                handles.push(handle);
                println!(
                    "  Receiver thread {} (socket {}) spawned on core {}",
                    socket_id, socket_id, core_id
                );
            }
        } else {
            println!(
                "WARNING: receiver_threads ({}) < num_sockets ({}). Using round-robin polling.",
                receiver_threads, num_sockets
            );
            let sockets_per_thread = (num_sockets + receiver_threads - 1) / receiver_threads;

            for thread_id in 0..receiver_threads {
                let start_socket = thread_id * sockets_per_thread;
                let end_socket = std::cmp::min(start_socket + sockets_per_thread, num_sockets);
                let socket_range = start_socket..end_socket;
                let socket_range_display = socket_range.clone();

                let return_rxs: Vec<flume::Receiver<Vec<u8>>> = (start_socket..end_socket)
                    .map(|sid| {
                        self.shared.net.buffer_return_receivers[sid]
                            .lock()
                            .take()
                            .expect("buffer_return_receivers already taken")
                    })
                    .collect();

                let core_id = receiver_offset + thread_id;
                let (pl, rps) = (packet_length, recv_pool_size);
                let (sd, tx2, socks, drops) = (
                    Arc::clone(&shutdown), tx.clone(),
                    Arc::clone(&sockets), Arc::clone(&drop_counters),
                );

                let handle = thread::Builder::new()
                    .name(format!("rx-multi-{}", thread_id))
                    .spawn(move || {
                        multi_socket_receiver_loop(
                            pl, rps, sd, tx2, socks, drops,
                            thread_id, socket_range, core_id, return_rxs,
                        );
                    })
                    .expect("Failed to spawn receiver thread");
                handles.push(handle);
                println!(
                    "  Multi-socket receiver {} polling sockets {:?} on core {}",
                    thread_id, socket_range_display, core_id
                );
            }
        }

        println!("=== Network Receiver Infrastructure Ready ===\n");
        handles
    }

    /// Spawn resolution threads (one per `system_threads` config value).
    fn spawn_resolution_threads(&self) -> Vec<JoinHandle<()>> {
        let mut handles = Vec::new();
        for thread_id in 0..self.shared.config.system_threads {
            let shared_clone = Arc::clone(&self.shared);
            let thread_core = self.shared.config.core_offset + thread_id;
            let thread_slot = self.shared.config.slots + thread_id;

            let handle = spawn(move || {
                crate::scheduler::set_current_worker_id(thread_slot);

                if let Some(ref recorder) = shared_clone.telemetry.async_recorder {
                    if let Some(tx) = recorder.get_worker_sender(thread_slot) {
                        set_worker_recorder(tx);
                    }
                }

                if core_affinity::set_for_current(core_affinity::CoreId { id: thread_core }) {
                    println!(
                        "Resolution thread {} pinned to core {:?} with slot {}",
                        thread_id, thread_core, thread_slot
                    );
                } else {
                    println!(
                        "Failed to pin resolution thread {} to core {:?}",
                        thread_id, thread_core
                    );
                }

                Self::resolution(shared_clone, thread_core, thread_id, thread_slot);
            });
            handles.push(handle);
        }
        print_debug(|| {
            format!(
                "{} Resolution threads spawned",
                self.shared.config.system_threads
            )
        });
        handles
    }

    /// Signal receiver threads to stop and join them, then report drop statistics.
    fn shutdown_receiver_threads(&self, handles: Vec<JoinHandle<()>>) {
        if handles.is_empty() {
            return;
        }

        println!("Shutting down {} receiver threads...", handles.len());
        self.shared.net.shutdown_flag.store(true, Ordering::SeqCst);

        for (idx, handle) in handles.into_iter().enumerate() {
            handle.join().unwrap();
            println!("  Receiver thread {} shut down successfully", idx);
        }

        // Report packet drop statistics
        if let Some(ref network_config) = self.shared.graph.network_config {
            let num_sockets = network_config.num_sockets;
            let mut total_drops = 0;
            println!("\nPacket Drop Statistics:");
            for socket_id in 0..num_sockets {
                let drops = self.shared.net.packet_drop_counters[socket_id]
                    .load(Ordering::SeqCst);
                total_drops += drops;
                if drops > 0 {
                    println!("  Socket {}: {} packets dropped", socket_id, drops);
                }
            }
            if total_drops == 0 {
                println!("  No packets dropped!");
            } else {
                println!("  TOTAL: {} packets dropped across all sockets", total_drops);
            }
        }

        let dropped_frames = self.shared.net.dropped_streams.load(Ordering::SeqCst);
        if dropped_frames > 0 {
            println!("\nDropped Frame Statistics:");
            println!(
                "  TOTAL: {} frames dropped (no available slots)",
                dropped_frames
            );
        }
    }

}

// ---------------------------------------------------------------------------
// Private helpers for SynRtBuilder::build()
// ---------------------------------------------------------------------------

/// Build the node cache from graph nodes, computing all pre-derived flags.
///
/// Sets: `successor_count`, `worker_resolvable`, `needs_result_store`,
/// `priority`, and `affinity_group` in addition to the base cache entry.
fn build_node_cache(app_graph: &Graph, scheduler: &SchedulerImpl) -> Vec<NodeCacheEntry> {
    let init_objects = app_graph.init_objects.as_ref().unwrap();
    let mut cache: Vec<NodeCacheEntry> = app_graph
        .nodes
        .iter()
        .map(|node| {
            node_cache_entry(
                node,
                init_objects,
                &app_graph.initial_nodes,
                &app_graph.condition_nodes,
            )
        })
        .collect();

    // successor_count — used by inline-continuation to decide whether to elide a spawn
    for (node_id, entry) in cache.iter_mut().enumerate() {
        if node_id < app_graph.successors.len() {
            entry.successor_count = app_graph.successors[node_id].len();
        }
    }

    // worker_resolvable — true when all successors are non-condition nodes;
    // allows the completing worker to resolve deps without touching the batch_queue.
    for node_id in 0..cache.len() {
        let all_non_condition = if node_id < app_graph.successors.len() {
            app_graph.successors[node_id]
                .iter()
                .all(|&succ_id| cache[succ_id as usize].node_condition.is_none())
        } else {
            true // no successors → eligible
        };
        cache[node_id].worker_resolvable = all_non_condition;
    }

    // needs_result_store — false when no successor reads this node via $res.
    // When false, node_results.set() can be elided on the hot path.
    for node_id in 0..cache.len() {
        let has_res_consumer = node_id < app_graph.successors.len()
            && app_graph.successors[node_id].iter().any(|&succ_id| {
                app_graph.nodes[succ_id as usize].args.iter().any(|arg| {
                    arg.type_.is_result()
                        && arg
                            .predecessor
                            .as_ref()
                            .map_or(false, |p| p.id == node_id as IdType)
                })
            });
        cache[node_id].needs_result_store = has_res_consumer;
    }

    // priority and affinity_group — pre-computed to avoid per-task lookups on the hot path
    {
        use crate::custom_scheduler::Priority;
        use crate::graph_struct::NodePriority;
        for (node_id, entry) in cache.iter_mut().enumerate() {
            let node = &app_graph.nodes[node_id];
            entry.priority = match node.priority {
                NodePriority::High => Priority::High,
                NodePriority::Normal => Priority::Normal,
                NodePriority::Low => Priority::Low,
            };
            entry.affinity_group = scheduler.get_affinity_group(node.use_workers.as_ref());
        }
    }

    cache
}

/// Precompute predecessor routing tables from the graph's successor/arg structure.
///
/// Returns three `num_nodes × num_nodes` tables:
/// - `pred_index_filter`: index range `[min, max)` within a predecessor's instances that a
///   successor reads. Used to skip dispatching to successors that don't read the completed instance.
/// - `pred_group_by`: the `group_by` divisor for grouped predecessors.
/// - `pred_succ_1to1_offset`: `indexes[0]` offset for 1:1 non-barrier single-index `$res` deps
///   with equal factor. Enables exact successor dispatch, eliminating `spin_wait` deadlocks.
fn build_predecessor_tables(
    app_graph: &Graph,
) -> (
    Vec<Vec<Option<(usize, usize)>>>,
    Vec<Vec<Option<usize>>>,
    Vec<Vec<Option<isize>>>,
) {
    let num_nodes = app_graph.nodes.len();
    let mut filter: Vec<Vec<Option<(usize, usize)>>> = vec![vec![None; num_nodes]; num_nodes];
    let mut group_by: Vec<Vec<Option<usize>>> = vec![vec![None; num_nodes]; num_nodes];
    let mut succ_1to1_offset: Vec<Vec<Option<isize>>> = vec![vec![None; num_nodes]; num_nodes];

    for succ_node in &app_graph.nodes {
        let succ_id = succ_node.id as usize;
        let succ_factor = succ_node.factor;

        for arg in &succ_node.args {
            let Some(pred) = &arg.predecessor else { continue };
            let pred_id = pred.id as usize;
            let pred_factor = app_graph.nodes[pred_id].factor;

            if !pred.indexes.is_empty() {
                let min_idx = *pred.indexes.iter().min().unwrap() as usize;
                let max_idx = *pred.indexes.iter().max().unwrap() as usize;
                let range_len = max_idx - min_idx + 1;

                let should_filter = if pred.group_by.is_some() {
                    true // always filter when group_by present (needed for offset calculation)
                } else if range_len < pred_factor && range_len == pred.indexes.len() {
                    range_len == succ_factor
                } else {
                    false
                };

                if should_filter {
                    filter[succ_id][pred_id] = Some((min_idx, max_idx + 1));
                }
            }

            if let Some(gb) = pred.group_by {
                group_by[succ_id][pred_id] = Some(gb);
            }

            // 1:1 non-barrier single-index $res with equal factors: store offset so we can
            // fire the exact successor instance that reads this predecessor.
            if !arg.is_barrier()
                && pred.group_by.is_none()
                && pred.indexes.len() == 1
                && succ_factor == pred_factor
                && succ_factor > 1
            {
                succ_1to1_offset[succ_id][pred_id] = Some(pred.indexes[0]);
            }
        }
    }

    (filter, group_by, succ_1to1_offset)
}

/// Build the per-slot task counters and condition instance tracking.
///
/// Returns:
/// - `pending_tasks`: per-slot regular (non-condition, non-initial) task count.
/// - `pending_cond_tasks`: per-slot condition task count.
/// - `cond_instances_to_spawn`: generational packed counters for condition node spawn tracking.
fn build_slot_counters(
    slots: usize,
    node_cache: &[NodeCacheEntry],
) -> (Vec<AtomicUsize>, Vec<AtomicUsize>, Vec<Vec<AtomicU64>>) {
    let total_tasks: usize = node_cache
        .iter()
        .filter(|nc| !nc.is_initial && !nc.is_condition)
        .map(|nc| nc.factor)
        .sum();
    let total_cond_tasks: usize = node_cache
        .iter()
        .filter(|nc| nc.is_condition)
        .map(|nc| nc.factor)
        .sum();

    let pending_tasks: Vec<AtomicUsize> = (0..slots)
        .map(|_| AtomicUsize::new(total_tasks))
        .collect();
    let pending_cond_tasks: Vec<AtomicUsize> = (0..slots)
        .map(|_| AtomicUsize::new(total_cond_tasks))
        .collect();

    // Packed (gen: u32, remaining_spawns: u32) — generation mismatch triggers lazy reinit.
    let cond_instances_to_spawn: Vec<Vec<AtomicU64>> = (0..slots)
        .map(|_| {
            node_cache
                .iter()
                .map(|nc| {
                    if nc.is_condition {
                        AtomicU64::new(crate::buffers::gen_pack(0, nc.factor as u32))
                    } else {
                        AtomicU64::new(crate::buffers::gen_pack(0, 0))
                    }
                })
                .collect()
        })
        .collect();

    (pending_tasks, pending_cond_tasks, cond_instances_to_spawn)
}

