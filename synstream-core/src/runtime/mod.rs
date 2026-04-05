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

// Re-export the public API types needed by external modules
pub use shared_data::{
    BatchQueueRx, BatchQueueTx, ExecCtx, GraphCache, NetworkInfra, RuntimeConfig, SharedData,
    SlotData, SlotState, Telemetry,
};
pub use node_cache::{ArgCacheEntry, NodeCacheEntry, NodeConditionCache};

use core_affinity;
use node_cache::node_cache_entry;
use network_init::prepare_network_infrastructure;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, sleep, spawn};
use std::time::{Duration, Instant};

use crate::async_recorder::{set_worker_recorder, AsyncRecorder};
use crate::debug::print_debug;
use crate::graph::*;
use crate::network::{multi_socket_receiver_loop, single_socket_receiver_loop};
use crate::resolution_state::{MultiThreadedState, ResolutionState};
use crate::scheduler::SchedulerImpl;
use crate::time_buffer::TimeBufferManager;
use crate::{graph_struct::*, IdType};
use crossbeam_channel::bounded as cb_bounded;

pub const RUN_SLEEP: Duration = Duration::from_secs(10);

// Main SynStream Runtime struct with shared context
pub struct SynRt {
    shared: Arc<SharedData>,
}

impl SynRt {
    pub fn new(
        app_graph: &Graph,
        slots: usize,
        max_streams: usize,
        max_runtime: Option<u64>,
        use_rdtsc: bool,
        record: bool,
        record_stream: Option<usize>,
        timing_enabled: bool,
        scheduler: SchedulerImpl,
        base_instant: Instant,
        slot_priority_enabled: bool,
        async_recorder: Option<Arc<AsyncRecorder>>, // Optional shared recorder from caller
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
    ) -> SynRt {
        // Initialize stream completion counters
        let stream_completion_counts = Arc::new(RwLock::new(Vec::new()));
        let mut completion_counts = stream_completion_counts.write();
        completion_counts.clear();

        let slots = std::cmp::min(slots, max_streams);
        for _ in 0..slots {
            completion_counts.push(AtomicUsize::new(0));
        }
        drop(completion_counts);

        // Build node cache for fast repeated access
        let mut node_cache: Vec<NodeCacheEntry> = app_graph
            .nodes
            .iter()
            .map(|node| {
                node_cache_entry(
                    node,
                    app_graph.init_objects.as_ref().unwrap(),
                    &app_graph.initial_nodes,
                    &app_graph.condition_nodes,
                )
            })
            .collect();

        // Phase 3B: Populate successor_count for inline execution optimization
        for (node_id, entry) in node_cache.iter_mut().enumerate() {
            if node_id < app_graph.successors.len() {
                entry.successor_count = app_graph.successors[node_id].len();
            }
        }

        // Compute worker_resolvable: true if ALL successors are non-condition nodes.
        // Worker-side resolution bypasses the batch_queue → resolution thread round-trip
        // by resolving dependencies directly on the completing worker thread.
        for node_id in 0..node_cache.len() {
            if node_id < app_graph.successors.len() {
                let all_succs_non_condition = app_graph.successors[node_id].iter().all(|&succ_id| {
                    node_cache[succ_id as usize].node_condition.is_none()
                });
                node_cache[node_id].worker_resolvable = all_succs_non_condition;
            } else {
                // No successors → eligible (just decrement pending_tasks)
                node_cache[node_id].worker_resolvable = true;
            }
        }

        // Compute needs_result_store: true if any successor reads this node via $res.
        // $barrier and $dep args are excluded: barriers carry no value; $dep is an
        // ordering-only dependency whose result is not fetched from the buffer.
        // When false, no successor consumes the result and node_results.set() can be elided.
        for node_id in 0..node_cache.len() {
            let has_res_succ = if node_id < app_graph.successors.len() {
                app_graph.successors[node_id].iter().any(|&succ_id| {
                    app_graph.nodes[succ_id as usize].args.iter().any(|arg| {
                        arg.type_.is_result()
                            && arg
                                .predecessor
                                .as_ref()
                                .map_or(false, |p| p.id == node_id as IdType)
                    })
                })
            } else {
                false
            };
            node_cache[node_id].needs_result_store = has_res_succ;
        }

        // Pre-compute scheduler priority and affinity group per node.
        // Avoids per-task node.name.clone(), node.use_workers.clone(), and
        // priority conversion in the hot send_to_scheduler loop.
        {
            use crate::custom_scheduler::Priority;
            use crate::graph_struct::NodePriority;
            for (node_id, entry) in node_cache.iter_mut().enumerate() {
                let node = &app_graph.nodes[node_id];
                entry.priority = match node.priority {
                    NodePriority::High => Priority::High,
                    NodePriority::Normal => Priority::Normal,
                    NodePriority::Low => Priority::Low,
                };
                entry.affinity_group = scheduler.get_affinity_group(node.use_workers.as_ref());
            }
        }

        // Core configuration
        let system_threads = scheduler.system_threads();
        let core_offset = scheduler.core_offset();
        let receiver_threads = scheduler.receiver_threads();
        let receiver_core_offset = scheduler.receiver_core_offset();
        let workers = scheduler.workers();

        // Allocate slots + system_threads for TimeBuffer (slots for worker streams, system_threads for system threads)
        let time_buffer = if timing_enabled {
            Some(Arc::new(TimeBufferManager::new_async(
                slots + system_threads,
                system_threads,
                use_rdtsc,
            )))
        } else {
            None
        };

        // Use shared recorder when provided, otherwise create one; disable when recording is off
        let async_recorder = if record {
            async_recorder.or_else(|| {
                Some(Arc::new(AsyncRecorder::new(
                    slots + system_threads + receiver_threads,
                    100,
                )))
            })
        } else {
            None
        };

        let job_counter = Arc::new(AtomicUsize::new(0));

        // Create bounded crossbeam channel for lock-free task completion delivery.
        // Ring-buffer internals eliminate per-send Box::new; ring-buffer pop replaces
        // linked-list drain. Channel carries only NodeInfo tokens; results are pre-stored
        // in node_results by workers.
        let (batch_queue_tx, batch_queue_rx): (BatchQueueTx, BatchQueueRx) =
            cb_bounded(batch_queue_capacity);

        // Initialize shared dependency tracking structures
        let dependency_count_vec: Vec<usize> = app_graph.dependency_count_vec();

        // Compute max_factor for flat index computation
        let max_factor = node_cache.iter().map(|n| n.factor).max().unwrap_or(1);
        let num_nodes = app_graph.nodes.len();

        // Always use MultiThreadedState: worker-side dependency resolution means
        // multiple worker threads call decrease_and_get_ready_into concurrently,
        // requiring thread-safe atomics regardless of system_threads count.
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
        // Print ResolutionState
        println!(
            "\nResolutionState initialized:\n{}\n",
            resolution_state.debug_info()
        );

        // Compute O(1) slot completion counters from node_cache (no per-slot Vec needed).
        // total_tasks: sum of factors for non-initial, non-condition nodes (dep-tracked tasks).
        // total_cond_tasks: sum of factors for condition nodes.
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

        // Initialize O(1) slot completion counters - Phase 1.2 optimization
        // These replace the O(N×F) linear scan in slot completion checking
        let slot_pending_tasks: Vec<AtomicUsize> =
            (0..slots).map(|_| AtomicUsize::new(total_tasks)).collect();
        let slot_pending_cond_tasks: Vec<AtomicUsize> = (0..slots)
            .map(|_| AtomicUsize::new(total_cond_tasks))
            .collect();

        // Initialize condition spawn counters - tracks remaining instances to spawn per condition node.
        // Each AtomicU64 packs (gen: u32, remaining_spawns: u32). Generation mismatch triggers
        // lazy reinit to nc.factor, eliminating the O(cond_nodes) reset loop at slot completion.
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

        // Initialize per-slot buffering queues (stores NodeInfo + packet data)
        let slot_buffers = Arc::new(RwLock::new(vec![Vec::new(); slots]));

        let (
            receiver_sockets,
            packet_sender,
            packet_receiver,
            packet_drop_counters,
            buffer_return_senders,
            buffer_return_receivers,
        ) = prepare_network_infrastructure(app_graph, socket_recv_buf_bytes, recv_pool_size);

        // Precompute pred_index_filter, pred_group_by, and pred_succ_1to1_offset.
        let num_nodes = app_graph.nodes.len();
        let (pred_index_filter, pred_group_by, pred_succ_1to1_offset) = {
            let mut filter: Vec<Vec<Option<(usize, usize)>>> =
                vec![vec![None; num_nodes]; num_nodes];
            let mut group_by: Vec<Vec<Option<usize>>> = vec![vec![None; num_nodes]; num_nodes];
            // For 1:1 non-barrier single-index $res deps with equal succ/pred factors:
            // stores indexes[0] offset k so caller can compute specific_succ_idx =
            // (pred_idx - k + succ_factor) % succ_factor, firing the exact successor
            // instance that reads this predecessor (eliminates spin_wait deadlock).
            let mut succ_1to1_offset: Vec<Vec<Option<isize>>> =
                vec![vec![None; num_nodes]; num_nodes];

            for succ_node in &app_graph.nodes {
                let succ_id = succ_node.id as usize;
                for arg in &succ_node.args {
                    if let Some(pred) = &arg.predecessor {
                        let pred_id = pred.id as usize;
                        let pred_factor = app_graph.nodes[pred_id].factor;

                        // Check if indexes form a contiguous subrange of [0, pred_factor)
                        // Create filter for:
                        // 1. Grouped predecessors (group_by present): allows many-to-few mapping via groups
                        // 2. 1:1 mapping (range_len == succ_factor): direct instance correspondence
                        // Single-index refs like indexes="0" are data references, not filters.
                        if !pred.indexes.is_empty() {
                            let min_idx = *pred.indexes.iter().min().unwrap() as usize;
                            let max_idx = *pred.indexes.iter().max().unwrap() as usize;
                            let range_len = max_idx - min_idx + 1;
                            let succ_factor = succ_node.factor;

                            // Determine if we should create a filter
                            let should_filter = if pred.group_by.is_some() {
                                // Always create filter when group_by present - needed for offset calculation
                                true
                            } else if range_len < pred_factor && range_len == pred.indexes.len() {
                                // Non-group_by case: create filter only for partial ranges
                                range_len == succ_factor
                            } else {
                                false
                            };

                            if should_filter {
                                filter[succ_id][pred_id] = Some((min_idx, max_idx + 1));
                            }
                        }

                        // Store group_by if present
                        if let Some(gb) = pred.group_by {
                            group_by[succ_id][pred_id] = Some(gb);
                        }

                        // 1:1 non-barrier single-index $res dep with equal factors:
                        // store the indexes offset so we can fire the exact successor
                        // instance that reads this predecessor (avoids ordinal mismatch).
                        let succ_factor = succ_node.factor;
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
            }
            (filter, group_by, succ_1to1_offset)
        };

        let shared = Arc::new(SharedData {
            graph: app_graph.clone(),
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
                max_streams,
                max_runtime,
                system_threads,
                receiver_threads,
                workers,
                core_offset,
                receiver_core_offset,
                slot_priority_enabled,
                coalesce_barriers,
                inline_continuation,
                single_slot_mode: slots == 1,
                record_stream,
                target_batch_size,
                batch_timeout_us,
                spin_iterations,
                sched_flush_threshold,
                spin_wait_spin_iters,
                spin_wait_yield_iters,
                spin_wait_park_ns,
                recv_pool_size,
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
                    (0..max_streams + slots)
                        .map(|_| AtomicBool::new(false))
                        .collect(),
                ),
            },
            exec: ExecCtx {
                scheduler: Arc::new(scheduler),
                batch_queue_tx,
                batch_queue_rx,
                resolution_state,
                node_results: Arc::new(crate::buffers::LockFreeResultMap::new(
                    &app_graph.nodes,
                    slots,
                )),
                initial_prep_done: Arc::new(AtomicUsize::new(0)),
            },
            telemetry: Telemetry {
                time_buffer,
                async_recorder,
                base_instant: Arc::new(base_instant),
                job_counter,
                stream_complete_counter: Arc::new(AtomicUsize::new(0)),
            },
        });

        SynRt { shared }
    }

    pub fn base_instant(&self) -> Instant {
        *self.shared.telemetry.base_instant
    }

    pub fn run(&mut self) {
        // Batch queue is already initialized in SharedData

        // Initialize node_results
        self.init_results(self.shared.config.slots);

        // Initiate synstream-runtime timing for system thread slots only
        for thread_id in 0..self.shared.config.system_threads {
            let system_slot = self.shared.config.slots + thread_id;
            if let Some(tb) = &self.shared.telemetry.time_buffer {
                tb.start_slot_processing(system_slot);
            }
        }

        let receiver_handles: Vec<std::thread::JoinHandle<()>> = if let Some(ref network_config) =
            self.shared.graph.network_config()
        {
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

            if receiver_threads >= num_sockets {
                println!("Using 1:1 thread-to-socket mapping (optimal)");
                for socket_id in 0..num_sockets {
                    let shared_clone = Arc::clone(&self.shared);
                    let core_id = receiver_offset + socket_id;

                    // Take the return-channel receiver end for this socket.
                    // Each receiver is taken exactly once here; subsequent access would panic.
                    let return_rx = self.shared.net.buffer_return_receivers[socket_id]
                        .lock()
                        .take()
                        .expect("buffer_return_receivers already taken");

                    let handle = thread::Builder::new()
                        .name(format!("rx-{}", socket_id))
                        .spawn(move || {
                            single_socket_receiver_loop(
                                shared_clone,
                                socket_id,
                                core_id,
                                return_rx,
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

                    // Collect the return-channel receiver ends for all sockets in this thread's range.
                    let return_rxs: Vec<flume::Receiver<Vec<u8>>> = (start_socket..end_socket)
                        .map(|sid| {
                            self.shared.net.buffer_return_receivers[sid]
                                .lock()
                                .take()
                                .expect("buffer_return_receivers already taken")
                        })
                        .collect();

                    let shared_clone = Arc::clone(&self.shared);
                    let core_id = receiver_offset + thread_id;

                    let handle = thread::Builder::new()
                        .name(format!("rx-multi-{}", thread_id))
                        .spawn(move || {
                            multi_socket_receiver_loop(
                                shared_clone,
                                thread_id,
                                socket_range,
                                core_id,
                                return_rxs,
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
        } else {
            println!("No network_config present - skipping network receiver setup");
            Vec::new()
        };

        // Spawn multiple resolution threads
        let mut resolution_handles = Vec::new();
        for thread_id in 0..self.shared.config.system_threads {
            let shared_for_resolution = Arc::clone(&self.shared);
            let thread_core = self.shared.config.core_offset + thread_id;
            // Each system thread gets its own slot: slots + thread_id
            let thread_slot = self.shared.config.slots + thread_id;

            let resolution_handle = spawn(move || {
                // Set worker ID for this system thread (slots + thread_id)
                crate::scheduler::set_current_worker_id(thread_slot);

                // Initialize per-worker recording channel for this system thread
                if let Some(ref recorder) = shared_for_resolution.telemetry.async_recorder {
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

                Self::resolution(shared_for_resolution, thread_core, thread_id, thread_slot);
            });
            resolution_handles.push(resolution_handle);
        }
        print_debug(|| format!("{} Resolution threads spawned", self.shared.config.system_threads));

        let start_time = Instant::now();
        // Check for max_runtime
        print_debug(|| "Max runtime check started".to_string());
        if let Some(max_runtime) = self.shared.config.max_runtime {
            sleep(RUN_SLEEP);
            let mut finish: bool = false;
            loop {
                let completed_streams = self.shared.telemetry.stream_complete_counter.load(Ordering::Acquire);

                let completed = { completed_streams == self.shared.config.max_streams };

                if start_time.elapsed().as_secs() > max_runtime {
                    println!("Max runtime reached exiting...");
                    finish = true;
                } else if completed {
                    println!("No pending jobs and all jobs completed, exiting...");
                    finish = true;
                }

                if finish {
                    // Signal all resolution threads to exit
                    self.shared.net.shutdown_flag.store(true, Ordering::SeqCst);
                    println!("Shutdown flag set - signaling resolution threads to exit");

                    // Process post-nodes if any
                    println!("Processing possible post-nodes...");
                    self.schedule_post_nodes();
                    // Signal flusher thread to shut down (will be done after loop)
                    break;
                }
                sleep(RUN_SLEEP);
            }
        }

        // No flusher thread to shutdown - batch_queue handles cleanup automatically

        // Wait for all resolution threads to finish
        for handle in resolution_handles {
            handle.join().unwrap();
        }

        // Gracefully shutdown receiver threads if they were spawned
        if !receiver_handles.is_empty() {
            println!(
                "Shutting down {} receiver threads...",
                receiver_handles.len()
            );

            // Signal shutdown
            self.shared.net.shutdown_flag.store(true, Ordering::SeqCst);

            // Wait for all receiver threads to exit
            for (idx, handle) in receiver_handles.into_iter().enumerate() {
                handle.join().unwrap();
                println!("  Receiver thread {} shut down successfully", idx);
            }

            // Report packet drop statistics
            if let Some(ref network_config) = self.shared.graph.network_config {
                let num_sockets = network_config.num_sockets;
                let mut total_drops = 0;
                println!("\nPacket Drop Statistics:");
                for socket_id in 0..num_sockets {
                    let drops = self.shared.net.packet_drop_counters[socket_id].load(Ordering::SeqCst);
                    total_drops += drops;
                    if drops > 0 {
                        println!("  Socket {}: {} packets dropped", socket_id, drops);
                    }
                }
                if total_drops == 0 {
                    println!("  No packets dropped!");
                } else {
                    println!(
                        "  TOTAL: {} packets dropped across all sockets",
                        total_drops
                    );
                }
            }

            // Report dropped frame statistics (frames discarded due to full slot table)
            let dropped_frames = self.shared.net.dropped_streams.load(Ordering::SeqCst);
            if dropped_frames > 0 {
                println!("\nDropped Frame Statistics:");
                println!("  TOTAL: {} frames dropped (no available slots)", dropped_frames);
            }
        }

        // Finish timing for system thread slots only
        for thread_id in 0..self.shared.config.system_threads {
            let system_slot = self.shared.config.slots + thread_id;
            if let Some(tb) = &self.shared.telemetry.time_buffer {
                let _ = tb.finish_slot_processing(system_slot);
            }
        }
    }

    fn init_results(&mut self, _slots: usize) {
        // Lock-free result map is already initialized in constructor
        // No initialization needed - atomic pointers start as null

        // Note: post_nodes slots are handled by extend_slot() calls if needed
        let _nodes = &self.shared.graph.nodes;
        let _post_nodes_opt = &self.shared.graph.post_nodes;
        // The LockFreeResultMap is created with the right capacity upfront
    }
}
