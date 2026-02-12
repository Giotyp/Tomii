use core_affinity;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, sleep, spawn, JoinHandle};
use std::time::{Duration, Instant};

use crate::async_recorder::{set_worker_recorder, submit_record, AsyncRecorder};
use crate::debug::print_debug;
use crate::graph::*;
use crate::graph_struct::*;
use crate::network::{
    bind_udp_socket_range, multi_socket_receiver_loop, single_socket_receiver_loop, NetworkSocket,
    PacketMessage,
};
use crate::resolution_state::{MultiThreadedState, ResolutionState, SingleThreadedState};
use crate::runtime_funcs::*;
use crate::scheduler::SchedulerImpl;
use crate::time_buffer::{TimeBufferManager, TimingMethod};
use crate::{buffers::*, IdType, Record};
use crossbeam_channel::{Receiver as BatchReceiver, Sender as BatchSender};
use synstream_types::*;

pub const RUN_SLEEP: Duration = Duration::from_secs(10);

/// Drain up to `max_items` from a crossbeam channel, waiting up to `timeout` for the first item.
/// Returns immediately with available items if any are ready (non-blocking fast path).
/// If no items ready, blocks for up to `timeout` for the first item, then drains remaining.
fn recv_batch<T>(
    rx: &crossbeam_channel::Receiver<T>,
    max_items: usize,
    timeout: Duration,
) -> Vec<T> {
    let mut batch = Vec::new();

    // Fast path: drain all immediately available items (non-blocking)
    while batch.len() < max_items {
        match rx.try_recv() {
            Ok(item) => batch.push(item),
            Err(_) => break,
        }
    }
    if !batch.is_empty() {
        return batch;
    }

    // Slow path: wait for first item with timeout
    match rx.recv_timeout(timeout) {
        Ok(item) => batch.push(item),
        Err(_) => return batch, // timeout or disconnected
    }

    // Drain remaining available items up to max
    while batch.len() < max_items {
        match rx.try_recv() {
            Ok(item) => batch.push(item),
            Err(_) => break,
        }
    }
    batch
}

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

        // Create crossbeam channel for lock-free task completion delivery
        let (batch_queue_tx, batch_queue_rx) = crossbeam_channel::unbounded();

        // Initialize shared dependency tracking structures
        let dependency_count_vec: Vec<usize> = app_graph.dependency_count_vec();

        // Compute max_factor for flat index computation
        let max_factor = node_cache.iter().map(|n| n.factor).max().unwrap_or(1);
        let num_nodes = app_graph.nodes.len();

        // Choose resolution state implementation based on system_threads
        let resolution_state: Arc<dyn ResolutionState> = if system_threads == 1 {
            println!("Using single-threaded resolution state (no locks)");
            Arc::new(SingleThreadedState::new(
                num_nodes,
                slots,
                max_factor,
                dependency_count_vec.clone(),
                &app_graph.nodes,
            ))
        } else {
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

        // Initialize remaining nodes trackers with AtomicUsize for thread-safe access
        let mut remaining_nodes = Vec::new();
        let mut remaining_cond_nodes = Vec::new();
        let mut remaining_init = Vec::new(); // Store initial values for reinit
        let mut node_id_to_rem = vec![0; app_graph.nodes.len()];
        let mut node_id_is_cond = vec![false; app_graph.nodes.len()]; // Track which nodes are condition nodes

        for _slot in 0..slots {
            let mut slot_remaining = Vec::new();
            let mut slot_cond_remaining = Vec::new();
            let mut slot_remaining_init = Vec::new();

            for node_id in 0..app_graph.nodes.len() {
                if app_graph.initial_nodes.contains(&(node_id as IdType)) {
                    slot_remaining.push(AtomicUsize::new(0));
                    slot_remaining_init.push(0);
                    node_id_to_rem[node_id] = slot_remaining.len() - 1;
                    node_id_is_cond[node_id] = false;
                } else if !app_graph.condition_nodes.contains(&(node_id as IdType)) {
                    let factor = node_cache[node_id].factor;
                    slot_remaining.push(AtomicUsize::new(factor));
                    slot_remaining_init.push(factor);
                    node_id_to_rem[node_id] = slot_remaining.len() - 1;
                    node_id_is_cond[node_id] = false;
                } else {
                    slot_cond_remaining.push(AtomicUsize::new(node_cache[node_id].factor));
                    node_id_to_rem[node_id] = slot_cond_remaining.len() - 1;
                    node_id_is_cond[node_id] = true;
                }
            }
            remaining_nodes.push(slot_remaining);
            remaining_cond_nodes.push(slot_cond_remaining);
            remaining_init.push(slot_remaining_init);
        }

        // Initialize O(1) slot completion counters - Phase 1.2 optimization
        // These replace the O(N×F) linear scan in slot completion checking
        let mut slot_pending_tasks = Vec::with_capacity(slots);
        let mut slot_pending_cond_tasks = Vec::with_capacity(slots);

        for slot in 0..slots {
            // Sum all initial dependency counts for non-initial nodes in this slot
            let total_tasks: usize = remaining_init[slot].iter().sum();
            slot_pending_tasks.push(AtomicUsize::new(total_tasks));

            // Sum all condition node factors for this slot
            let total_cond_tasks: usize = remaining_cond_nodes[slot]
                .iter()
                .map(|atomic| atomic.load(Ordering::SeqCst))
                .sum();
            slot_pending_cond_tasks.push(AtomicUsize::new(total_cond_tasks));
        }

        // Initialize condition spawn counters - tracks remaining instances to spawn per condition node
        // Used to skip condition evaluation once all instances have been spawned
        let cond_instances_to_spawn: Vec<Vec<AtomicUsize>> = (0..slots)
            .map(|_| {
                node_cache
                    .iter()
                    .map(|nc| {
                        if nc.is_condition {
                            AtomicUsize::new(nc.factor) // Start with factor count
                        } else {
                            AtomicUsize::new(0) // Non-condition nodes: not used
                        }
                    })
                    .collect()
            })
            .collect();

        // Initialize per-slot buffering queues (stores NodeInfo + packet data)
        let slot_buffers = Arc::new(RwLock::new(vec![Vec::new(); slots]));

        let (receiver_sockets, packet_sender, packet_receiver, packet_drop_counters) =
            prepare_network_infrastructure(app_graph);

        let shared = Arc::new(SharedData {
            graph: app_graph.clone(),
            slots,
            max_streams,
            max_runtime,
            system_threads,
            receiver_threads,
            workers,
            core_offset,
            receiver_core_offset,
            record_stream,
            node_cache,
            node_results: Arc::new(crate::buffers::LockFreeResultMap::new(
                &app_graph.nodes,
                slots,
            )),
            stream_complete_counter: Arc::new(AtomicUsize::new(0)),
            running_streams: Arc::new(RwLock::new(Vec::new())),
            time_buffer,
            scheduler: Arc::new(scheduler),
            async_recorder,
            base_instant: Arc::new(base_instant),
            job_counter,
            batch_queue_tx,
            batch_queue_rx,
            target_batch_size,
            batch_timeout_us,
            resolution_state,
            remaining_nodes: Arc::new(remaining_nodes),
            remaining_cond_nodes: Arc::new(remaining_cond_nodes),
            node_id_to_rem: Arc::new(node_id_to_rem),
            node_id_is_cond: Arc::new(node_id_is_cond),
            remaining_init: Arc::new(remaining_init),
            initial_prep_done: Arc::new(AtomicUsize::new(0)),
            slot_pending_tasks: Arc::new(slot_pending_tasks),
            slot_pending_cond_tasks: Arc::new(slot_pending_cond_tasks),
            cond_instances_to_spawn: Arc::new(cond_instances_to_spawn),
            slot_states: Arc::new(RwLock::new(vec![SlotState::Inactive; slots])),
            last_slot_assigned: Arc::new(AtomicUsize::new(0)),
            slot_priority_enabled,
            slot_buffers,
            // Network fields (empty vecs when no network_config)
            receive_finished: Arc::new(AtomicBool::new(false)),
            packet_sender,
            packet_receiver,
            receiver_sockets,
            packet_drop_counters,
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            slot_packet_counters: Arc::new((0..slots).map(|_| AtomicUsize::new(0)).collect()),
            streams_receive_counter: Arc::new(AtomicUsize::new(0)),
            // Initialize packet completion flags to false for all slots
            slot_packet_complete: Arc::new((0..slots).map(|_| AtomicBool::new(false)).collect()),
            // Initialize in-flight batch processing counter to 0 for all slots
            slot_processing_count: Arc::new((0..slots).map(|_| AtomicUsize::new(0)).collect()),
        });

        SynRt { shared }
    }

    pub fn base_instant(&self) -> Instant {
        *self.shared.base_instant
    }

    pub fn run(&mut self) {
        // Batch queue is already initialized in SharedData

        // Initialize node_results
        self.init_results(self.shared.slots);

        // Initiate synstream-runtime timing for system thread slots only
        for thread_id in 0..self.shared.system_threads {
            let system_slot = self.shared.slots + thread_id;
            if let Some(tb) = &self.shared.time_buffer {
                tb.start_slot_processing(system_slot);
            }
        }

        let receiver_handles: Vec<JoinHandle<()>> = if let Some(ref network_config) =
            self.shared.graph.network_config()
        {
            let num_sockets = network_config.num_sockets;
            let buffer_depth = network_config.buffer_depth;

            println!("\n=== Initializing Network Receiver Infrastructure ===");
            println!("Number of sockets: {}", num_sockets);
            println!("Buffer depth: {} packets per socket", buffer_depth);

            assert_eq!(
                self.shared.receiver_sockets.len(),
                num_sockets,
                "Network config expected {} sockets but {} were allocated",
                num_sockets,
                self.shared.receiver_sockets.len()
            );

            let receiver_threads = self.shared.receiver_threads;
            let receiver_offset = self.shared.receiver_core_offset;
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

                    let handle = thread::Builder::new()
                        .name(format!("rx-{}", socket_id))
                        .spawn(move || {
                            single_socket_receiver_loop(shared_clone, socket_id, core_id);
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
        for thread_id in 0..self.shared.system_threads {
            let shared_for_resolution = Arc::clone(&self.shared);
            let thread_core = self.shared.core_offset + thread_id;
            // Each system thread gets its own slot: slots + thread_id
            let thread_slot = self.shared.slots + thread_id;

            let resolution_handle = spawn(move || {
                // Set worker ID for this system thread (slots + thread_id)
                crate::scheduler::set_current_worker_id(thread_slot);

                // Initialize per-worker recording channel for this system thread
                if let Some(ref recorder) = shared_for_resolution.async_recorder {
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
        print_debug(|| format!("{} Resolution threads spawned", self.shared.system_threads));

        let start_time = Instant::now();
        // Check for max_runtime
        print_debug(|| "Max runtime check started".to_string());
        if let Some(max_runtime) = self.shared.max_runtime {
            sleep(RUN_SLEEP);
            let mut finish: bool = false;
            loop {
                let completed_streams = self.shared.stream_complete_counter.load(Ordering::SeqCst);

                let completed = { completed_streams == self.shared.max_streams };

                if start_time.elapsed().as_secs() > max_runtime {
                    println!("Max runtime reached exiting...");
                    finish = true;
                } else if completed {
                    println!("No pending jobs and all jobs completed, exiting...");
                    finish = true;
                }

                if finish {
                    // Signal all resolution threads to exit
                    self.shared.shutdown_flag.store(true, Ordering::SeqCst);
                    println!("Shutdown flag set - signaling resolution threads to exit");

                    // Process post-nodes if any
                    println!("Processing possible post-nodes...");
                    self.schedule_post_nodes();
                    // Signal flusher thread to shut down (will be done after loop)
                    break;
                }
                sleep(RUN_SLEEP);
                //
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
            self.shared.shutdown_flag.store(true, Ordering::SeqCst);

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
                    let drops = self.shared.packet_drop_counters[socket_id].load(Ordering::SeqCst);
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
        }

        // Finish timing for system thread slots only
        for thread_id in 0..self.shared.system_threads {
            let system_slot = self.shared.slots + thread_id;
            if let Some(tb) = &self.shared.time_buffer {
                let _ = tb.finish_slot_processing(system_slot);
            }
        }
    }
}

// Execution Threads
impl SynRt {
    fn preparation(
        shared: &Arc<SharedData>,
        nodes_to_schedule: &Vec<NodeInfo>,
        thread_core: usize,
        thread_slot: usize,
    ) {
        let start_time = if let Some(tb) = &shared.time_buffer {
            tb.measure_time()
        } else {
            TimingMethod::Instant(Instant::now())
        };
        let start_ns = shared.base_instant.elapsed().as_nanos();

        // Schedule Task - args will be built in the worker thread
        let pre_built_args_vec = vec![None; nodes_to_schedule.len()];
        let custom_func_vec = vec![None; nodes_to_schedule.len()];
        send_to_scheduler(
            &shared,
            nodes_to_schedule,
            &pre_built_args_vec,
            &custom_func_vec,
        );

        if let Some(tb) = &shared.time_buffer {
            let end_time = tb.measure_time();
            let duration = tb.measure_duration(start_time, end_time);
            tb.add_task_time(thread_slot, "Preparation", usize::MAX, duration);
        }

        // Lock-free recording via per-worker channel
        let current_stream = shared.stream_complete_counter.load(Ordering::SeqCst);
        if shared.async_recorder.is_some() && should_record_slot(&shared, current_stream) {
            let end_ns = shared.base_instant.elapsed().as_nanos();
            let job_id = shared.job_counter.fetch_add(1, Ordering::SeqCst);
            submit_record(Record {
                slot: thread_slot,
                job_id,
                start_ns,
                end_ns,
                worker: thread_core,
                task_id: IdType::MAX - 1,
                index: 0,
            });
        }
    }

    /// Resolution Thread: Processes completed compute tasks and manages stream lifecycle
    fn resolution(
        shared: Arc<SharedData>,
        thread_core: usize,
        thread_id: usize,
        thread_slot: usize,
    ) {
        // Initialize async recorder for system thread using universal indexing
        if let Some(ref recorder) = shared.async_recorder {
            let channel_index = thread_core - shared.core_offset;
            if let Some(tx) = recorder.get_worker_sender(channel_index) {
                set_worker_recorder(tx);
            }
        }

        // let all_slots: Vec<usize> = (0..shared.slots).collect();
        if thread_id == 0 {
            // Ensure only one thread does initial preparation
            if shared
                .initial_prep_done
                .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                print_debug(|| {
                    format!(
                        "Thread {} in Core {} performing initial preparation",
                        thread_id, thread_core
                    )
                });

                let activate_streams: Vec<usize> = {
                    if shared.slot_priority_enabled {
                        // activate first stream
                        vec![0]
                    } else {
                        // activate all streams
                        (0..shared.slots).collect()
                    }
                };

                if !shared.graph.initial_nodes.is_empty() {
                    // run assign_stream_to_available_slot for each stream to set slot state to Active
                    let assigned_slots: Vec<usize> = activate_streams
                        .iter()
                        .map(|&stream_id| assign_stream_to_available_slot(&shared, stream_id).0) // Extract slot_id from (slot_id, activated) tuple
                        .collect();

                    let compute_nodes = initial_nodes(&shared, assigned_slots);
                    if !compute_nodes.is_empty() {
                        Self::preparation(&shared, &compute_nodes, thread_core, thread_slot);
                    }
                }
            }
        }

        // prefetch cond indexes for efficiency
        let cond_indexes = shared.graph.get_condition_indexes();

        // Persistent completion tracking across all batches for this stream
        let mut stream_slot_activity: HashMap<usize, bool> = HashMap::new();

        // Packet Process Function
        let network_config_opt = shared.graph.network_config();
        // Track start of idle/wait periods so we can record waiting time
        let mut wait_start_ns: Option<u128> = None;
        // Minimum wait duration (in nanoseconds) to record - reduces record spam
        // Only record wait periods longer than 100μs to avoid thousands of tiny records
        const MIN_WAIT_RECORD_NS: u128 = 100_000; // 100μs

        let receive_timeout = Duration::from_micros(shared.batch_timeout_us);
        let mut packets_received: bool = false;

        // Process completed nodes with dynamic batching from scheduler
        loop {
            // Check shutdown flag first to exit immediately when signaled
            if shared.shutdown_flag.load(Ordering::SeqCst) {
                println!(
                    "Thread {} detected shutdown signal, exiting resolution loop",
                    thread_id
                );
                break;
            }

            // Poll packet channels if:
            // 1. Receivers are still active (!receive_finished), OR
            let mut should_poll_packets =
                thread_id == 0 && !shared.receive_finished.load(Ordering::SeqCst);

            while should_poll_packets {
                if let Some(network_config) = network_config_opt.as_ref() {
                    let stream_packets = network_config.stream_packets;
                    let packet_process_func = network_config.extract_packet_func.unwrap();

                    // Drain all available packets from channel (non-blocking)
                    // Using try_iter() to get all immediately available packets without batching limit
                    let packets: Vec<PacketMessage> = shared.packet_receiver.try_iter().collect();
                    let packet_rcv = if let Some(tb) = &shared.time_buffer {
                        tb.measure_time()
                    } else {
                        TimingMethod::Instant(Instant::now())
                    };

                    for packet_msg in packets {
                        let receiver_core_id = packet_msg.receiver_core_id;
                        let packet_timestamp = packet_msg.timestamp;
                        if let Some(tb) = &shared.time_buffer {
                            let dur = tb.measure_duration(
                                TimingMethod::Instant(packet_msg.timestamp),
                                packet_rcv.clone(),
                            );
                            tb.add_task_time(thread_slot, "Packet Received", usize::MAX, dur);
                        }

                        // Process packet and record
                        let received_bytes_cm = CmTypes::from_any(packet_msg.packet_bytes);
                        let start_proc = if let Some(tb) = &shared.time_buffer {
                            tb.measure_time()
                        } else {
                            TimingMethod::Instant(Instant::now())
                        };

                        let packet_cm = packet_process_func(vec![received_bytes_cm]);

                        if let Some(tb) = &shared.time_buffer {
                            let end_proc = tb.measure_time();
                            let dur = tb.measure_duration(start_proc, end_proc);
                            tb.add_task_time(thread_slot, "Packet Processing", usize::MAX, dur);
                        }

                        // Create temporary node_info with index=0 for ID function call
                        // The actual index will be set after slot assignment
                        let mut node_info = NodeInfo::new(0, 0, 0, 0); // network node id=0

                        // Call id_function to determine which stream this packet belongs to
                        let start_id = if let Some(tb) = &shared.time_buffer {
                            tb.measure_time()
                        } else {
                            TimingMethod::Instant(Instant::now())
                        };

                        let new_stream_opt = process_id_function(&shared, &packet_cm);

                        if let Some(tb) = &shared.time_buffer {
                            let end_id = tb.measure_time();
                            let dur = tb.measure_duration(start_id, end_id);
                            tb.add_task_time(thread_slot, "ID Function", usize::MAX, dur);
                        }

                        if let Some(new_stream) = new_stream_opt {
                            // Assign stream to an available slot
                            let start_sa = if let Some(tb) = &shared.time_buffer {
                                tb.measure_time()
                            } else {
                                TimingMethod::Instant(Instant::now())
                            };

                            let (assigned_slot, newly_activated) =
                                assign_stream_to_available_slot(&shared, new_stream);
                            node_info.slot = assigned_slot;

                            if let Some(tb) = &shared.time_buffer {
                                let end_sa = tb.measure_time();
                                let dur = tb.measure_duration(start_sa, end_sa);
                                tb.add_task_time(thread_slot, "Slot Assignment", usize::MAX, dur);
                            }

                            // CRITICAL FIX (Issue A): If slot was just activated from Inactive → Active,
                            // spawn initial compute nodes BEFORE processing any packets
                            if newly_activated {
                                let activated_compute_nodes =
                                    initial_nodes(&shared, vec![assigned_slot]);
                                if !activated_compute_nodes.is_empty() {
                                    print_debug(|| {
                                        format!(
                                            "Spawning {} initial nodes for newly activated slot {} (stream {})",
                                            activated_compute_nodes.len(),
                                            assigned_slot,
                                            new_stream
                                        )
                                    });
                                    Self::preparation(
                                        &shared,
                                        &activated_compute_nodes,
                                        thread_core,
                                        thread_slot,
                                    );
                                }
                            }

                            let packet_index = shared.slot_packet_counters[node_info.slot]
                                .fetch_add(1, Ordering::SeqCst);

                            node_info.index = packet_index;
                        } else {
                            // ID function failed, skip processing this node
                            print_debug(|| {
                                format!(
                                            "Thread {:?} -- Skipping further processing of node {:?} due to ID function failure",
                                            thread_id, node_info)
                            });
                            continue;
                        }

                        // CRITICAL FIX #3: Double-checked locking to prevent TOCTOU race
                        // Check slot state atomically while holding lock to ensure decision is valid
                        let slot_is_active = {
                            let states = shared.slot_states.read();
                            states[node_info.slot] == SlotState::Active
                        };

                        if slot_is_active {
                            let info_res = (node_info.clone(), packet_cm);

                            let start_ns_pkt = shared.base_instant.elapsed().as_nanos();
                            let start_proc = if let Some(tb) = &shared.time_buffer {
                                tb.measure_time()
                            } else {
                                TimingMethod::Instant(Instant::now())
                            };
                            Self::process_batch_resolution(
                                &shared,
                                vec![info_res],
                                thread_core,
                                thread_id,
                                thread_slot,
                                &cond_indexes,
                                &mut stream_slot_activity,
                                start_ns_pkt,
                            );
                            if let Some(tb) = &shared.time_buffer {
                                let end_proc = tb.measure_time();
                                let dur = tb.measure_duration(start_proc, end_proc);
                                tb.add_task_time(thread_slot, "Batch Resolution", usize::MAX, dur);
                            };
                        } else {
                            let mut slot_buffers = shared.slot_buffers.write();
                            slot_buffers[node_info.slot]
                                .push((node_info.clone(), packet_cm.clone()));
                            drop(slot_buffers); // Release write lock immediately
                        }

                        packets_received = true;

                        if shared.async_recorder.is_some() {
                            let receiver_slot = shared.slots + shared.system_threads;
                            let job_id = shared.job_counter.fetch_add(1, Ordering::SeqCst);

                            let packet_rcv = packet_timestamp
                                .duration_since(*shared.base_instant)
                                .as_nanos();
                            let delta_ns = 10000u128; // Small delta to make it visible in graphs

                            submit_record(Record {
                                slot: receiver_slot,
                                job_id,
                                start_ns: packet_rcv,
                                end_ns: packet_rcv + delta_ns,
                                worker: receiver_core_id,
                                task_id: 0,
                                index: node_info.index,
                            });
                        }

                        // Check if this slot has received all its packets (stream fully received)
                        let packet_count =
                            shared.slot_packet_counters[node_info.slot].load(Ordering::SeqCst);
                        if packet_count == stream_packets {
                            // Exactly-once semantics: atomically claim completion ownership
                            // Uses swap to ensure only ONE thread marks this stream as complete
                            // This prevents double-counting if multiple threads see the final packet
                            let already_completed = shared.slot_packet_complete[node_info.slot]
                                .swap(true, Ordering::SeqCst);

                            if !already_completed {
                                // DEBUG: Check counter values when all packets received
                                let pending_tasks = shared.slot_pending_tasks[node_info.slot]
                                    .load(Ordering::SeqCst);
                                let pending_cond = shared.slot_pending_cond_tasks[node_info.slot]
                                    .load(Ordering::SeqCst);
                                print_debug(|| {
                                    format!(
                                        "Thread {:?} -- All {} packets received for slot {} stream | pending_tasks={}, pending_cond={}",
                                        thread_id, stream_packets, node_info.slot, pending_tasks, pending_cond
                                    )
                                });

                                // Increment total streams received counter
                                let completed_streams = shared
                                    .streams_receive_counter
                                    .fetch_add(1, Ordering::SeqCst)
                                    + 1;

                                // Check if all expected streams have been received
                                if completed_streams >= shared.max_streams {
                                    println!(
                                        "All {} streams received ({} packets each) - receivers will shutdown",
                                        shared.max_streams, stream_packets
                                    );
                                    // Signal receivers to stop, but NOT resolution threads
                                    shared.receive_finished.store(true, Ordering::SeqCst);
                                }
                            } else {
                                print_debug(|| {
                                    format!(
                                        "Thread {:?} -- Slot {} completion already claimed by another thread",
                                        thread_id, node_info.slot
                                    )
                                });
                            }
                        }
                    }
                }
                should_poll_packets = !shared.receive_finished.load(Ordering::SeqCst);
            }

            // Pull batch from lock-free queue with timeout
            let batch = recv_batch(
                &shared.batch_queue_rx,
                shared.target_batch_size,
                receive_timeout,
            );

            // Check shutdown immediately after blocking call returns
            if shared.shutdown_flag.load(Ordering::SeqCst) {
                println!(
                    "Thread {} detected shutdown after receive, exiting",
                    thread_id
                );
                break;
            }

            // If nothing arrived from network AND scheduler, mark start of wait period.
            // Otherwise, if we previously were waiting, record the idle interval now.
            // Treat "work" as either network activity (packets_received) OR a non-empty batch.
            let has_work = packets_received || !batch.is_empty();
            if !has_work {
                if let Some(start_ns_wait) = wait_start_ns.take() {
                    // Only record if recorder enabled and slot chosen for recording
                    if shared.async_recorder.is_some() {
                        let current_stream = shared.stream_complete_counter.load(Ordering::SeqCst);
                        if should_record_slot(&shared, current_stream) {
                            let end_ns = shared.base_instant.elapsed().as_nanos();
                            let wait_duration = end_ns.saturating_sub(start_ns_wait);

                            // Only submit record if wait time exceeds minimum threshold
                            // This prevents thousands of tiny records for short idle periods
                            if wait_duration >= MIN_WAIT_RECORD_NS {
                                let job_id = shared.job_counter.fetch_add(1, Ordering::SeqCst);
                                submit_record(Record {
                                    slot: thread_slot,
                                    job_id,
                                    start_ns: start_ns_wait,
                                    end_ns,
                                    worker: thread_core,
                                    task_id: IdType::MAX - 2,
                                    index: 0,
                                });
                            } else {
                                // Wait period too short - put the start time back to continue accumulating
                                wait_start_ns = Some(start_ns_wait);
                            }
                        }
                    }
                }
            }

            let empty_batch = batch.is_empty();

            // Process the entire batch
            if !empty_batch {
                let start_ns_batch = shared.base_instant.elapsed().as_nanos();
                let start_proc = if let Some(tb) = &shared.time_buffer {
                    tb.measure_time()
                } else {
                    TimingMethod::Instant(Instant::now())
                };
                Self::process_batch_resolution(
                    &shared,
                    batch,
                    thread_core,
                    thread_id,
                    thread_slot,
                    &cond_indexes,
                    &mut stream_slot_activity,
                    start_ns_batch,
                );
                if let Some(tb) = &shared.time_buffer {
                    let end_proc = tb.measure_time();
                    let dur = tb.measure_duration(start_proc, end_proc);
                    tb.add_task_time(thread_slot, "Batch Resolution", usize::MAX, dur);
                }
            }

            let start_proc = if let Some(tb) = &shared.time_buffer {
                tb.measure_time()
            } else {
                TimingMethod::Instant(Instant::now())
            };
            // Check slots for completion
            Self::check_slots(
                &shared,
                &mut stream_slot_activity,
                thread_id,
                thread_core,
                thread_slot,
                &cond_indexes,
            );
            if let Some(tb) = &shared.time_buffer {
                let end_proc = tb.measure_time();
                let dur = tb.measure_duration(start_proc, end_proc);
                tb.add_task_time(thread_slot, "Slot Check", usize::MAX, dur);
            }
            wait_start_ns = Some(shared.base_instant.elapsed().as_nanos());

            packets_received = false;

            // Check for completion of all streams
            let completed_streams = shared.stream_complete_counter.load(Ordering::SeqCst);

            if completed_streams == shared.max_streams {
                println!(
                    "Thread {} detected all streams completed, exiting resolution loop",
                    thread_id
                );
                break;
            }
        }
    }
}

// Helper Functions
impl SynRt {
    /// Process a batch of completed nodes: store results, update dependencies, schedule successors
    /// Returns true if work was performed (for timing/recording purposes)
    fn process_batch_resolution(
        shared: &Arc<SharedData>,
        batch: Vec<(NodeInfo, CmTypes)>,
        thread_core: usize,
        thread_id: usize,
        thread_slot: usize,
        cond_indexes: &[Vec<usize>],
        stream_slot_activity: &mut HashMap<usize, bool>,
        start_ns: u128,
    ) {
        if batch.is_empty() {
            return;
        }
        let start_time = if let Some(tb) = &shared.time_buffer {
            tb.measure_time()
        } else {
            TimingMethod::Instant(Instant::now())
        };

        // CRITICAL FIX #2: Track which slots are being processed to increment processing_count
        // This prevents completion detection from triggering while we're still processing tasks
        let mut slots_in_batch: std::collections::HashSet<usize> = std::collections::HashSet::new();
        for (node_info, _) in &batch {
            slots_in_batch.insert(node_info.slot);
        }

        // Increment processing_count for all slots in this batch BEFORE any processing
        // This ensures completion check sees processing_count > 0 and waits
        for &slot in &slots_in_batch {
            shared.slot_processing_count[slot].fetch_add(1, Ordering::SeqCst);
        }

        // Store results and decrement atomics
        // This phase must be sequential due to ID function side effects
        let mut nodes_sent_in_slot: HashMap<usize, usize> = HashMap::new();
        let mut nodes_for_successor_processing = Vec::new();

        let mut succesor_updates = Vec::new();
        for (node_info, result) in batch.into_iter() {
            // Mark stream activity FIRST for all nodes (including network nodes id=0)
            // This ensures check_slots() will examine this slot for completion
            stream_slot_activity.insert(node_info.slot, true);

            if node_info.post_node {
                // Store Result - lock-free atomic store
                shared.node_results.set(&node_info, result);
                continue;
            }

            // store result - lock-free atomic store (no contention)
            shared.node_results.set(&node_info, result);

            // Decrement remaining_nodes counter now that this task is confirmed completed
            // Using pre-computed node_id_is_cond flag for lock-free branch
            let node_id_usize = node_info.id as usize;
            let node_id_to_rem_idx = shared.node_id_to_rem[node_id_usize];
            let node_cache_entry = &shared.node_cache[node_id_usize];

            // Lock-free access using pre-computed is_condition flag
            if node_cache_entry.is_condition {
                shared.remaining_cond_nodes[node_info.slot][node_id_to_rem_idx]
                    .fetch_sub(1, Ordering::SeqCst);

                // Phase 1.2: Also decrement slot-wide condition counter
                let prev_cond =
                    shared.slot_pending_cond_tasks[node_info.slot].fetch_sub(1, Ordering::SeqCst);

                // DEBUG: Track condition task completions
                if prev_cond <= 10 || prev_cond % 100 == 0 {
                    print_debug(|| {
                        format!(
                            "COND task completed: slot={}, node_id={} ({}), prev_pending_cond={}, new={}",
                            node_info.slot, node_id_usize, node_cache_entry.name, prev_cond, prev_cond - 1
                        )
                    });
                }
            } else if !node_cache_entry.is_initial {
                shared.remaining_nodes[node_info.slot][node_id_to_rem_idx]
                    .fetch_sub(1, Ordering::SeqCst);

                // Phase 1.2: Also decrement slot-wide task counter for O(1) completion check
                // This maintains synchronization with per-node remaining_nodes atomics
                let prev_tasks =
                    shared.slot_pending_tasks[node_info.slot].fetch_sub(1, Ordering::SeqCst);

                // DEBUG: Track task completions (log frequently for network node, sparsely for others)
                let is_network = node_id_usize == 0;
                if is_network && (prev_tasks <= 10 || prev_tasks % 100 == 0) {
                    print_debug(|| {
                        format!(
                            "NETWORK packet processed: slot={}, prev_pending_tasks={}, new={}",
                            node_info.slot,
                            prev_tasks,
                            prev_tasks - 1
                        )
                    });
                } else if !is_network && (prev_tasks <= 10 || prev_tasks % 50 == 0) {
                    print_debug(|| {
                        format!(
                            "COMPUTE task completed: slot={}, node_id={} ({}), prev_pending_tasks={}, new={}",
                            node_info.slot, node_id_usize, node_cache_entry.name, prev_tasks, prev_tasks - 1
                        )
                    });
                }
            }
            nodes_for_successor_processing.push(node_info.clone());
            succesor_updates.push(collect_successors_for_node(&shared, &node_info))
        }

        // Process dependency updates using pre-collected successor data
        for (idx, node_info) in nodes_for_successor_processing.into_iter().enumerate() {
            let succ_updates = succesor_updates.get(idx).cloned().unwrap_or_default();

            let mut nodes_to_schedule: Vec<NodeInfo> = Vec::new();

            // Batch process dependency decrements using resolution state
            // if not exist, init nodes_sent for slot to 0
            let nodes_sent: &mut usize = nodes_sent_in_slot.entry(node_info.slot).or_insert(0);

            // NEW: Collect unique successor node_ids with their has_cond flag
            // This allows us to call decrease_and_get_ready() once per node instead of once per instance
            use std::collections::HashMap;
            let mut unique_successors: HashMap<usize, bool> = HashMap::new();
            for (_, has_cond, succ_id) in &succ_updates {
                unique_successors.insert(*succ_id as usize, *has_cond);
            }

            // Process each unique successor ONCE using optimized per-node decrements
            for (succ_node_id, has_cond) in unique_successors {
                // Optimization: Skip condition evaluation if all instances already spawned
                if has_cond {
                    let remaining_spawns = shared.cond_instances_to_spawn[node_info.slot]
                        [succ_node_id]
                        .load(Ordering::SeqCst);

                    if remaining_spawns == 0 {
                        // All instances already spawned, skip condition evaluation entirely
                        let succ_cache = &shared.node_cache[succ_node_id];
                        continue; // Skip to next successor
                    }
                }

                // Call the new optimized method that decrements once and returns all ready indices
                let ready_indices = shared
                    .resolution_state
                    .decrease_and_get_ready(node_info.slot, succ_node_id);

                // Schedule all newly ready instances
                for ready_index in ready_indices {
                    let succ_info = NodeInfo::new(
                        succ_node_id as IdType,
                        node_info.slot,
                        ready_index,
                        node_info.index,
                    );

                    if !has_cond {
                        nodes_to_schedule.push(succ_info);
                        *nodes_sent += 1;
                    } else {
                        // Collect condition nodes - will evaluate outside lock
                        let cond_idx = shared.node_cache[succ_node_id].cond_index;
                        let succ_id = succ_info.id as usize;
                        let succ_cache = &shared.node_cache[succ_id];

                        // Check for node-level condition (new format)
                        let condition_passed = if let Some(cond_cache) = &succ_cache.node_condition
                        {
                            let node_cond = shared.graph.nodes[succ_id].condition.as_ref().unwrap();
                            evaluate_node_condition(&shared, &succ_info, cond_cache, node_cond)
                        } else {
                            // Fall back to arg-based condition (old format)
                            conditions_met(&shared, &succ_info, &cond_indexes[cond_idx])
                        };

                        if condition_passed {
                            nodes_to_schedule.push(succ_info.clone());
                            *nodes_sent += 1;

                            // Decrement spawn counter - one less instance to spawn for this node
                            let prev_spawns = shared.cond_instances_to_spawn[node_info.slot]
                                [succ_id]
                                .fetch_sub(1, Ordering::Release);

                            print_debug(|| {
                                format!(
                                    "Condition passed for node {} ({}) index {}: remaining spawns {} -> {}",
                                    succ_id, succ_cache.name, succ_info.index, prev_spawns, prev_spawns - 1
                                )
                            });
                        } else {
                            // Condition failed - restore dependency to prevent zombie state
                            shared.resolution_state.increment_dependency(&succ_info);

                            // Reset sent flag so it can be marked later
                            shared.resolution_state.reset_sent(
                                node_info.slot,
                                succ_info.id as usize,
                                succ_info.index,
                            );
                        }
                    }
                }
            }
            // Schedule all ready nodes collected from this completed node
            Self::preparation(&shared, &nodes_to_schedule, thread_core, thread_slot);
        }

        // Only record timing/metrics when actual work was performed
        if let Some(tb) = &shared.time_buffer {
            let end_time = if let Some(tb) = &shared.time_buffer {
                tb.measure_time()
            } else {
                TimingMethod::Instant(Instant::now())
            };
            let duration = tb.measure_duration(start_time, end_time);
            tb.add_task_time(thread_slot, &format!("Resolution"), usize::MAX, duration);
        }

        // Lock-free recording via per-worker channel
        let current_stream = shared.stream_complete_counter.load(Ordering::SeqCst);
        if shared.async_recorder.is_some() && should_record_slot(&shared, current_stream) {
            let job_id = shared.job_counter.fetch_add(1, Ordering::SeqCst);
            let end_ns = shared.base_instant.elapsed().as_nanos();
            submit_record(Record {
                slot: thread_slot,
                job_id,
                start_ns,
                end_ns,
                worker: thread_core,
                task_id: IdType::MAX,
                // arbitrary index value
                index: 0,
            });
        }

        // CRITICAL FIX #2 (continued): Decrement processing_count AFTER all successor processing
        // This allows completion detection to proceed once this thread has finished all work
        // MUST use SeqCst to ensure ordering: successors processed → count decremented → completion check sees 0
        for &slot in &slots_in_batch {
            shared.slot_processing_count[slot].fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn check_slots(
        shared: &Arc<SharedData>,
        stream_slot_activity: &mut HashMap<usize, bool>,
        thread_id: usize,
        thread_core: usize,
        thread_slot: usize,
        cond_indexes: &[Vec<usize>],
    ) {
        // CRITICAL FIX (Bug #15): Check ALL slots with assigned streams, not just slots in activity map
        // Per-thread activity map can miss completions when threads don't have the completing slot
        let slots_to_check: Vec<usize> = {
            let running_streams = shared.running_streams.read();
            running_streams.iter().map(|(_, slot)| *slot).collect()
        };

        // Clear activity map AFTER getting slots to check (not before)
        // This prevents redundant checking while ensuring we don't miss completions
        stream_slot_activity.clear();

        for proc_slot in slots_to_check {
            // Skip buffering slots - they cannot complete until activated
            if shared.slot_priority_enabled && !is_slot_active(&shared, proc_slot) {
                continue;
            }

            // Check if all nodes in this slot have been processed (O(1) lock-free)
            // Phase 1.2 optimization: Use aggregated counters instead of O(N×F) scan
            // Must check BOTH regular tasks AND condition tasks for complete slot processing
            // CRITICAL FIX #2: Also require processing_count == 0 to ensure no threads are mid-processing
            let pending_regular = shared.slot_pending_tasks[proc_slot].load(Ordering::SeqCst);
            let pending_cond = shared.slot_pending_cond_tasks[proc_slot].load(Ordering::SeqCst);
            let processing_count = shared.slot_processing_count[proc_slot].load(Ordering::SeqCst);

            let all_nodes_processed =
                pending_regular == 0 && pending_cond == 0 && processing_count == 0;

            if all_nodes_processed {
                // Atomically claim ownership of this slot's completion.
                // try_complete_slot checks and marks in a single critical section,
                // so exactly one thread wins when multiple threads race here.
                // This replaces the previous is_slot_completed() + mark_slot_completed()
                // pair which had a TOCTOU window: between the separate check and mark,
                // another thread could complete the full reinit+restart cycle and unmark
                // the slot, causing the losing thread to double-complete and corrupt state.
                if !shared.resolution_state.try_complete_slot(proc_slot) {
                    continue; // Another thread already owns this completion
                }

                print_debug(|| {
                    format!(
                        "Thread {:?} -- Completed iteration at slot {}",
                        thread_id, proc_slot
                    )
                });

                // Reset dependency_map for this slot via resolution state
                shared
                    .resolution_state
                    .reinit_dependencies(&shared.graph.nodes, proc_slot);

                // Reset packet completion flag for the next stream
                // Allows completion detection to work for the new iteration
                shared.slot_packet_complete[proc_slot].store(false, Ordering::SeqCst);

                // Reset per-slot packet counter for the next stream
                // This ensures the network node index starts at 0 for the new stream
                shared.slot_packet_counters[proc_slot].store(0, Ordering::SeqCst);

                // Reinit remaining_nodes for this slot using pre-computed init values (lock-free)
                let slot_remaining = &shared.remaining_nodes[proc_slot];
                let slot_init = &shared.remaining_init[proc_slot];
                for (node_rem_idx, init_val) in slot_init.iter().enumerate() {
                    slot_remaining[node_rem_idx].store(*init_val, Ordering::SeqCst);
                }

                // Phase 1.2: Reinit slot-wide counters for O(1) completion check
                let total_tasks: usize = slot_init.iter().sum();
                shared.slot_pending_tasks[proc_slot].store(total_tasks, Ordering::SeqCst);

                // DEBUG: Track counter reset values
                print_debug(|| {
                    format!(
                        "RESET slot {} counters: slot_pending_tasks={}, slot_pending_cond_tasks will be set next",
                        proc_slot, total_tasks
                    )
                });

                // Reinit remaining_cond_nodes for this slot (reset to factor values)
                let slot_cond_remaining = &shared.remaining_cond_nodes[proc_slot];
                let mut total_cond_tasks = 0;
                for node_id in 0..shared.graph.nodes.len() {
                    if shared.node_id_is_cond[node_id] {
                        let node_id_to_rem_idx = shared.node_id_to_rem[node_id];
                        let factor = shared.node_cache[node_id].factor;
                        slot_cond_remaining[node_id_to_rem_idx].store(factor, Ordering::SeqCst);
                        total_cond_tasks += factor;
                    }
                }

                // Phase 1.2: Reinit slot-wide condition counter
                shared.slot_pending_cond_tasks[proc_slot].store(total_cond_tasks, Ordering::SeqCst);

                // DEBUG: Track condition counter reset
                print_debug(|| {
                    format!(
                        "RESET slot {} cond counter: slot_pending_cond_tasks={}",
                        proc_slot, total_cond_tasks
                    )
                });

                // CRITICAL FIX #2 (continued): Reset in-flight processing counter for next stream
                // Must be 0 before new stream starts processing, otherwise completion detection will wait forever
                shared.slot_processing_count[proc_slot].store(0, Ordering::SeqCst);

                // Phase 1.3: Reset condition spawn counters for next stream
                // Each condition node starts with its full factor count available to spawn
                for (node_idx, node_cache_entry) in shared.node_cache.iter().enumerate() {
                    if node_cache_entry.is_condition {
                        shared.cond_instances_to_spawn[proc_slot][node_idx]
                            .store(node_cache_entry.factor, Ordering::SeqCst);
                    }
                }

                // Clear nodes_sent_to_queue for this slot - MUST happen before new nodes spawn
                shared.resolution_state.clear_slot_sent_flags(proc_slot);

                // CRITICAL FIX (Bug #21): Unmark slot completion flag so it can complete again
                // for the next stream. This MUST happen after state reset but before new stream
                // is assigned. Without this, when slot_priority is enabled, the slot remains
                // marked as completed and try_complete_slot() returns false for all subsequent
                // streams, causing them to hang forever.
                shared.resolution_state.unmark_slot_completed(proc_slot);

                print_debug(|| {
                    format!(
                        "Cleared all state for slot {} before spawning new stream",
                        proc_slot
                    )
                });

                // Check if we should start a new iteration and release the slot
                // IMPORTANT: Must call this BEFORE activate_next_slot() so the slot is released
                // and activate_next_slot() sees available_slots[proc_slot] == usize::MAX
                let can_restart = process_slot_completion(&shared, proc_slot);
                stream_slot_activity.remove(&proc_slot);

                // In slot-priority mode: rotate active slot and activate next buffered slot
                // This must happen AFTER process_slot_completion() so the completing slot is released
                let activated_slot_info = if shared.slot_priority_enabled {
                    activate_next_slot(&shared, Some(proc_slot))
                } else {
                    None
                };

                // Track whether we activated a buffering slot (for restart decision below)
                let _buffering_slot_was_activated = activated_slot_info.is_some();

                // Process activated slot: spawn initial nodes AND process buffered packets
                // CRITICAL: Initial nodes must be spawned BEFORE processing buffered packets
                // to ensure the compute graph starts for the activated slot
                if let Some((activated_slot, buffered_batch)) = activated_slot_info {
                    print_debug(|| {
                        format!(
                            "Activated slot {} from Buffering to Active (completing slot: {})",
                            activated_slot, proc_slot
                        )
                    });

                    // CRITICAL FIX: Spawn initial compute nodes for the activated slot FIRST
                    // This starts the compute graph for the newly activated stream
                    let activated_compute_nodes = initial_nodes(&shared, vec![activated_slot]);

                    print_debug(|| {
                        format!(
                            "Spawning {} initial nodes for activated slot {}",
                            activated_compute_nodes.len(),
                            activated_slot
                        )
                    });
                    if !activated_compute_nodes.is_empty() {
                        Self::preparation(
                            &shared,
                            &activated_compute_nodes,
                            thread_core,
                            thread_slot,
                        );
                    }

                    // Then process buffered network packets
                    // These are network packets that arrived while the slot was buffering
                    if !buffered_batch.is_empty() {
                        print_debug(|| {
                            format!(
                                "Processing {} buffered network packets for activated slot {}",
                                buffered_batch.len(),
                                activated_slot
                            )
                        });
                        let start_ns_batch = shared.base_instant.elapsed().as_nanos();
                        Self::process_batch_resolution(
                            &shared,
                            buffered_batch,
                            thread_core,
                            thread_id,
                            thread_slot,
                            &cond_indexes,
                            stream_slot_activity,
                            start_ns_batch,
                        );
                    }
                }

                // CRITICAL: When slot-priority is enabled, only ONE slot should be Active at a time
                // Never restart the completing slot in slot-priority mode; wait for packet arrival
                // to trigger assignment via assign_stream_to_available_slot
                let should_restart_completing_slot = can_restart && !shared.slot_priority_enabled;

                if should_restart_completing_slot {
                    // Spawn initial compute nodes for the restarting slot
                    // (network nodes are handled by receivers, not scheduled)
                    let compute_nodes = initial_nodes(&shared, vec![proc_slot]);

                    print_debug(|| {
                        format!(
                            "Spawned {} initial nodes for restarting slot {}",
                            compute_nodes.len(),
                            proc_slot
                        )
                    });

                    if !compute_nodes.is_empty() {
                        Self::preparation(&shared, &compute_nodes, thread_core, thread_slot);
                    }
                }
            }
        }
    }

    fn schedule_post_nodes(&mut self) {
        let nodes = &self.shared.graph.post_nodes;
        if let Some(post_nodes) = nodes {
            let stream_use = self.shared.slots + self.shared.system_threads; // Use last available slot for post-nodes
            for post_node in post_nodes {
                let mut post_schedule: Vec<NodeInfo> = Vec::new();
                let mut pre_build_args: Vec<Option<Vec<CmTypes>>> = Vec::new();
                let mut functions: Vec<Option<CmPtr>> = Vec::new();
                for index in 0..post_node.factor {
                    let mut node_info = NodeInfo::new(post_node.id, stream_use, index, 0);
                    node_info.set_post_node(true);

                    let arg_vec =
                        parse_args(&self.shared, &post_node.args, index, stream_use, 0, None);

                    let func: Option<CmPtr> = post_node.func_ptr;
                    pre_build_args.push(Some(arg_vec));
                    functions.push(func);
                    post_schedule.push(node_info);
                }
                send_to_scheduler(&self.shared, &post_schedule, &pre_build_args, &functions);
                print_debug(|| format!("Added post node: {}", post_node.name));
                // Wait until all are completed by checking node_results
                let mut completed_count = 0;
                while completed_count < post_node.factor {
                    sleep(Duration::from_millis(10));
                    completed_count = 0;
                    // Lock-free check - no RwLock needed
                    for i in 0..post_node.factor {
                        let node_info = NodeInfo::new(post_node.id, stream_use, i, 0);
                        if self.shared.node_results.result_exists(&node_info) {
                            completed_count += 1;
                        }
                    }
                }
            }
            print_debug(|| "All post-nodes completed".to_string());
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

    pub fn print_statistics(
        &self,
        bench_name: &str,
        out_file: Option<&str>,
        exclude_streams: usize,
    ) {
        if let Some(tb) = &self.shared.time_buffer {
            tb.print_stats(bench_name, out_file, exclude_streams);
        }
    }

    pub fn write_record(&self, path: &str) {
        self.shared.scheduler.write_record(path);
        self.write_runtime_record(path);
    }

    pub fn write_runtime_record(&self, _path: &str) {
        if let Some(_rec) = &self.shared.async_recorder {
            // The AsyncRecorder handles all record writing via write_to_csv
            // This method is a no-op since AsyncRecorder already exported everything
            println!("Runtime: async_recorder records already written via scheduler");
        } else {
            println!("Runtime: recorder not enabled");
        }
    }
}

fn prepare_network_infrastructure(
    graph: &Graph,
) -> (
    Vec<NetworkSocket>,
    BatchSender<PacketMessage>,
    BatchReceiver<PacketMessage>,
    Vec<AtomicUsize>,
) {
    let (packet_sender, packet_receiver) = crossbeam_channel::unbounded();
    if let Some(config_spec) = graph.network_config() {
        let num_sockets = config_spec.num_sockets;

        let receiver_sockets =
            bind_udp_socket_range(&config_spec.address, config_spec.start_port, num_sockets);

        let packet_drop_counters = (0..num_sockets).map(|_| AtomicUsize::new(0)).collect();

        (
            receiver_sockets,
            packet_sender,
            packet_receiver,
            packet_drop_counters,
        )
    } else {
        (Vec::new(), packet_sender, packet_receiver, Vec::new())
    }
}
