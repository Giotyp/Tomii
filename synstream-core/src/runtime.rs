use core_affinity;
use crossbeam_channel::{Receiver, Sender};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{sleep, spawn, JoinHandle};
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
use synstream_types::*;

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
        available_stream_slots: Arc<RwLock<Vec<usize>>>, // Shared with scheduler for recording filter
        // Phase 2: Accept shared batch infrastructure from caller
        batch_buffer: Arc<Mutex<Vec<(NodeInfo, CmTypes)>>>,
        batch_last_sent: Arc<Mutex<Instant>>,
        batching_size: usize,
        flush_notify_tx: crossbeam_channel::Sender<()>,
        flusher_shutdown: Arc<AtomicUsize>,
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

        // Phase 2: Batch infrastructure is now passed in from caller (no duplication)

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
                .map(|atomic| atomic.load(Ordering::Relaxed))
                .sum();
            slot_pending_cond_tasks.push(AtomicUsize::new(total_cond_tasks));
        }

        // Initialize slot states for priority processing
        let slot_states = Arc::new(RwLock::new(Vec::new()));
        {
            let mut states = slot_states.write();
            for slot_id in 0..slots {
                if slot_priority_enabled && slot_id == 0 {
                    // Only slot 0 is active initially
                    states.push(SlotState::Active);
                } else if slot_priority_enabled {
                    // All other slots start buffering
                    states.push(SlotState::Buffering);
                } else {
                    // When disabled, all slots are active
                    states.push(SlotState::Active);
                }
            }
        }

        // Initialize per-slot buffering queues
        let slot_buffers = Arc::new(RwLock::new(vec![Vec::new(); slots]));

        let (receiver_sockets, packet_senders, packet_receivers, packet_drop_counters) =
            prepare_network_infrastructure(app_graph);

        let shared = Arc::new(SharedData {
            graph: app_graph.clone(),
            slots,
            max_streams,
            max_runtime,
            node_cache,
            node_results: Arc::new(RwLock::new(VecMap::new(CmTypes::Init))),
            stream_complete_counter: Arc::new(AtomicUsize::new(0)),
            available_stream_slots,
            time_buffer,
            scheduler: Arc::new(scheduler),
            completed_tx: Arc::new(RwLock::new(None)),
            system_threads,
            receiver_threads,
            workers,
            core_offset,
            receiver_core_offset,
            record_stream,
            async_recorder,
            base_instant: Arc::new(base_instant),
            job_counter,
            resolution_state,
            remaining_nodes: Arc::new(remaining_nodes),
            remaining_cond_nodes: Arc::new(remaining_cond_nodes),
            node_id_to_rem: Arc::new(node_id_to_rem),
            node_id_is_cond: Arc::new(node_id_is_cond),
            remaining_init: Arc::new(remaining_init),
            initial_prep_done: Arc::new(AtomicUsize::new(0)),
            slot_pending_tasks: Arc::new(slot_pending_tasks),
            slot_pending_cond_tasks: Arc::new(slot_pending_cond_tasks),
            batch_buffer,
            batch_last_sent,
            batching_size,
            flush_notify_tx: flush_notify_tx.clone(),
            flusher_shutdown,
            slot_states,
            slot_priority_enabled,
            slot_buffers,
            // Network fields (empty - will be initialized in run() if network_config present)
            packet_senders,
            packet_receivers,
            receiver_sockets,
            packet_drop_counters,
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            stream_packet_counter: Arc::new(AtomicUsize::new(0)),
            streams_receive_counter: Arc::new(AtomicUsize::new(0)),
        });

        SynRt { shared }
    }

    pub fn base_instant(&self) -> Instant {
        *self.shared.base_instant
    }

    pub fn run(&mut self) {
        // create completed channel for batched results
        let (completed_tx, completed_rx) =
            crossbeam_channel::unbounded::<Vec<(NodeInfo, CmTypes)>>();

        // Set completed_tx in SharedData
        {
            let mut tx_lock = self.shared.completed_tx.write();
            *tx_lock = Some(completed_tx.clone());
        }

        // Set completed_tx in the scheduler and start the flusher thread
        self.shared.scheduler.set_completed_tx(completed_tx);

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

                    let handle = spawn(move || {
                        single_socket_receiver_loop(shared_clone, socket_id, core_id);
                    });
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

                    let handle = spawn(move || {
                        multi_socket_receiver_loop(shared_clone, thread_id, socket_range, core_id);
                    });
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
            let completed_rx_clone = completed_rx.clone();
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

                Self::resolution(
                    shared_for_resolution,
                    completed_rx_clone,
                    thread_core,
                    thread_id,
                    thread_slot,
                );
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
                    // set exit signal
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

        // Shutdown and flush the flusher thread in the scheduler
        self.shared.scheduler.shutdown_flusher();

        // Drop all senders to signal resolution threads to exit
        // This will close the channel and unblock the resolution threads
        {
            let mut tx_lock = self.shared.completed_tx.write();
            *tx_lock = None; // Drop the sender in SharedData
        }
        {
            let tx_ref = self.shared.scheduler.get_completed_tx_ref();
            let mut tx_lock = tx_ref.lock();
            *tx_lock = None; // Drop the sender in scheduler
        }

        // Wait for all resolution threads to finish (they will unblock when channel closes)
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
                    let drops = self.shared.packet_drop_counters[socket_id].load(Ordering::Relaxed);
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
        use_network_scheduler: bool,
    ) {
        let start_time = if let Some(tb) = &shared.time_buffer {
            tb.measure_time()
        } else {
            TimingMethod::Instant(Instant::now())
        };
        let start_ns = shared.base_instant.elapsed().as_nanos();
        print_debug(|| format!("Preparing {:?} nodes", nodes_to_schedule.len()));

        // Schedule Task - args will be built in the worker thread
        let pre_built_args_vec = vec![None; nodes_to_schedule.len()];
        let custom_func_vec = vec![None; nodes_to_schedule.len()];
        send_to_scheduler(
            &shared,
            nodes_to_schedule,
            &pre_built_args_vec,
            &custom_func_vec,
            use_network_scheduler,
        );

        if let Some(tb) = &shared.time_buffer {
            let end_time = tb.measure_time();
            let duration = tb.measure_duration(start_time, end_time);
            tb.add_task_time(thread_slot, "Preparation Thread", usize::MAX, duration);
        }

        // Lock-free recording via per-worker channel
        let current_stream = shared.stream_complete_counter.load(Ordering::Relaxed);
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
        completed_rx: Receiver<Vec<(NodeInfo, CmTypes)>>,
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

                // Compute nodes (nx=false) follow slot-priority - only active slots
                // Network nodes are handled by dedicated receiver threads (see inject_network_packet)
                let active_slots: Vec<usize> = (0..shared.slots)
                    .filter(|&slot| is_slot_active(&shared, slot))
                    .collect();

                if shared.slot_priority_enabled {
                    print_debug(|| {
                        format!(
                            "Slot-Priority: Starting compute nodes for active slots: {:?}",
                            active_slots
                        )
                    });
                }

                let compute_nodes = initial_nodes(&shared, active_slots);

                // Send compute nodes to regular scheduler (only active slots)
                if !compute_nodes.is_empty() {
                    Self::preparation(&shared, &compute_nodes, thread_core, thread_slot, false);
                }
            }
        }

        // Network nodes are handled by dedicated receiver threads, not scheduled
        // (see inject_network_packet implementation)

        // prefetch cond indexes for efficiency
        let cond_indexes = shared.graph.get_condition_indexes();

        // Persistent completion tracking across all batches for this stream
        let mut stream_slot_activity: HashMap<usize, bool> = HashMap::new();

        // Packet Process Function
        let network_config_opt = shared.graph.network_config();
        // Track start of idle/wait periods so we can record waiting time
        let mut wait_start_ns: Option<u128> = None;

        let mut receive_finished: bool = false;
        let mut first_packet_received: bool = false;

        // Process completed nodes with dynamic batching from scheduler
        loop {
            // PRIORITY 1: Poll network packets (low-latency path)
            // Only poll if network_config is present and receivers were spawned
            let mut network_received_nodes = Vec::new();
            if let Some(network_config) = network_config_opt.as_ref() {
                let stream_packets = network_config.stream_packets;
                if !receive_finished && !shared.packet_receivers.is_empty() {
                    let packet_process_func = network_config.extract_packet_func.unwrap();

                    // Block until first packet arrives
                    if !first_packet_received {
                        for receiver_id in 0..shared.packet_receivers.len() {
                            if let Ok(packet_msg) = shared.packet_receivers[receiver_id].recv() {
                                first_packet_received = true;
                                print_debug(|| {
                                    "First packet received, switching to non-blocking polling"
                                        .to_string()
                                });
                                // Process this first packet
                                let received_bytes_cm =
                                    CmTypes::from_any(packet_msg.packet_bytes.clone());
                                let packet_cm = packet_process_func(vec![received_bytes_cm]);
                                let counter = shared.stream_packet_counter.clone();
                                let packet_index = counter.fetch_add(1, Ordering::Relaxed);
                                let node_info = NodeInfo::new(0, 0, packet_index, 0);
                                network_received_nodes.push((node_info, packet_cm));

                                if shared.async_recorder.is_some() {
                                    let receiver_slot = shared.slots + shared.system_threads;
                                    let job_id = shared.job_counter.fetch_add(1, Ordering::SeqCst);
                                    let packet_rcv = packet_msg
                                        .timestamp
                                        .duration_since(*shared.base_instant)
                                        .as_nanos();
                                    let delta_ns = 10000u128;
                                    submit_record(Record {
                                        slot: receiver_slot,
                                        job_id,
                                        start_ns: packet_rcv,
                                        end_ns: packet_rcv + delta_ns,
                                        worker: packet_msg.receiver_core_id,
                                        task_id: 0,
                                        index: packet_index,
                                    });
                                }

                                if packet_index + 1 == stream_packets {
                                    shared.stream_packet_counter.store(0, Ordering::Relaxed);
                                    print_debug(|| {
                                        format!(
                                            "All {} packets for stream received",
                                            stream_packets
                                        )
                                    });
                                    let completed_streams = shared
                                        .streams_receive_counter
                                        .fetch_add(1, Ordering::Relaxed)
                                        + 1;
                                    if completed_streams >= shared.max_streams {
                                        println!(
                                            "All {} streams received ({} packets each) - receivers will shutdown",
                                            shared.max_streams, stream_packets
                                        );
                                        shared.shutdown_flag.store(true, Ordering::SeqCst);
                                        receive_finished = true;
                                    }
                                }
                                break;
                            }
                        }
                    }

                    // Non-blocking poll - process all available packets
                    for receiver_id in 0..shared.packet_receivers.len() {
                        while let Ok(packet_msg) = shared.packet_receivers[receiver_id].try_recv() {
                            let received_bytes_cm =
                                CmTypes::from_any(packet_msg.packet_bytes.clone());

                            let packet_cm = packet_process_func(vec![received_bytes_cm]);
                            let counter = shared.stream_packet_counter.clone();
                            let packet_index = counter.fetch_add(1, Ordering::Relaxed);
                            let node_info = NodeInfo::new(0, 0, packet_index, 0); // assuming node_id 0 for network input
                            network_received_nodes.push((node_info, packet_cm));

                            // Submit record for packet reception
                            // Note: Network packets are recorded separately as they arrive before
                            // stream/slot assignment. For --record-stream filtering, network packets
                            // are always recorded since filtering happens at compute task level.
                            if shared.async_recorder.is_some() {
                                let receiver_slot = shared.slots + shared.system_threads;
                                let job_id = shared.job_counter.fetch_add(1, Ordering::SeqCst);

                                // Convert rdtsc timestamp to nanoseconds relative to base_instant
                                // Assuming packet_msg.timestamp is already in rdtsc units or nanos
                                let packet_rcv = packet_msg
                                    .timestamp
                                    .duration_since(*shared.base_instant)
                                    .as_nanos();
                                let delta_ns = 10000u128; // Small delta to make it visible in graphs

                                submit_record(Record {
                                    slot: receiver_slot,
                                    job_id,
                                    start_ns: packet_rcv,
                                    end_ns: packet_rcv + delta_ns,
                                    worker: packet_msg.receiver_core_id,
                                    task_id: 0,
                                    index: packet_index,
                                });
                            }

                            if packet_index + 1 == stream_packets {
                                // Reset counter for next stream
                                shared.stream_packet_counter.store(0, Ordering::Relaxed);
                                print_debug(|| {
                                    format!("All {} packets for stream received", stream_packets)
                                });
                                // Increase total stream receive counter
                                let completed_streams = shared
                                    .streams_receive_counter
                                    .fetch_add(1, Ordering::Relaxed)
                                    + 1; // +1 because fetch_add returns old value

                                // Log when all expected streams have been received
                                // Note: Receiver threads will continue running until main loop exits
                                if completed_streams >= shared.max_streams {
                                    println!(
                                        "All {} streams received ({} packets each) - receivers will shutdown",
                                        shared.max_streams, stream_packets
                                    );
                                    // Signal shutdown
                                    shared.shutdown_flag.store(true, Ordering::SeqCst);
                                    receive_finished = true;
                                }
                            }
                        }
                    }
                }
            }

            // Track if any work was performed this iteration (network packets, batch processing, etc.)
            let mut work_performed = false;

            // PRIORITY 2: Receive batch from scheduler (non-blocking)
            let mut batch = match completed_rx.try_recv() {
                Ok(batch_from_scheduler) => batch_from_scheduler,
                Err(crossbeam_channel::TryRecvError::Empty) => Vec::new(), // No messages yet, continue
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    println!(
                        "Resolution thread {} detected channel closure, exiting...",
                        thread_id
                    );
                    break; // Exit the resolution loop
                }
            };

            // Combine network received nodes with scheduled batch
            // Place network nodes at the front to prioritize processing
            let mut empty_network: bool = true;
            if !network_received_nodes.is_empty() {
                empty_network = false;
                let network_count = network_received_nodes.len();
                batch.splice(0..0, network_received_nodes);
                print_debug(|| format!("Injected {:?} network nodes to batch", network_count));
            }

            // If nothing arrived from network or scheduler, mark start of wait period.
            // Otherwise, if we previously were waiting, record the idle interval now.
            if empty_network && batch.is_empty() {
                if wait_start_ns.is_none() {
                    wait_start_ns = Some(shared.base_instant.elapsed().as_nanos());
                }
            } else {
                if let Some(start_ns_wait) = wait_start_ns.take() {
                    // Only record if recorder enabled and slot chosen for recording
                    if shared.async_recorder.is_some() {
                        let current_stream = shared.stream_complete_counter.load(Ordering::Relaxed);
                        if should_record_slot(&shared, current_stream) {
                            let end_ns = shared.base_instant.elapsed().as_nanos();
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
                        }
                    }
                }
            }

            // Local tracking for THIS batch only
            let mut nodes_sent_in_slot: HashMap<usize, usize> = HashMap::new();

            let start_ns = shared.base_instant.elapsed().as_nanos();
            let start_time = if let Some(tb) = &shared.time_buffer {
                tb.measure_time()
            } else {
                TimingMethod::Instant(Instant::now())
            };

            // Process the entire batch
            if !batch.is_empty() {
                work_performed = true; // Processing nodes from scheduler or network
            }

            // PHASE 2A: Sequential Phase 1 - Store results and decrement atomics
            // This phase must be sequential due to ID function side effects
            let mut nodes_for_successor_processing = Vec::new();

            for (mut node_info, result) in batch.into_iter() {
                print_debug(|| {
                    format!(
                        "Thread {:?} -- Processing Completed {:?}",
                        thread_id, node_info
                    )
                });

                if node_info.post_node {
                    // Store Result
                    shared.node_results.write().set(&node_info, result);
                    continue;
                }

                // Get Id function and validate slot
                let new_stream_opt = process_id_function(&shared, &node_info, &result);
                if let Some(new_stream) = new_stream_opt {
                    // Assign streams to an available stream slot
                    node_info.slot = assign_stream_to_available_slot(&shared, new_stream);
                } else {
                    // ID function failed, skip processing this node
                    print_debug(|| {
                        format!(
                            "Thread {:?} -- Skipping further processing of node {:?} due to ID function failure",
                            thread_id, node_info
                        )
                    });
                    continue;
                }

                // store result - single lock acquisition (consumes result)
                shared.node_results.write().set(&node_info, result);

                // Mark this slot as having activity (for persistent completion tracking)
                stream_slot_activity.insert(node_info.slot, true);

                // Decrement remaining_nodes counter now that this task is confirmed completed
                // Using pre-computed node_id_is_cond flag for lock-free branch
                let node_id_usize = node_info.id as usize;
                let node_id_to_rem_idx = shared.node_id_to_rem[node_id_usize];
                let node_cache_entry = &shared.node_cache[node_id_usize];

                // Lock-free access using pre-computed is_condition flag
                if node_cache_entry.is_condition {
                    shared.remaining_cond_nodes[node_info.slot][node_id_to_rem_idx]
                        .fetch_sub(1, Ordering::Release);

                    // Phase 1.2: Also decrement slot-wide condition counter
                    shared.slot_pending_cond_tasks[node_info.slot].fetch_sub(1, Ordering::Release);
                } else if !node_cache_entry.is_initial {
                    shared.remaining_nodes[node_info.slot][node_id_to_rem_idx]
                        .fetch_sub(1, Ordering::Release);

                    // Phase 1.2: Also decrement slot-wide task counter for O(1) completion check
                    // This maintains synchronization with per-node remaining_nodes atomics
                    shared.slot_pending_tasks[node_info.slot].fetch_sub(1, Ordering::Release);
                }

                // Collect node for successor processing (Phase 2A: Parallel Phase 2)
                // Note: result has been consumed by .set(), so we only store node_info
                nodes_for_successor_processing.push(node_info);
            }

            // PHASE 2A: Parallel Phase 2 - Collect successor information in parallel
            // This phase only reads from immutable graph/cache structures, no side effects
            // We need to convert nodes back to (node_info, dummy_result) for the parallel collection function
            // Note: The result value is not used in successor collection, only node_info matters
            let batch_for_parallel: Vec<(NodeInfo, CmTypes)> = nodes_for_successor_processing
                .iter()
                .map(|node_info| (node_info.clone(), CmTypes::Bool(false)))
                .collect();

            let all_successor_updates = if !batch_for_parallel.is_empty() {
                collect_batch_successors(&shared, &batch_for_parallel)
            } else {
                Vec::new()
            };

            // PHASE 2A: Sequential Phase 3 - Process dependency updates using pre-collected successor data
            for (idx, node_info) in nodes_for_successor_processing.into_iter().enumerate() {
                let succ_updates = all_successor_updates.get(idx).cloned().unwrap_or_default();

                let mut nodes_to_schedule: Vec<NodeInfo> = Vec::new();

                print_debug(|| {
                    format!(
                        "Thread {:?} -- Successors of node {:?}: {:?}",
                        thread_id,
                        node_info,
                        succ_updates.len()
                    )
                });

                // Batch process dependency decrements using resolution state
                // Collect condition nodes to check OUTSIDE the lock to avoid nested locking
                let mut cond_nodes_to_check: Vec<(NodeInfo, usize)> = Vec::new();

                // if not exist, init nodes_sent for slot to 0
                let nodes_sent: &mut usize = nodes_sent_in_slot.entry(node_info.slot).or_insert(0);

                for (succ_info, has_cond, succ_id) in succ_updates {
                    if let Some(dep) = shared.resolution_state.decrease_dependency(&succ_info) {
                        if dep == 0 {
                            // Try to atomically claim the slot via resolution state
                            if shared.resolution_state.try_mark_sent(
                                node_info.slot,
                                succ_id as usize,
                                succ_info.index,
                            ) {
                                if !has_cond {
                                    nodes_to_schedule.push(succ_info);
                                    *nodes_sent += 1;
                                } else {
                                    // Collect condition nodes - will evaluate outside lock
                                    let cond_idx = shared.node_cache[succ_id as usize].cond_index;
                                    cond_nodes_to_check.push((succ_info, cond_idx));
                                }
                            }
                        }
                    }
                }

                // Evaluate conditions OUTSIDE the locks - conditions_met takes node_results.read()
                for (succ_info, cond_idx) in cond_nodes_to_check {
                    if conditions_met(&shared, &succ_info, &cond_indexes[cond_idx]) {
                        nodes_to_schedule.push(succ_info.clone());
                        *nodes_sent += 1;
                    } else {
                        // Condition failed - restore dependency to prevent zombie state
                        shared.resolution_state.increment_dependency(&succ_info);

                        // Reset sent flag so it can be marked later
                        shared.resolution_state.reset_sent(
                            node_info.slot,
                            succ_info.id as usize,
                            succ_info.index,
                        );

                        print_debug(|| {
                            format!(
                                "Condition failed for node {:?}[{}] - restored dependency",
                                succ_info.id, succ_info.index
                            )
                        });
                    }
                }

                // Separate nodes by slot state: active slots → scheduler, buffering slots → buffer
                let mut active_nodes = Vec::new();
                let mut buffered_by_slot: Vec<Vec<NodeInfo>> = vec![Vec::new(); shared.slots];

                for node_info in nodes_to_schedule {
                    if is_slot_active(&shared, node_info.slot) {
                        active_nodes.push(node_info);
                    } else {
                        buffered_by_slot[node_info.slot].push(node_info);
                    }
                }

                if !active_nodes.is_empty() {
                    // Increment nodes_sent for each active slot
                    for node_info in &active_nodes {
                        *nodes_sent_in_slot.entry(node_info.slot).or_insert(0) += 1;
                    }
                    Self::preparation(&shared, &active_nodes, thread_core, thread_slot, false);
                }

                // Buffer nodes from inactive slots
                if !buffered_by_slot.is_empty() {
                    let mut slot_buffers = shared.slot_buffers.write();
                    for (slot, nodes) in buffered_by_slot.iter().enumerate() {
                        if nodes.is_empty() {
                            continue;
                        }
                        slot_buffers[slot].extend(nodes.clone());
                        // Mark that this slot had activity (for completion check) - both persistent and per-batch
                        nodes_sent_in_slot.entry(slot).or_insert(0);
                        stream_slot_activity.insert(slot, true);

                        print_debug(|| {
                            format!(
                                "Thread {:?} -- Buffered {:?} nodes for slot {:?} ",
                                thread_id,
                                nodes.len(),
                                slot
                            )
                        });
                    }
                }
            } // End of batch processing loop

            // Check for stream completion - iterate over ALL slots with activity, not just current batch
            // Collect slots first to avoid borrow issues when mutating the map
            let slots_to_check: Vec<usize> = stream_slot_activity.keys().copied().collect();

            for proc_slot in slots_to_check {
                print_debug(|| {
                    format!(
                        "Checking slot {} for completion (completed={}, active={})",
                        proc_slot,
                        shared.resolution_state.is_slot_completed(proc_slot),
                        if shared.slot_priority_enabled {
                            is_slot_active(&shared, proc_slot).to_string()
                        } else {
                            "N/A".to_string()
                        }
                    )
                });

                // Skip slots already marked as completed
                if shared.resolution_state.is_slot_completed(proc_slot) {
                    continue;
                }

                // Skip buffering slots - they cannot complete until activated
                if shared.slot_priority_enabled && !is_slot_active(&shared, proc_slot) {
                    continue;
                }

                // Check if all nodes in this slot have been processed (O(1) lock-free)
                // Phase 1.2 optimization: Use aggregated counter instead of O(N×F) scan
                let all_nodes_processed =
                    shared.slot_pending_tasks[proc_slot].load(Ordering::Acquire) == 0;

                print_debug(|| {
                    let pending = shared.slot_pending_tasks[proc_slot].load(Ordering::Acquire);
                    format!(
                        "Slot {} pending_tasks: {}, all_processed={}",
                        proc_slot, pending, all_nodes_processed
                    )
                });

                if all_nodes_processed {
                    print_debug(|| {
                        format!(
                            "Thread {:?} -- Completed iteration at slot {}",
                            thread_id, proc_slot
                        )
                    });

                    // Mark this slot as completed
                    shared.resolution_state.mark_slot_completed(proc_slot);

                    // CRITICAL: Reset ALL state BEFORE checking process_slot_completion
                    // This prevents race conditions where new nodes complete before state is clean
                    print_debug(|| {
                        format!(
                            "Resetting all state for slot {} before starting new iteration",
                            proc_slot
                        )
                    });

                    // Reset dependency_map for this slot via resolution state
                    shared
                        .resolution_state
                        .reinit_dependencies(&shared.graph.nodes, proc_slot);

                    // Reinit remaining_nodes for this slot using pre-computed init values (lock-free)
                    let slot_remaining = &shared.remaining_nodes[proc_slot];
                    let slot_init = &shared.remaining_init[proc_slot];
                    for (node_rem_idx, init_val) in slot_init.iter().enumerate() {
                        slot_remaining[node_rem_idx].store(*init_val, Ordering::Release);
                    }

                    // Phase 1.2: Reinit slot-wide counters for O(1) completion check
                    let total_tasks: usize = slot_init.iter().sum();
                    shared.slot_pending_tasks[proc_slot].store(total_tasks, Ordering::Release);

                    // Reinit remaining_cond_nodes for this slot (reset to factor values)
                    let slot_cond_remaining = &shared.remaining_cond_nodes[proc_slot];
                    let mut total_cond_tasks = 0;
                    for node_id in 0..shared.graph.nodes.len() {
                        if shared.node_id_is_cond[node_id] {
                            let node_id_to_rem_idx = shared.node_id_to_rem[node_id];
                            let factor = shared.node_cache[node_id].factor;
                            slot_cond_remaining[node_id_to_rem_idx]
                                .store(factor, Ordering::Release);
                            total_cond_tasks += factor;
                        }
                    }

                    // Phase 1.2: Reinit slot-wide condition counter
                    shared.slot_pending_cond_tasks[proc_slot]
                        .store(total_cond_tasks, Ordering::Release);

                    // Clear nodes_sent_to_queue for this slot - MUST happen before new nodes spawn
                    shared.resolution_state.clear_slot_sent_flags(proc_slot);

                    print_debug(|| {
                        format!(
                            "Cleared all state for slot {} before spawning new stream",
                            proc_slot
                        )
                    });

                    // In slot-priority mode: rotate active slot and activate next buffered slot
                    let buffered_nodes = if shared.slot_priority_enabled {
                        transition_slot_to_buffering(&shared, proc_slot);
                        print_debug(|| format!("Calling activate_next_slot()"));
                        activate_next_slot(&shared, Some(proc_slot))
                    } else {
                        None
                    };

                    // Flush buffered nodes from newly activated slot (if any)
                    if let Some(nodes) = buffered_nodes {
                        if !nodes.is_empty() {
                            Self::preparation(&shared, &nodes, thread_core, thread_slot, false);
                        }
                    }

                    // Check if we should start a new iteration
                    if process_slot_completion(&shared, proc_slot) {
                        print_debug(|| {
                            format!(
                                "Starting new iteration for slot {} - spawning initial nodes",
                                proc_slot
                            )
                        });

                        // Remove from completed set since we're starting again
                        shared.resolution_state.unmark_slot_completed(proc_slot);

                        // Network nodes are managed by receiver threads; no per-slot network tracking here

                        // Clear activity tracking for this slot's new stream iteration
                        stream_slot_activity.remove(&proc_slot);
                        print_debug(|| {
                            format!(
                                "Cleared stream_slot_activity for slot {} to start new stream iteration",
                                proc_slot
                            )
                        });

                        // Spawn initial compute nodes for the restarting slot
                        // (network nodes are handled by receivers, not scheduled)
                        let compute_nodes = initial_nodes(&shared, vec![proc_slot]);

                        // Apply slot-priority buffering for compute nodes
                        if !compute_nodes.is_empty() {
                            if shared.slot_priority_enabled && !is_slot_active(&shared, proc_slot) {
                                let mut slot_buffers = shared.slot_buffers.write();
                                slot_buffers[proc_slot].extend(compute_nodes);
                            } else {
                                Self::preparation(
                                    &shared,
                                    &compute_nodes,
                                    thread_core,
                                    thread_slot,
                                    false,
                                );
                            }
                        }
                    }
                }
            }

            // Only record timing/metrics when actual work was performed
            if work_performed {
                if let Some(tb) = &shared.time_buffer {
                    let end_time = if let Some(tb) = &shared.time_buffer {
                        tb.measure_time()
                    } else {
                        TimingMethod::Instant(Instant::now())
                    };
                    let duration = tb.measure_duration(start_time, end_time);
                    tb.add_task_time(
                        thread_slot,
                        &format!("Resolution Thread {}", thread_id),
                        usize::MAX,
                        duration,
                    );
                }

                // Lock-free recording via per-worker channel
                let current_stream = shared.stream_complete_counter.load(Ordering::Relaxed);
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
            }
        } // End of resolution processing loop
    }
}

// Helper Functions
impl SynRt {
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
                send_to_scheduler(
                    &self.shared,
                    &post_schedule,
                    &pre_build_args,
                    &functions,
                    false,
                );
                print_debug(|| format!("Added post node: {}", post_node.name));
                // Wait until all are completed by checking node_results
                let mut completed_count = 0;
                while completed_count < post_node.factor {
                    sleep(Duration::from_millis(10));
                    completed_count = 0;
                    let results_read = self.shared.node_results.read();
                    for i in 0..post_node.factor {
                        let node_info = NodeInfo::new(post_node.id, stream_use, i, 0);
                        if results_read.result_exists(&node_info) {
                            completed_count += 1;
                        }
                    }
                    drop(results_read);
                }
            }
            print_debug(|| "All post-nodes completed".to_string());
        }
    }

    fn init_results(&mut self, slots: usize) {
        // Initialize node_results with factor entries
        let nodes = &self.shared.graph.nodes;
        let mut node_results_lock = self.shared.node_results.write();
        node_results_lock.init_map(&nodes, slots, None);

        // Initialize post_nodes if any
        let post_nodes_opt = &self.shared.graph.post_nodes;
        if let Some(post_nodes) = post_nodes_opt {
            node_results_lock.extend_map(&post_nodes);
        }
    }

    pub fn print_statistics(&self, bench_name: &str, out_file: Option<&str>) {
        if let Some(tb) = &self.shared.time_buffer {
            tb.print_stats(bench_name, out_file);
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
    Vec<Sender<PacketMessage>>,
    Vec<Receiver<PacketMessage>>,
    Vec<AtomicUsize>,
) {
    if let Some(config_spec) = graph.network_config() {
        let receiver_sockets = bind_udp_socket_range(
            &config_spec.address,
            config_spec.start_port,
            config_spec.num_sockets,
        );

        let mut packet_senders = Vec::with_capacity(config_spec.num_sockets);
        let mut packet_receivers = Vec::with_capacity(config_spec.num_sockets);
        for _ in 0..config_spec.num_sockets {
            let (tx, rx) = crossbeam_channel::bounded(config_spec.buffer_depth);
            packet_senders.push(tx);
            packet_receivers.push(rx);
        }

        let packet_drop_counters = (0..config_spec.num_sockets)
            .map(|_| AtomicUsize::new(0))
            .collect();

        (
            receiver_sockets,
            packet_senders,
            packet_receivers,
            packet_drop_counters,
        )
    } else {
        (Vec::new(), Vec::new(), Vec::new(), Vec::new())
    }
}
