use core_affinity;
use crossbeam_channel::Receiver;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{sleep, spawn, JoinHandle};
use std::time::{Duration, Instant};

use crate::async_recorder::{set_worker_recorder, submit_record, AsyncRecorder};
use crate::debug::print_debug;
use crate::func_reg::get_func;
use crate::graph::*;
use crate::graph_struct::*;
use crate::network::{multi_socket_receiver_loop, single_socket_receiver_loop, NetworkSocket};
use crate::resolution_state::{MultiThreadedState, ResolutionState, SingleThreadedState};
use crate::runtime_funcs::*;
use crate::scheduler::{Scheduler, SchedulerImpl};
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
        timing_enabled: bool,
        system_threads: usize,
        _nrx: usize,
        slot_priority_enabled: bool,
        async_recorder: Option<Arc<AsyncRecorder>>, // Optional shared recorder from caller
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

        let available_stream_slots = Arc::new(RwLock::new(Vec::new()));
        let mut available_write = available_stream_slots.write();
        for _ in 0..slots {
            available_write.push(std::usize::MAX); // real stream id
        }
        drop(available_write);

        // Build node cache for fast repeated access
        let node_cache: Vec<NodeCacheEntry> = app_graph
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
                    slots + system_threads + _nrx,
                    100,
                )))
            })
        } else {
            None
        };
        let base_instant = Arc::new(Instant::now());
        let job_counter = Arc::new(AtomicUsize::new(0));
        // core_offset is updated in run()
        let core_offset = Arc::new(AtomicUsize::new(0));

        // Initialize batch buffer for scheduler-side batching
        let batch_buffer = Arc::new(Mutex::new(Vec::new()));
        let batch_last_sent = Arc::new(Mutex::new(Instant::now()));
        let flusher_shutdown = Arc::new(AtomicUsize::new(0));

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
            scheduler: Arc::new(RwLock::new(None)),
            network_scheduler: Arc::new(RwLock::new(None)),
            completed_tx: Arc::new(RwLock::new(None)),
            workers: Arc::new(AtomicUsize::new(1)), // Will be set in run()
            async_recorder,
            base_instant,
            job_counter,
            core_offset,
            resolution_state,
            remaining_nodes: Arc::new(remaining_nodes),
            remaining_cond_nodes: Arc::new(remaining_cond_nodes),
            node_id_to_rem: Arc::new(node_id_to_rem),
            node_id_is_cond: Arc::new(node_id_is_cond),
            remaining_init: Arc::new(remaining_init),
            initial_prep_done: Arc::new(AtomicUsize::new(0)),
            system_threads,
            batch_buffer,
            batch_last_sent,
            flusher_shutdown,
            slot_states,
            slot_priority_enabled,
            slot_buffers,
            // Network fields (empty - will be initialized in run() if network_config present)
            packet_senders: Vec::new(),
            packet_receivers: Vec::new(),
            receiver_sockets: Vec::new(),
            packet_drop_counters: Vec::new(),
            network_config: app_graph.network_config.clone(),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
        });

        SynRt { shared }
    }

    pub fn base_instant(&self) -> Instant {
        *self.shared.base_instant
    }

    pub fn run(&mut self, scheduler: SchedulerImpl, system_threads: usize) {
        // Overwrite workers
        self.shared
            .workers
            .store(scheduler.workers(), Ordering::SeqCst);

        // create completed channel for batched results
        let (completed_tx, completed_rx) =
            crossbeam_channel::unbounded::<Vec<(NodeInfo, CmTypes)>>();

        // Set completed_tx in SharedData
        {
            let mut tx_lock = self.shared.completed_tx.write();
            *tx_lock = Some(completed_tx.clone());
        }

        // Set completed_tx in the scheduler and start the flusher thread
        scheduler.set_completed_tx(completed_tx);

        // Get network workers count before moving scheduler
        let nrx = scheduler.network_workers();

        // Store scheduler
        let core_offset: usize;
        {
            let mut scheduler_lock = self.shared.scheduler.write();
            core_offset = scheduler.core_offset().unwrap_or(0);
            *scheduler_lock = Some(Arc::new(scheduler));
        }

        self.shared.core_offset.store(core_offset, Ordering::SeqCst);

        // Initialize node_results
        self.init_results(self.shared.slots);

        // Initiate synstream-runtime timing for system thread slots only
        for thread_id in 0..system_threads {
            let system_slot = self.shared.slots + thread_id;
            if let Some(tb) = &self.shared.time_buffer {
                tb.start_slot_processing(system_slot);
            }
        }

        // Initialize network receiver infrastructure if network_config present
        let receiver_handles: Vec<JoinHandle<()>> = if let Some(ref network_config) =
            self.shared.network_config
        {
            let num_sockets = network_config.num_sockets;
            let buffer_depth = network_config.buffer_depth;

            println!("\n=== Initializing Network Receiver Infrastructure ===");
            println!("Number of sockets: {}", num_sockets);
            println!("Buffer depth: {} packets per socket", buffer_depth);

            // Initialize sockets using new or legacy method
            let mut sockets = Vec::with_capacity(num_sockets);

            // Get init_objects and obj_id_map for socket reference resolution
            let init_objects = self.shared.graph.init_objects.as_ref().unwrap();
            let obj_id_map = &self.shared.graph.obj_id_map;

            #[allow(deprecated)]
            if let Some(ref socket_refs) = network_config.socket_refs {
                // Method 1: Individual socket references
                println!("Using individual socket references");
                for socket_ref_name in socket_refs {
                    let obj_id = obj_id_map.get(socket_ref_name).unwrap_or_else(|| {
                        panic!(
                            "Socket reference '{}' not found in initializations",
                            socket_ref_name
                        )
                    });

                    let socket_result = &init_objects[*obj_id][0];
                    let socket = extract_single_socket(socket_result, socket_ref_name);
                    sockets.push(socket);
                }
            } else if let Some(ref range_ref) = network_config.socket_range_ref {
                // Method 2: Socket range reference
                println!("Using socket range reference: {}", range_ref);
                let obj_id = obj_id_map.get(range_ref).unwrap_or_else(|| {
                    panic!(
                        "Socket range reference '{}' not found in initializations",
                        range_ref
                    )
                });

                let sockets_result = &init_objects[*obj_id][0];
                sockets = extract_socket_vector(sockets_result, range_ref, num_sockets);
            } else if let Some(ref socket_init_func_name) = network_config.socket_initializer {
                // LEGACY METHOD (DEPRECATED): Use user-provided initialization function
                eprintln!("⚠️  WARNING: Using deprecated 'socket_initializer' field");
                eprintln!("    Please migrate to 'socket_refs' or 'socket_range_ref'");
                println!("Socket initializer (DEPRECATED): {}", socket_init_func_name);

                // Get socket initializer function pointer
                let init_func_ptr = get_func(socket_init_func_name).unwrap_or_else(|| {
                    panic!(
                        "Socket initializer function '{}' not found in user library",
                        socket_init_func_name
                    )
                });

                // Initialize sockets by calling user-provided function
                for socket_id in 0..num_sockets {
                    let socket_id_cmtype = CmTypes::from_any(socket_id);
                    let args = vec![socket_id_cmtype];

                    // SAFETY: Calling user-provided FFI function
                    let result = init_func_ptr(args);

                    // Extract socket from result (wrapped in CmTypes::Any)
                    let socket = match result {
                        CmTypes::Any(any_arc) => {
                            let any_lock = any_arc.read().unwrap();
                            // Try to downcast to UdpSocket
                            if let Some(udp_socket) = any_lock.downcast_ref::<std::net::UdpSocket>() {
                                // Clone the socket by trying to duplicate it
                                match udp_socket.try_clone() {
                                    Ok(cloned) => NetworkSocket::Udp(cloned),
                                    Err(e) => panic!(
                                        "Failed to clone UdpSocket from '{}': {}",
                                        socket_init_func_name, e
                                    ),
                                }
                            } else {
                                panic!(
                                    "Socket initializer '{}' must return UdpSocket wrapped in CmTypes::Any",
                                    socket_init_func_name
                                );
                            }
                        }
                        other => panic!(
                            "Socket initializer '{}' must return socket wrapped in CmTypes::Any, got {:?}",
                            socket_init_func_name, other
                        ),
                    };

                    sockets.push(socket);
                    println!("  Socket {} initialized successfully", socket_id);
                }
            } else {
                panic!("network_config must specify either 'socket_refs', 'socket_range_ref', or 'socket_initializer'");
            }

            assert_eq!(
                sockets.len(),
                num_sockets,
                "Initialized {} sockets but network_config.num_sockets={}",
                sockets.len(),
                num_sockets
            );

            println!("SynRt-Net {} network sockets initialized", num_sockets);

            // Create SPSC channels (one per socket)
            let mut packet_senders = Vec::with_capacity(num_sockets);
            let mut packet_receivers = Vec::with_capacity(num_sockets);
            for _socket_id in 0..num_sockets {
                let (tx, rx) = crossbeam_channel::bounded(buffer_depth);
                packet_senders.push(tx);
                packet_receivers.push(rx);
            }
            println!(
                "Created {} SPSC channels with {} capacity each",
                num_sockets, buffer_depth
            );

            // Create packet drop counters
            let packet_drop_counters: Vec<AtomicUsize> =
                (0..num_sockets).map(|_| AtomicUsize::new(0)).collect();

            // Store in SharedData (requires mutable access - we're in run())
            // We need to use unsafe pointer casting since SharedData is already in Arc
            // Actually, we can't modify SharedData after Arc::new(). Need different approach.
            // Solution: Initialize these fields BEFORE creating SharedData, or use interior mutability.
            // For now, let's use a workaround: directly access mutable self.shared before it's truly shared

            // WORKAROUND: Since we're still in run() and resolution threads haven't started,
            // we can use Arc::get_mut() to get mutable access
            let shared_mut = Arc::get_mut(&mut self.shared)
                .expect("SharedData should be exclusively owned at this point");

            shared_mut.receiver_sockets = sockets;
            shared_mut.packet_senders = packet_senders;
            shared_mut.packet_receivers = packet_receivers;
            shared_mut.packet_drop_counters = packet_drop_counters;

            // Determine receiver thread allocation
            // CRITICAL: Receiver threads allocated AFTER system threads
            // nrx was already retrieved before moving scheduler
            let receiver_offset = core_offset + system_threads; // Receivers after system threads

            // Get dylib path for frame ID extraction (from environment or default)
            let dylib_path =
                std::env::var("DYLIB_PATH").unwrap_or_else(|_| "./libmimolib.so".to_string());

            println!(
                "\nSpawning {} receiver threads starting at core {}",
                nrx, receiver_offset
            );
            println!("Using dylib: {} for frame ID extraction", dylib_path);

            let mut handles = Vec::with_capacity(nrx);

            if nrx >= num_sockets {
                // Optimal case: 1:1 thread-to-socket mapping
                println!("Using 1:1 thread-to-socket mapping (optimal)");
                for socket_id in 0..num_sockets {
                    let shared_clone = Arc::clone(&self.shared);
                    let core_id = receiver_offset + socket_id;
                    let dylib_clone = dylib_path.clone();

                    let handle = spawn(move || {
                        single_socket_receiver_loop(shared_clone, socket_id, core_id, dylib_clone);
                    });
                    handles.push(handle);
                    println!(
                        "  Receiver thread {} (socket {}) spawned on core {}",
                        socket_id, socket_id, core_id
                    );
                }
            } else {
                // Round-robin polling: fewer threads than sockets
                println!(
                    "WARNING: nrx ({}) < num_sockets ({}). Using round-robin polling.",
                    nrx, num_sockets
                );
                let sockets_per_thread = (num_sockets + nrx - 1) / nrx; // Ceiling division

                for thread_id in 0..nrx {
                    let start_socket = thread_id * sockets_per_thread;
                    let end_socket = std::cmp::min(start_socket + sockets_per_thread, num_sockets);
                    let socket_range = start_socket..end_socket;
                    let socket_range_display = socket_range.clone(); // For display

                    let shared_clone = Arc::clone(&self.shared);
                    let core_id = receiver_offset + thread_id;
                    let dylib_clone = dylib_path.clone();

                    let handle = spawn(move || {
                        multi_socket_receiver_loop(
                            shared_clone,
                            thread_id,
                            socket_range,
                            core_id,
                            dylib_clone,
                        );
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
        for thread_id in 0..system_threads {
            let shared_for_resolution = Arc::clone(&self.shared);
            let completed_rx_clone = completed_rx.clone();
            let thread_core = core_offset + thread_id;
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
        print_debug(|| format!("{} Resolution threads spawned", system_threads));

        let start_time = Instant::now();
        // Check for max_runtime
        print_debug(|| "Max runtime check started".to_string());
        if let Some(max_runtime) = self.shared.max_runtime {
            let scheduler_guard = self.shared.scheduler.read();

            let scheduler = match scheduler_guard.as_ref() {
                Some(s) => s,
                None => {
                    eprintln!("Scheduler is not initialized");
                    return;
                }
            };
            let mut prev_completed_jobs = scheduler.total_jobs_completed();
            let mut prev_spawned_jobs = scheduler.total_jobs_spawned();
            drop(scheduler_guard);
            sleep(RUN_SLEEP);
            loop {
                let scheduler_guard = self.shared.scheduler.read();

                let scheduler = match scheduler_guard.as_ref() {
                    Some(s) => s,
                    None => {
                        eprintln!("Scheduler is not initialized");
                        return;
                    }
                };

                let curr_completed_jobs = scheduler.total_jobs_completed();
                let curr_spawned_jobs = scheduler.total_jobs_spawned();
                let pending_jobs = scheduler.pending_jobs();

                let completed = {
                    if pending_jobs == 0
                        && curr_completed_jobs > 0
                        && curr_completed_jobs == prev_completed_jobs
                        && curr_spawned_jobs == prev_spawned_jobs
                    {
                        true
                    } else {
                        false
                    }
                };

                prev_completed_jobs = curr_completed_jobs;
                prev_spawned_jobs = curr_spawned_jobs;

                drop(scheduler_guard);

                if start_time.elapsed().as_secs() > max_runtime || completed {
                    // set exit signal
                    println!("Max runtime reached or graph completed, exiting...");
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
        {
            let scheduler_guard = self.shared.scheduler.read();
            if let Some(scheduler) = scheduler_guard.as_ref() {
                scheduler.shutdown_flusher();
            }
        }

        // Drop all senders to signal resolution threads to exit
        // This will close the channel and unblock the resolution threads
        {
            let mut tx_lock = self.shared.completed_tx.write();
            *tx_lock = None; // Drop the sender in SharedData
        }
        {
            let scheduler_guard = self.shared.scheduler.read();
            if let Some(scheduler) = scheduler_guard.as_ref() {
                let tx_ref = scheduler.get_completed_tx_ref();
                let mut tx_lock = tx_ref.lock();
                *tx_lock = None; // Drop the sender in scheduler
            }
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
            if let Some(ref network_config) = self.shared.network_config {
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
                    println!("  No packets dropped - excellent!");
                } else {
                    println!(
                        "  TOTAL: {} packets dropped across all sockets",
                        total_drops
                    );
                }
            }
        }

        // Finish timing for system thread slots only
        for thread_id in 0..system_threads {
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
        if shared.async_recorder.is_some() {
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
    ///
    /// ARCHITECTURE NOTE:
    /// This thread has two independent responsibilities:
    /// 1. PACKET RECEPTION (independent): Polls packet_receivers from network receivers
    ///    and injects parsed packets into node_results via inject_network_packet()
    /// 2. TASK SCHEDULING (via scheduler): Spawns compute node tasks (nx=false) respecting
    ///    slot-priority buffering when enabled
    ///
    /// Network nodes (nx=true) are NOT spawned by this thread. They are triggered by
    /// external packet arrival, not by task scheduling. Receiver threads handle packets
    /// on independent cores (2-3), while this thread coordinates on system cores.
    fn resolution(
        shared: Arc<SharedData>,
        completed_rx: Receiver<Vec<(NodeInfo, CmTypes)>>,
        thread_core: usize,
        thread_id: usize,
        thread_slot: usize,
    ) {
        // Initialize async recorder for system thread using universal indexing
        if let Some(ref recorder) = shared.async_recorder {
            let system_core_offset = shared.core_offset.load(Ordering::SeqCst);
            let channel_index = thread_core - system_core_offset;
            if let Some(tx) = recorder.get_worker_sender(channel_index) {
                set_worker_recorder(tx);
            }
        }

        let all_slots: Vec<usize> = (0..shared.slots).collect();
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

        // Process completed nodes with dynamic batching from scheduler
        loop {
            // PRIORITY 1: Poll network packets (low-latency path)
            // Only poll if network_config is present and receivers were spawned
            if !shared.packet_receivers.is_empty() {
                for antenna_id in 0..shared.packet_receivers.len() {
                    // Non-blocking poll - process all available packets
                    while let Ok(packet_msg) = shared.packet_receivers[antenna_id].try_recv() {
                        // Inject packet directly into resolution system
                        inject_network_packet(&shared, packet_msg);
                    }
                }
            }

            // PRIORITY 2: Receive batch from scheduler (blocking)
            let batch = match completed_rx.recv() {
                Ok(batch_from_scheduler) => batch_from_scheduler,
                Err(_) => return, // Channel closed
            };

            print_debug(|| {
                format!(
                    "Thread {:?} -- Processing batch of {} nodes",
                    thread_id,
                    batch.len()
                )
            });

            // Local tracking for THIS batch only
            let mut nodes_sent_in_slot: HashMap<usize, usize> = HashMap::new();

            let start_ns = shared.base_instant.elapsed().as_nanos();
            let start_time = if let Some(tb) = &shared.time_buffer {
                tb.measure_time()
            } else {
                TimingMethod::Instant(Instant::now())
            };

            // Process the entire batch
            for (mut node_info, result) in batch {
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

                // store result - single lock acquisition
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
                } else if !node_cache_entry.is_initial {
                    shared.remaining_nodes[node_info.slot][node_id_to_rem_idx]
                        .fetch_sub(1, Ordering::Release);
                }

                // Get successors
                let successors: &Vec<IdType> = {
                    if node_id_usize >= shared.graph.successors.len() {
                        &Vec::new()
                    } else {
                        &shared.graph.successors[node_id_usize]
                    }
                };

                print_debug(|| {
                    format!(
                        "Thread {:?} -- Successors of node {:?}: {:?}",
                        thread_id, node_info, successors
                    )
                });

                let mut nodes_to_schedule: Vec<NodeInfo> = Vec::new();

                // Collect all potential successors with their info
                let mut succ_updates: Vec<(NodeInfo, bool, IdType)> = Vec::new();

                for succ_id in successors {
                    let succ_id = *succ_id;
                    let succ_cache = &shared.node_cache[succ_id as usize];

                    // Use pre-computed flag for lock-free check
                    let has_condition = succ_cache.is_condition;

                    // Lock-free remaining count access
                    let remaining = {
                        let succ_id_to_rem_idx = shared.node_id_to_rem[succ_id as usize];
                        if has_condition {
                            shared.remaining_cond_nodes[node_info.slot][succ_id_to_rem_idx]
                                .load(Ordering::Acquire)
                        } else {
                            shared.remaining_nodes[node_info.slot][succ_id_to_rem_idx]
                                .load(Ordering::Acquire)
                        }
                    };

                    if remaining == 0 {
                        continue;
                    }

                    let succ_factor = succ_cache.factor;
                    let node_factor = node_cache_entry.factor;

                    let pred_count = succ_cache
                        .pred_vec
                        .get(node_info.id as usize)
                        .cloned()
                        .unwrap_or(0);

                    let succ_indexes = {
                        if succ_factor == node_factor && pred_count <= 1 {
                            vec![node_info.index]
                        } else if !has_condition {
                            let num_indexes = std::cmp::max(succ_factor, remaining);
                            (0..num_indexes).collect::<Vec<_>>()
                        } else {
                            vec![node_info.index % succ_factor]
                        }
                    };

                    print_debug(|| {
                        format!(
                            "Thread {:?} -- Processing successor id {} - {:?} of node {:?}",
                            thread_id, succ_id, succ_indexes, node_info
                        )
                    });

                    for succ_index in succ_indexes {
                        let succ_info =
                            NodeInfo::new(succ_id, node_info.slot, succ_index, node_info.index);
                        succ_updates.push((succ_info, has_condition, succ_id));
                    }
                }

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
                        // Reset the flag via resolution state
                        shared.resolution_state.reset_sent(
                            node_info.slot,
                            succ_info.id as usize,
                            succ_info.index,
                        );
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

            print_debug(|| {
                format!(
                    "Stream_slot_activity has {} slots to check: {:?}",
                    slots_to_check.len(),
                    slots_to_check
                )
            });

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

                // Check if all nodes in this slot have been processed (lock-free)
                // This works across batch boundaries now because stream_slot_activity is persistent
                let all_nodes_processed = shared.remaining_nodes[proc_slot]
                    .iter()
                    .all(|count| count.load(Ordering::Acquire) == 0);

                print_debug(|| {
                    let counts: Vec<usize> = shared.remaining_nodes[proc_slot]
                        .iter()
                        .map(|c| c.load(Ordering::Acquire))
                        .collect();
                    format!(
                        "Slot {} remaining_nodes: {:?}, all_processed={}",
                        proc_slot, counts, all_nodes_processed
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

                    // Reinit remaining_cond_nodes for this slot (reset to factor values)
                    let slot_cond_remaining = &shared.remaining_cond_nodes[proc_slot];
                    for node_id in 0..shared.graph.nodes.len() {
                        if shared.node_id_is_cond[node_id] {
                            let node_id_to_rem_idx = shared.node_id_to_rem[node_id];
                            let factor = shared.node_cache[node_id].factor;
                            slot_cond_remaining[node_id_to_rem_idx]
                                .store(factor, Ordering::Release);
                        }
                    }

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
                            print_debug(|| {
                                format!(
                                    "Slot-Priority: Flushing {} buffered nodes from activated slot",
                                    nodes.len()
                                )
                            });
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
                                print_debug(|| {
                                    format!(
                                        "Slot-Priority: Buffered {} init nodes for slot {}",
                                        slot_buffers[proc_slot].len(),
                                        proc_slot
                                    )
                                });
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
            if shared.async_recorder.is_some() {
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
        // Get scheduler with proper error handling
        let scheduler_guard = self.shared.scheduler.read();

        let scheduler = match scheduler_guard.as_ref() {
            Some(s) => s,
            None => {
                eprintln!("Scheduler is not initialized");
                return;
            }
        };

        scheduler.write_record(path);
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

/// Helper function to extract a single socket from CmTypes::Any
fn extract_single_socket(result: &CmTypes, name: &str) -> NetworkSocket {
    match result {
        CmTypes::Any(any_arc) => {
            let any_lock = any_arc.read().unwrap();
            if let Some(udp_socket) = any_lock.downcast_ref::<std::net::UdpSocket>() {
                match udp_socket.try_clone() {
                    Ok(cloned) => NetworkSocket::Udp(cloned),
                    Err(e) => panic!("Failed to clone UdpSocket from '{}': {}", name, e),
                }
            } else {
                panic!(
                    "Socket reference '{}' must contain UdpSocket wrapped in CmTypes::Any",
                    name
                );
            }
        }
        other => panic!(
            "Socket reference '{}' must be CmTypes::Any, got {:?}",
            name, other
        ),
    }
}

/// Helper function to extract a vector of sockets from CmTypes::VecAny
fn extract_socket_vector(
    result: &CmTypes,
    name: &str,
    expected_count: usize,
) -> Vec<NetworkSocket> {
    match result {
        CmTypes::VecAny(lock) => {
            let guard = lock.read().unwrap();

            if guard.len() != expected_count {
                panic!(
                    "Socket range '{}' contains {} sockets but expected {}",
                    name,
                    guard.len(),
                    expected_count
                );
            }

            let mut sockets = Vec::with_capacity(expected_count);
            for (idx, boxed) in guard.iter().enumerate() {
                if let Some(udp_socket) = boxed.downcast_ref::<std::net::UdpSocket>() {
                    match udp_socket.try_clone() {
                        Ok(cloned) => sockets.push(NetworkSocket::Udp(cloned)),
                        Err(e) => {
                            panic!("Failed to clone UdpSocket[{}] from '{}': {}", idx, name, e)
                        }
                    }
                } else {
                    panic!("Socket range '{}' element {} is not UdpSocket", name, idx);
                }
            }
            sockets
        }
        other => panic!(
            "Socket range reference '{}' must be CmTypes::VecAny, got {:?}",
            name, other
        ),
    }
}
