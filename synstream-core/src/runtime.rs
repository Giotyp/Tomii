use core_affinity;
use crossbeam_channel::Receiver;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{sleep, spawn};
use std::time::{Duration, Instant};

use crate::debug::print_debug;
use crate::graph::*;
use crate::graph_struct::*;
use crate::runtime_funcs::*;
use crate::scheduler::{Scheduler, SchedulerImpl};
use crate::time_buffer::TimeBufferManager;
use crate::{buffers::*, IdType, Record};
use synstream_types::*;

/// Main SynStream Runtime struct with shared context
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
        system_threads: usize,
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
        let time_buffer = Arc::new(TimeBufferManager::new_async(
            slots + system_threads,
            system_threads,
            use_rdtsc,
        ));

        let recorder = if record {
            Some(Arc::new(Mutex::new(HashMap::new())))
        } else {
            None
        };
        let base_instant = Arc::new(Instant::now());
        let job_counter = Arc::new(AtomicUsize::new(0));
        // core_offset is updated in run()
        let core_offset = Arc::new(AtomicUsize::new(0));

        // Initialize shared dependency tracking structures
        let dependency_count_vec: Vec<usize> = app_graph.dependency_count_vec();
        let mut dependency_map = VecMap::new(0);
        dependency_map.init_map(&app_graph.nodes, slots, Some(dependency_count_vec.clone()));

        // Compute max_factor for flat index computation
        let max_factor = node_cache.iter().map(|n| n.factor).max().unwrap_or(1);
        let num_nodes = app_graph.nodes.len();

        // Initialize remaining nodes trackers with AtomicUsize for thread-safe access
        let mut remaining_nodes = Vec::new();
        let mut remaining_cond_nodes = Vec::new();
        let mut remaining_init = Vec::new(); // Store initial values for reinit
        let mut node_id_to_rem = vec![0; app_graph.nodes.len()];
        let mut node_id_is_cond = vec![false; app_graph.nodes.len()]; // Track which nodes are condition nodes
        
        // Lock-free sent_to_queue: nodes_sent_to_queue[slot][node_id * max_factor + index]
        let mut nodes_sent_to_queue: Vec<Vec<AtomicBool>> = Vec::new();

        for _slot in 0..slots {
            let mut slot_remaining = Vec::new();
            let mut slot_cond_remaining = Vec::new();
            let mut slot_remaining_init = Vec::new();
            // Initialize flat AtomicBool array for lock-free sent_to_queue checks
            let mut slot_sent: Vec<AtomicBool> = Vec::with_capacity(num_nodes * max_factor);
            for _ in 0..(num_nodes * max_factor) {
                slot_sent.push(AtomicBool::new(false));
            }
            nodes_sent_to_queue.push(slot_sent);

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
            completed_tx: Arc::new(RwLock::new(None)),
            workers: Arc::new(AtomicUsize::new(1)), // Will be set in run()
            recorder,
            base_instant,
            job_counter,
            core_offset,
            dependency_map: Arc::new(RwLock::new(dependency_map)),
            remaining_nodes: Arc::new(remaining_nodes),
            remaining_cond_nodes: Arc::new(remaining_cond_nodes),
            nodes_sent_to_queue: Arc::new(nodes_sent_to_queue),
            max_factor,
            completed_slots: Arc::new(Mutex::new(std::collections::HashSet::new())),
            node_id_to_rem: Arc::new(node_id_to_rem),
            node_id_is_cond: Arc::new(node_id_is_cond),
            remaining_init: Arc::new(remaining_init),
            initial_prep_done: Arc::new(AtomicUsize::new(0)),
            system_threads,
        });

        SynRt { shared }
    }

    pub fn base_instant(&self) -> Instant {
        *self.shared.base_instant
    }

    pub fn run(
        &mut self,
        scheduler: SchedulerImpl,
        system_threads: usize,
        batching_size: usize,
        batching_limit: u64,
    ) {
        // Overwrite workers
        self.shared
            .workers
            .store(scheduler.workers(), Ordering::SeqCst);

        // create completed channel
        let (completed_tx, completed_rx) = crossbeam_channel::unbounded::<(NodeInfo, CmTypes)>();
        {
            let mut tx_lock = self.shared.completed_tx.write();
            *tx_lock = Some(completed_tx);
        }
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
            self.shared.time_buffer.start_slot_processing(system_slot);
        }

        // Spawn multiple resolution threads
        let mut resolution_handles = Vec::new();
        for thread_id in 0..system_threads {
            let shared_for_resolution = Arc::clone(&self.shared);
            let completed_rx_clone = completed_rx.clone();
            let thread_core = core_offset + thread_id;
            // Each system thread gets its own slot: slots + thread_id
            let thread_slot = self.shared.slots + thread_id;

            let resolution_handle = spawn(move || {
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
                    batching_size,
                    batching_limit,
                );
            });
            resolution_handles.push(resolution_handle);
        }
        print_debug(|| format!("{} Resolution threads spawned", system_threads));

        let start_time = Instant::now();
        // Check for max_runtime
        print_debug(|| "Max runtime check started".to_string());
        if let Some(max_runtime) = self.shared.max_runtime {
            loop {
                if start_time.elapsed().as_secs() > max_runtime {
                    // set exit signal
                    println!("Max runtime reached, exiting...");
                    // Process post-nodes if any
                    println!("Processing possible post-nodes...");
                    self.schedule_post_nodes();
                    // Close completed channel - send exit signal to all threads
                    {
                        let tx_lock = self.shared.completed_tx.read();
                        if let Some(ref tx) = *tx_lock {
                            for _ in 0..system_threads {
                                tx.send((NodeInfo::new(IdType::MAX, 0, 0, 0), CmTypes::None))
                                    .unwrap();
                            }
                        }
                    }
                    break;
                }
                sleep(Duration::from_secs(2));
            }
        }

        // Wait for all threads to finish
        for handle in resolution_handles {
            handle.join().unwrap();
        }

        // Finish timing for system thread slots only
        for thread_id in 0..system_threads {
            let system_slot = self.shared.slots + thread_id;
            let _ = self.shared.time_buffer.finish_slot_processing(system_slot);
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
        for node_info in nodes_to_schedule {
            let start_time = shared.time_buffer.measure_time();
            let start_ns = shared.base_instant.elapsed().as_nanos();
            print_debug(|| format!("Preparing {:?}", node_info));

            // Schedule Task - args will be built in the worker thread
            send_to_scheduler(&shared, node_info, None, None);

            let end_time = shared.time_buffer.measure_time();
            let end_ns = shared.base_instant.elapsed().as_nanos();
            let duration = shared.time_buffer.measure_duration(start_time, end_time);
            shared.time_buffer.add_task_time(
                thread_slot,
                "Preparation Thread",
                usize::MAX,
                duration,
            );

            if let Some(rec) = &shared.recorder {
                let job_id = shared.job_counter.fetch_add(1, Ordering::SeqCst);
                let mut map = rec.lock();
                let vec = map.entry(thread_slot).or_insert_with(Vec::new);
                vec.push(Record {
                    job_id,
                    start_ns,
                    end_ns,
                    worker: thread_core,
                    task_id: node_info.id,
                    index: node_info.index,
                });
            }
        }
    }

    fn resolution(
        shared: Arc<SharedData>,
        completed_rx: Receiver<(NodeInfo, CmTypes)>,
        thread_core: usize,
        thread_id: usize,
        thread_slot: usize,
        batching_size: usize,
        batching_limit: u64,
    ) {
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
                println!(
                    "Thread {} in Core {} performing initial preparation",
                    thread_id, thread_core
                );

                // Find and send initial nodes to ready channel
                let slot_vec: Vec<usize> = (0..shared.slots).collect();
                let init_nodes = initial_nodes(&shared, slot_vec);
                let mut nodes_to_schedule = Vec::new();
                for node_info in init_nodes {
                    nodes_to_schedule.push(node_info);
                }
                Self::preparation(&shared, &nodes_to_schedule, thread_core, thread_slot);
            }
        }

        // prefetch cond indexes for efficiency
        let cond_indexes = shared.graph.get_condition_indexes();

        let mut complete_iteration: bool;

        // Process completed nodes with dynamic batching
        let batch_timeout = Duration::from_micros(batching_limit);
        loop {
            let mut batch: Vec<(NodeInfo, CmTypes)> = Vec::with_capacity(batching_size);

            // Collect first item (blocking)
            match completed_rx.recv() {
                Ok(first_item) => {
                    if first_item.0.id == IdType::MAX {
                        // Exit signal received, stopping thread
                        return;
                    }
                    batch.push(first_item);
                }
                Err(_) => return, // Channel closed
            }

            // Try to collect more items up to batching_size or until timeout
            let batch_start = Instant::now();
            while batch.len() < batching_size {
                let remaining_time = batch_timeout.saturating_sub(batch_start.elapsed());

                match completed_rx.recv_timeout(remaining_time) {
                    Ok(item) => {
                        if item.0.id == IdType::MAX {
                            // Exit signal received, process current batch then stop
                            break;
                        }
                        batch.push(item);
                    }
                    Err(_) => break, // Timeout or channel closed
                }
            }

            print_debug(|| format!("Processing batch of {} nodes", batch.len()));

            // Process the entire batch
            for (mut node_info, result) in batch {
                let start_ns = shared.base_instant.elapsed().as_nanos();
                let start_time = shared.time_buffer.measure_time();

                print_debug(|| format!("Processing Completed {:?}", node_info));

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
                            "Skipping further processing of node {:?} due to ID function failure",
                            node_info
                        )
                    });
                    continue;
                }

                // store result - single lock acquisition
                shared.node_results.write().set(&node_info, result);

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

                print_debug(|| format!("Successors of node {:?}: {:?}", node_info, successors));

                let mut nodes_sent = 0;
                let mut nodes_to_schedule: Vec<NodeInfo> = Vec::new();

                // Collect all potential successors with their info
                let mut succ_updates: Vec<(NodeInfo, bool, IdType, usize)> = Vec::new(); // Added flat_idx
                let max_factor = shared.max_factor;
                
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
                            "Processing successor id {} - {:?} of node {:?}",
                            succ_id, succ_indexes, node_info
                        )
                    });

                    for succ_index in succ_indexes {
                        // Compute flat index for later atomic claim
                        let flat_idx = (succ_id as usize) * max_factor + succ_index;
                        let succ_info =
                            NodeInfo::new(succ_id, node_info.slot, succ_index, node_info.index);
                        succ_updates.push((succ_info, has_condition, succ_id, flat_idx));
                    }
                }

                // Batch process dependency decrements
                // Collect condition nodes to check OUTSIDE the lock to avoid nested locking
                let mut cond_nodes_to_check: Vec<(NodeInfo, usize, usize)> = Vec::new(); // Added flat_idx
                {
                    let mut dep_map = shared.dependency_map.write();
                    print_debug(|| {
                        format!(
                            "Dependency map before processing successors of {:?}: {:?}",
                            node_info, *dep_map
                        )
                    });

                    for (succ_info, has_cond, succ_id, flat_idx) in succ_updates {
                        if let Some(dep) = dep_map.decrease(&succ_info) {
                            if dep == 0 {
                                // Only now try to atomically claim the slot
                                // This ensures we only claim when dependencies are satisfied
                                if shared.nodes_sent_to_queue[node_info.slot][flat_idx]
                                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                                    .is_ok()
                                {
                                    if !has_cond {
                                        print_debug(|| {
                                            format!("Sent successor {:?} to ready channel", succ_info)
                                        });
                                        nodes_to_schedule.push(succ_info);
                                        nodes_sent += 1;
                                    } else {
                                        // Collect condition nodes - will evaluate outside lock
                                        let cond_idx = shared.node_cache[succ_id as usize].cond_index;
                                        cond_nodes_to_check.push((succ_info, cond_idx, flat_idx));
                                    }
                                }
                            } else {
                                print_debug(|| {
                                    format!(
                                        "Successor {:?} not ready, remaining dependencies: {}",
                                        succ_info, dep
                                    )
                                });
                            }
                        }
                    }
                    
                    print_debug(|| {
                        format!(
                            "Dependency map after processing successors of {:?}: {:?}",
                            node_info, *dep_map
                        )
                    });
                } // dep_map released here
                
                // Evaluate conditions OUTSIDE the locks - conditions_met takes node_results.read()
                for (succ_info, cond_idx, flat_idx) in cond_nodes_to_check {
                    if conditions_met(&shared, &succ_info, &cond_indexes[cond_idx]) {
                        print_debug(|| {
                            format!("Sent successor {:?} to ready channel", succ_info)
                        });
                        nodes_to_schedule.push(succ_info);
                        nodes_sent += 1;
                    } else {
                        print_debug(|| {
                            format!("Conditions not met for successor {:?}", succ_info)
                        });
                        // Reset the atomic flag since condition not met
                        shared.nodes_sent_to_queue[node_info.slot][flat_idx]
                            .store(false, Ordering::Release);
                    }
                }

                // Schedule Nodes
                Self::preparation(&shared, &nodes_to_schedule, thread_core, thread_slot);

                // Check for stream completion - only process each slot once (thread-safe)
                complete_iteration = false;
                if nodes_sent == 0 {
                    let should_check_completion = {
                        let completed_lock = shared.completed_slots.lock();
                        !completed_lock.contains(&node_info.slot)
                    };

                    if should_check_completion {
                        // Check if all nodes in this slot have been processed (lock-free)
                        let all_nodes_processed = shared.remaining_nodes[node_info.slot]
                            .iter()
                            .all(|count| count.load(Ordering::Acquire) == 0);

                        if all_nodes_processed {
                            print_debug(|| {
                                format!("Completed iteration at slot {}", node_info.slot)
                            });

                            // Mark this slot as completed (thread-safe)
                            {
                                let mut completed_lock = shared.completed_slots.lock();
                                completed_lock.insert(node_info.slot);
                            }

                            // Reset dependency_map for this slot
                            {
                                let mut dep_map = shared.dependency_map.write();
                                dep_map.reinit_slot(node_info.slot);
                            }

                            // Reinit remaining_nodes for this slot using pre-computed init values (lock-free)
                            let slot_remaining = &shared.remaining_nodes[node_info.slot];
                            let slot_init = &shared.remaining_init[node_info.slot];
                            for (node_rem_idx, init_val) in slot_init.iter().enumerate() {
                                slot_remaining[node_rem_idx].store(*init_val, Ordering::Release);
                            }

                            // Clear nodes_sent_to_queue for this slot (lock-free reset of all atomic flags)
                            for flag in shared.nodes_sent_to_queue[node_info.slot].iter() {
                                flag.store(false, Ordering::Release);
                            }

                            complete_iteration = true;
                        }
                    }
                }
                let end_time = shared.time_buffer.measure_time();

                if let Some(rec) = &shared.recorder {
                    let job_id = shared.job_counter.fetch_add(1, Ordering::SeqCst);
                    let end_ns = shared.base_instant.elapsed().as_nanos();
                    let mut map = rec.lock();
                    let vec = map.entry(thread_slot).or_insert_with(Vec::new);
                    vec.push(Record {
                        job_id,
                        start_ns,
                        end_ns,
                        worker: thread_core,
                        task_id: IdType::MAX,
                        index: node_info.index,
                    });
                }

                let duration = shared.time_buffer.measure_duration(start_time, end_time);
                shared.time_buffer.add_task_time(
                    thread_slot,
                    &format!("Resolution Thread {}", thread_id),
                    usize::MAX,
                    duration,
                );
                if complete_iteration {
                    // Add initial nodes for new iteration
                    if process_slot_completion(&shared, node_info.slot) {
                        // Remove from completed set since we're starting again (thread-safe)
                        {
                            let mut completed_lock = shared.completed_slots.lock();
                            completed_lock.remove(&node_info.slot);
                        }
                        let init_nodes = initial_nodes(&shared, vec![node_info.slot]);
                        let mut nodes_to_schedule = Vec::new();
                        for node_info in init_nodes {
                            nodes_to_schedule.push(node_info);
                        }
                        Self::preparation(&shared, &nodes_to_schedule, thread_core, thread_slot);
                    }
                }
            } // End of batch processing for loop
        } // End of batching loop
    }
}

// Helper Functions
impl SynRt {
    fn schedule_post_nodes(&mut self) {
        let nodes = &self.shared.graph.post_nodes;
        if let Some(post_nodes) = nodes {
            let stream_use = self.shared.slots + self.shared.system_threads; // Use last available slot for post-nodes
            for post_node in post_nodes {
                for index in 0..post_node.factor {
                    let mut node_info = NodeInfo::new(post_node.id, stream_use, index, 0);
                    node_info.set_post_node(true);

                    let arg_vec =
                        parse_args(&self.shared, &post_node.args, index, stream_use, 0, None);

                    let func = post_node.func_ptr;

                    send_to_scheduler(&self.shared, &node_info, Some(arg_vec), func);
                }
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
        self.shared.time_buffer.print_stats(bench_name, out_file);
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

    pub fn write_runtime_record(&self, path: &str) {
        if let Some(rec) = &self.shared.recorder {
            let map = rec.lock();
            if map.is_empty() {
                println!("Runtime: no recorded events to write");
                return;
            }
            match std::fs::OpenOptions::new().append(true).open(path) {
                Ok(mut f) => {
                    for (slot, vec) in map.iter() {
                        for r in vec.iter() {
                            let _ = writeln!(
                                f,
                                "{},{},{},{},{},{},{}",
                                slot,
                                r.job_id,
                                r.start_ns,
                                r.end_ns,
                                r.worker,
                                r.task_id,
                                    r.index
                                );
                            }
                        }
                        println!("Runtime: appended {} slots to {}", map.len(), path);
                    }
                    Err(e) => {
                        eprintln!("Runtime: failed to open {} for append: {}", path, e);
                    }
                }
        } else {
            println!("Runtime: recorder not enabled");
        }
    }
}
