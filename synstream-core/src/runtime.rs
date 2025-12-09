use core_affinity;
use crossbeam_channel::Receiver;
use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
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
    ) -> SynRt {
        // Initialize stream completion counters
        let stream_completion_counts = Arc::new(RwLock::new(Vec::new()));
        let mut completion_counts = stream_completion_counts.write().unwrap();
        completion_counts.clear();

        let slots = std::cmp::min(slots, max_streams);
        for _ in 0..slots {
            completion_counts.push(AtomicUsize::new(0));
        }
        drop(completion_counts);

        let available_stream_slots = Arc::new(RwLock::new(Vec::new()));
        let mut available_write = available_stream_slots.write().unwrap();
        for _ in 0..slots {
            available_write.push(std::usize::MAX); // real stream id
        }
        drop(available_write);

        // Build node cache for fast repeated access
        let node_cache: Vec<NodeCacheEntry> = app_graph
            .nodes
            .iter()
            .map(|node| node_cache_entry(node, app_graph.init_objects.as_ref().unwrap()))
            .collect();

        let time_buffer = Arc::new(TimeBufferManager::new_async(slots + 1, use_rdtsc));

        let recorder = if record {
            Some(Arc::new(Mutex::new(HashMap::new())))
        } else {
            None
        };
        let base_instant = Arc::new(Instant::now());
        let job_counter = Arc::new(AtomicUsize::new(0));
        // core_offset is updated in run()
        let core_offset = Arc::new(AtomicUsize::new(0));

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
        });

        SynRt { shared }
    }

    pub fn base_instant(&self) -> Instant {
        self.shared.base_instant.as_ref().clone()
    }

    pub fn run(&mut self, scheduler: SchedulerImpl, batching_size: usize, batching_limit: u64) {
        // Overwrite workers
        self.shared
            .workers
            .store(scheduler.workers(), Ordering::SeqCst);

        // create completed channel
        let (completed_tx, completed_rx) = crossbeam_channel::unbounded::<(NodeInfo, CmTypes)>();
        {
            let mut tx_lock = self.shared.completed_tx.write().unwrap();
            *tx_lock = Some(completed_tx);
        }
        // Store scheduler
        let core_offset: usize;
        {
            let mut scheduler_lock = self.shared.scheduler.write().unwrap();
            core_offset = scheduler.core_offset().unwrap_or(0);
            *scheduler_lock = Some(Arc::new(scheduler));
        }

        self.shared.core_offset.store(core_offset, Ordering::SeqCst);

        // Initialize node_results
        self.init_results(self.shared.slots);

        // Initiate synstream-runtime timing for scheduling threads
        self.shared
            .time_buffer
            .start_slot_processing(self.shared.slots);

        // Spawn resolution thread
        let shared_for_resolution = Arc::clone(&self.shared);
        let resolution_handle = spawn(move || {
            if core_affinity::set_for_current(core_affinity::CoreId { id: core_offset }) {
                print_debug(|| format!("Resolution thread pinned to core {:?}", core_offset));
            } else {
                print_debug(|| "Failed to pin resolution thread to core".to_string());
            }
            Self::resolution(
                shared_for_resolution,
                completed_rx,
                core_offset,
                batching_size,
                batching_limit,
            );
        });
        print_debug(|| "Resolution thread spawned".to_string());

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
                    // Close completed channel
                    {
                        let tx_lock = self.shared.completed_tx.read().unwrap();
                        if let Some(ref tx) = *tx_lock {
                            tx.send((NodeInfo::new(IdType::MAX, 0, 0, 0), CmTypes::None))
                                .unwrap();
                        }
                    }
                    break;
                }
                sleep(Duration::from_secs(2));
            }
        }

        // Wait for thread to finish
        resolution_handle.join().unwrap();

        let _ = self
            .shared
            .time_buffer
            .finish_slot_processing(self.shared.slots);
    }
}

// Execution Threads
impl SynRt {
    fn preparation(
        shared: &Arc<SharedData>,
        nodes_to_schedule: &Vec<NodeInfo>,
        core_offset: usize,
    ) {
        for node_info in nodes_to_schedule {
            let start_ns = shared.base_instant.elapsed().as_nanos();
            print_debug(|| format!("Preparing {:?}", node_info));

            let node = &shared.node_cache[node_info.id as usize];

            let arg_vec = create_node_args(
                &shared,
                node,
                node_info.id,
                node_info.index,
                node_info.slot,
                node_info.pred_index,
            );

            if !arg_vec.is_empty() {
                // Schedule Task
                send_to_scheduler(&shared, node_info, arg_vec, None);
            }

            let end_ns = shared.base_instant.elapsed().as_nanos();
            let duration = shared.time_buffer.measure_duration(
                shared.time_buffer.measure_time(),
                shared.time_buffer.measure_time(),
            ); // approximate
            shared.time_buffer.add_task_time(
                shared.slots,
                "Preparation Thread",
                usize::MAX,
                duration,
            );

            if let Some(rec) = &shared.recorder {
                let job_id = shared.job_counter.fetch_add(1, Ordering::SeqCst);
                let mut map = rec.lock().unwrap();
                let vec = map.entry(shared.slots).or_insert_with(Vec::new);
                vec.push(Record {
                    job_id,
                    start_ns,
                    end_ns,
                    worker: core_offset,
                    task_id: node_info.id,
                    index: node_info.index,
                });
            }
        }
    }

    fn resolution(
        shared: Arc<SharedData>,
        completed_rx: Receiver<(NodeInfo, CmTypes)>,
        core_offset: usize,
        batching_size: usize,
        batching_limit: u64,
    ) {
        let dependency_count_vec: Vec<usize> = shared.graph.dependency_count_vec();
        let mut dependency_map = VecMap::new(0);
        dependency_map.init_map(
            &shared.graph.nodes,
            shared.slots,
            Some(dependency_count_vec),
        );
        print_debug(|| format!("Initialized dependency map:\n{:?}", dependency_map));

        // prefetch cond indexes for efficiency
        let cond_indexes = shared.graph.get_condition_indexes();

        // Find and send initial nodes to ready channel
        let slot_vec: Vec<usize> = (0..shared.slots).collect();
        let init_nodes = initial_nodes(&shared, slot_vec);
        let mut nodes_to_schedule = Vec::new();
        for node_info in init_nodes {
            nodes_to_schedule.push(node_info);
        }
        Self::preparation(&shared, &nodes_to_schedule, core_offset);

        // Initialize remaining processing nodes tracker
        let mut node_id_to_rem = vec![0; shared.graph.nodes.len()];
        let mut remaining_nodes = Vec::new();
        let mut remaining_cond_nodes = Vec::new();
        // Track which nodes have been sent to ready_queue to avoid double-sends
        let mut nodes_sent_to_queue: Vec<std::collections::HashSet<NodeInfo>> = Vec::new();

        for slot in 0..shared.slots {
            remaining_nodes.push(Vec::new());
            remaining_cond_nodes.push(Vec::new());
            nodes_sent_to_queue.push(std::collections::HashSet::new());
            for node_id in 0..shared.graph.nodes.len() {
                if shared.graph.initial_nodes.contains(&(node_id as IdType)) {
                    remaining_nodes[slot].push(0);
                    node_id_to_rem[node_id] = remaining_nodes[slot].len() - 1;
                } else if !shared.graph.condition_nodes.contains(&(node_id as IdType)) {
                    remaining_nodes[slot].push(shared.node_cache[node_id].factor);
                    node_id_to_rem[node_id] = remaining_nodes[slot].len() - 1;
                } else {
                    remaining_cond_nodes[slot].push(shared.node_cache[node_id].factor);
                    node_id_to_rem[node_id] = remaining_cond_nodes[slot].len() - 1;
                }
            }
        }

        // Track which slots have been completed to avoid double-processing
        let mut completed_slots: std::collections::HashSet<usize> =
            std::collections::HashSet::new();

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
                    let mut res_lock = shared.node_results.write().unwrap();
                    res_lock.set(&node_info, result);
                    drop(res_lock);
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

                // store result
                let mut res_lock = shared.node_results.write().unwrap();
                res_lock.set(&node_info, result);
                drop(res_lock);

                // Decrement remaining_nodes counter now that this task is confirmed completed
                let node_id_usize = node_info.id as usize;
                if shared
                    .graph
                    .condition_nodes
                    .contains(&(node_id_usize as IdType))
                {
                    remaining_cond_nodes[node_info.slot][node_id_to_rem[node_id_usize]] -= 1;
                } else if !shared
                    .graph
                    .initial_nodes
                    .contains(&(node_id_usize as IdType))
                {
                    remaining_nodes[node_info.slot][node_id_to_rem[node_id_usize]] -= 1;
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

                print_debug(|| {
                    format!(
                        "Remaining nodes before processing successors: {:?}",
                        remaining_nodes[node_info.slot]
                    )
                });
                print_debug(|| {
                    format!(
                        "Remaining conditional nodes before processing successors: {:?}",
                        remaining_cond_nodes[node_info.slot]
                    )
                });

                let mut nodes_sent = 0;
                let mut nodes_to_schedule: Vec<NodeInfo> = Vec::new();
                for succ_id in successors {
                    let succ_id = *succ_id;

                    let has_condition = shared.graph.condition_nodes.contains(&succ_id);

                    let remaining = {
                        if has_condition {
                            remaining_cond_nodes[node_info.slot][node_id_to_rem[succ_id as usize]]
                        } else {
                            remaining_nodes[node_info.slot][node_id_to_rem[succ_id as usize]]
                        }
                    };

                    if remaining == 0 {
                        continue;
                    }

                    let succ_factor = shared.node_cache[succ_id as usize].factor;
                    let node_factor = shared.node_cache[node_info.id as usize].factor;

                    let pred_count = shared.node_cache[succ_id as usize]
                        .pred_vec
                        .get(node_info.id as usize)
                        .cloned()
                        .unwrap_or(0);

                    let succ_indexes = {
                        if succ_factor == node_factor && pred_count <= 1 {
                            vec![node_info.index]
                        } else if !shared.graph.condition_nodes.contains(&succ_id) {
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
                        let succ_info =
                            NodeInfo::new(succ_id, node_info.slot, succ_index, node_info.index);

                        // Skip if this node has already been sent to the queue
                        if nodes_sent_to_queue[node_info.slot].contains(&succ_info) {
                            continue;
                        }

                        let dep_opt = dependency_map.decrease(&succ_info);
                        if let Some(dep) = dep_opt {
                            if dep == 0 {
                                if !shared.graph.condition_nodes.contains(&succ_id) {
                                    print_debug(|| {
                                        format!("Sent successor {:?} to ready channel", succ_info)
                                    });
                                    nodes_to_schedule.push(succ_info.clone());

                                    // Mark this node as sent to avoid double-sends
                                    nodes_sent_to_queue[node_info.slot].insert(succ_info);

                                    // Increase nodes_sent counter
                                    nodes_sent += 1;
                                    // DO NOT decrement remaining_nodes here - only when task completes
                                } else {
                                    let index = &shared
                                        .graph
                                        .condition_nodes
                                        .iter()
                                        .position(|&x| x == succ_id)
                                        .unwrap();
                                    if conditions_met(&shared, &succ_info, &cond_indexes[*index]) {
                                        print_debug(|| {
                                            format!(
                                                "Sent successor {:?} to ready channel",
                                                succ_info
                                            )
                                        });
                                        nodes_to_schedule.push(succ_info.clone());

                                        // Mark this node as sent to avoid double-sends
                                        nodes_sent_to_queue[node_info.slot].insert(succ_info);

                                        nodes_sent += 1;
                                        // DO NOT decrement remaining_cond_nodes here - only when task completes
                                    } else {
                                        print_debug(|| {
                                            format!(
                                                "Conditions not met for successor {:?}",
                                                succ_info
                                            )
                                        });
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
                }
                // Schedule Nodes
                Self::preparation(&shared, &nodes_to_schedule, core_offset);
                // print dependency map state
                print_debug(|| {
                    format!(
                        "Dependency map state after processing node {:?}:\n{:?}",
                        node_info, dependency_map
                    )
                });

                // Check for stream completion - only process each slot once
                complete_iteration = false;
                if nodes_sent == 0 && !completed_slots.contains(&node_info.slot) {
                    // Check if all nodes in this slot have been processed
                    let all_nodes_processed = remaining_nodes[node_info.slot]
                        .iter()
                        .all(|&count| count == 0);

                    if all_nodes_processed {
                        print_debug(|| format!("Completed iteration at slot {}", node_info.slot));
                        // Mark this slot as completed
                        completed_slots.insert(node_info.slot);

                        // Reset dependency_map for this slot
                        dependency_map.reinit_slot(node_info.slot);
                        // Reinint remaining_proc_nodes for this slot
                        for node_id in 0..remaining_nodes[node_info.slot].len() {
                            remaining_nodes[node_info.slot][node_id] =
                                shared.graph.nodes[node_id].factor;
                        }
                        // Clear nodes_sent_to_queue for this slot for new iteration
                        nodes_sent_to_queue[node_info.slot].clear();

                        complete_iteration = true;
                    }
                }
                let end_time = shared.time_buffer.measure_time();

                if let Some(rec) = &shared.recorder {
                    let job_id = shared.job_counter.fetch_add(1, Ordering::SeqCst);
                    let end_ns = shared.base_instant.elapsed().as_nanos();
                    let mut map = rec.lock().unwrap();
                    let vec = map.entry(shared.slots).or_insert_with(Vec::new);
                    vec.push(Record {
                        job_id,
                        start_ns,
                        end_ns,
                        worker: core_offset,
                        task_id: IdType::MAX,
                        index: 0,
                    });
                }

                let duration = shared.time_buffer.measure_duration(start_time, end_time);
                shared.time_buffer.add_task_time(
                    shared.slots,
                    "Resolution Thread",
                    usize::MAX,
                    duration,
                );
                if complete_iteration {
                    // Add initial nodes for new iteration
                    if process_slot_completion(&shared, node_info.slot) {
                        // Remove from completed set since we're starting again
                        completed_slots.remove(&node_info.slot);
                        let init_nodes = initial_nodes(&shared, vec![node_info.slot]);
                        let mut nodes_to_schedule = Vec::new();
                        for node_info in init_nodes {
                            nodes_to_schedule.push(node_info);
                        }
                        Self::preparation(&shared, &nodes_to_schedule, core_offset);
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
            let stream_use = self.shared.slots; // initialized +1 in init_results
            for post_node in post_nodes {
                for index in 0..post_node.factor {
                    let mut node_info = NodeInfo::new(post_node.id, stream_use, index, 0);
                    node_info.set_post_node(true);

                    let arg_vec =
                        parse_args(&self.shared, &post_node.args, index, stream_use, 0, None);

                    let func = post_node.func_ptr;

                    send_to_scheduler(&self.shared, &node_info, arg_vec, func);
                }
                print_debug(|| format!("Added post node: {}", post_node.name));
                // Wait until all are completed by checking node_results
                let mut completed_count = 0;
                while completed_count < post_node.factor {
                    sleep(Duration::from_millis(10));
                    completed_count = 0;
                    let results_read = self.shared.node_results.read().unwrap();
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
        let mut node_results_lock = self.shared.node_results.write().unwrap();
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
        let scheduler_guard = match self.shared.scheduler.read() {
            Ok(guard) => guard,
            Err(e) => {
                eprintln!("Failed to acquire scheduler lock: {}", e);
                return;
            }
        };

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
            if let Ok(map) = rec.lock() {
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
                                    self.shared.core_offset.load(Ordering::SeqCst),
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
            }
        } else {
            println!("Runtime: recorder not enabled");
        }
    }
}
