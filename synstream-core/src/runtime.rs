use core_affinity;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, RwLock};
use std::thread::{sleep, spawn};
use std::time::{Duration, Instant};

use crate::debug::print_debug;
use crate::graph::*;
use crate::graph_struct::*;
use crate::scheduler::{Scheduler, SchedulerImpl};
use crate::time_buffer::TimeBufferManager;
use crate::{buffers::*, IdType};
use synstream_types::*;

/// Shared data across all SynStream threads - immutable or internally synchronized
pub struct SharedData {
    // Immutable data
    graph: Graph,
    total_nodes_per_stream: usize,
    slots: usize,
    max_streams: usize,
    max_runtime: Option<u64>,

    // Internally synchronized data
    node_results: Arc<RwLock<VecMap<CmTypes>>>,
    stream_complete_counter: Arc<AtomicUsize>,
    stream_completion_counts: Arc<RwLock<Vec<AtomicUsize>>>,
    available_stream_slots: Arc<RwLock<Vec<usize>>>,
    time_buffer: Arc<RwLock<TimeBufferManager>>,

    // Shared between threads
    scheduler: Arc<RwLock<Option<Arc<SchedulerImpl>>>>,
    completed_tx: Arc<RwLock<Option<Sender<(NodeInfo, CmTypes)>>>>,
    workers: Arc<AtomicUsize>,
}

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
    ) -> SynRt {
        let total_nodes = app_graph.total_executed_nodes();
        print_debug(&format!(
            "Total nodes to execute per stream: {}",
            total_nodes
        ));

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

        // Set the fields of the struct's graphs copy - one for each stream
        // Create an additional graph as a static copy

        let shared = Arc::new(SharedData {
            graph: app_graph.clone(),
            total_nodes_per_stream: total_nodes,
            slots,
            max_streams,
            max_runtime,
            node_results: Arc::new(RwLock::new(VecMap::new(CmTypes::Init))),
            stream_complete_counter: Arc::new(AtomicUsize::new(0)),
            stream_completion_counts,
            available_stream_slots,
            time_buffer: Arc::new(RwLock::new(TimeBufferManager::new_async(
                slots + 1,
                use_rdtsc,
            ))),
            scheduler: Arc::new(RwLock::new(None)),
            completed_tx: Arc::new(RwLock::new(None)),
            workers: Arc::new(AtomicUsize::new(1)), // Will be set in run()
        });

        SynRt { shared }
    }

    pub fn run(&mut self, scheduler: SchedulerImpl) {
        // Overwrite workers
        self.shared
            .workers
            .store(scheduler.workers(), Ordering::SeqCst);

        // create completed channel
        let (completed_tx, completed_rx) = std::sync::mpsc::channel::<(NodeInfo, CmTypes)>();
        {
            let mut tx_lock = self.shared.completed_tx.write().unwrap();
            *tx_lock = Some(completed_tx);
        }
        // create ready channel
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<NodeInfo>();
        // Store scheduler
        {
            let mut scheduler_lock = self.shared.scheduler.write().unwrap();
            *scheduler_lock = Some(Arc::new(scheduler));
        }

        // Initialize node_results
        self.init_results(self.shared.slots);

        // Get available cores and select one for pinning both threads
        let core_ids = core_affinity::get_core_ids().unwrap_or_default();
        let target_core = if !core_ids.is_empty() {
            // Use the last core to avoid interfering with main thread
            Some(core_ids[core_ids.len() - 1])
        } else {
            print_debug("No cores available for affinity setting");
            None
        };

        // Spawn preparation thread
        let shared_for_prep = Arc::clone(&self.shared);
        let target_core_prep = target_core;
        let preparation_handle = spawn(move || {
            // Pin this thread to the selected core
            if let Some(core) = target_core_prep {
                if core_affinity::set_for_current(core) {
                    print_debug(&format!("Preparation thread pinned to core {:?}", core));
                } else {
                    print_debug("Failed to pin preparation thread to core");
                }
            }
            Self::preparation(shared_for_prep, ready_rx);
        });
        print_debug("Preparation thread spawned");

        // Spawn resolution thread
        let shared_for_resolution = Arc::clone(&self.shared);
        let target_core_resolution = target_core;
        let resolution_handle = spawn(move || {
            // Pin this thread to the same core as the preparation thread
            if let Some(core) = target_core_resolution {
                if core_affinity::set_for_current(core) {
                    print_debug(&format!("Resolution thread pinned to core {:?}", core));
                } else {
                    print_debug("Failed to pin resolution thread to core");
                }
            }
            Self::resolution(shared_for_resolution, completed_rx, ready_tx);
        });
        print_debug("Resolution thread spawned");

        // Initiate synstream-runtime timing
        let time_read = self.shared.time_buffer.read().unwrap();
        time_read.start_slot_processing(self.shared.slots);
        drop(time_read);

        let start_time = Instant::now();
        // Check for max_runtime
        print_debug("Max runtime check started");
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
                            tx.send((NodeInfo::new(IdType::MAX, 0, 0), CmTypes::None))
                                .unwrap();
                        }
                    }
                    break;
                }
                sleep(Duration::from_secs(2));
            }
        }

        // Wait for threads to finish
        preparation_handle.join().unwrap();
        resolution_handle.join().unwrap();

        let time_read = self.shared.time_buffer.read().unwrap();
        let _ = time_read.finish_slot_processing(self.shared.slots);
        drop(time_read);
    }
}

// Execution Threads
impl SynRt {
    fn preparation(shared: Arc<SharedData>, ready_rx: Receiver<NodeInfo>) {
        // Gathers arguments and sends node to scheduler

        while let Ok(node_info) = ready_rx.recv() {
            let time_read = shared.time_buffer.read().unwrap();
            let start_time = time_read.measure_time();
            drop(time_read);

            print_debug(&format!("Preparing {:?}", node_info));

            let node = &shared.graph.nodes[node_info.id as usize];

            let arg_vec = Self::create_node_args(&shared, node, node_info.index, node_info.slot);

            if !arg_vec.is_empty() {
                let node_id = NodeInfo::new(node_info.id, node_info.slot, node_info.index);
                // Schedule Task
                Self::send_to_scheduler(&shared, node_id, arg_vec);
            }

            let time_read = shared.time_buffer.read().unwrap();
            let end_time = time_read.measure_time();
            let duration = time_read.measure_duration(start_time, end_time);
            time_read.add_task_time(shared.slots, "Preparation Thread", duration);
            drop(time_read);
        }
    }

    fn resolution(
        shared: Arc<SharedData>,
        completed_rx: Receiver<(NodeInfo, CmTypes)>,
        ready_tx: Sender<NodeInfo>,
    ) {
        // Store how many nodes of each type per slot are completed
        let mut completed_count_map: Vec<Vec<usize>> = vec![Vec::new(); shared.slots];
        for node_id in 0..shared.graph.nodes.len() {
            for slot in 0..shared.slots {
                completed_count_map[slot].insert(node_id, 0);
            }
        }

        let nodes = &shared.graph.nodes;
        let dependency_count_vec: Vec<usize> = shared.graph.dependency_count_vec();
        let mut dependency_map = VecMap::new(0);
        dependency_map.init_map(&nodes, shared.slots, Some(dependency_count_vec));
        print_debug(&format!(
            "Initialized dependency map:\n{}",
            dependency_map.print_map()
        ));

        let (condition_nodes, arg_indexes) = shared.graph.get_condition_nodes();

        // Find and send initial nodes to ready channel
        let slot_vec: Vec<usize> = (0..shared.slots).collect();
        let init_nodes = Self::init_nodes(&shared, slot_vec);
        for node_info in init_nodes {
            ready_tx.send(node_info).unwrap();
        }

        // Process completed nodes
        while let Ok((mut node_info, result)) = completed_rx.recv() {
            let time_read = shared.time_buffer.read().unwrap();
            let start_time = time_read.measure_time();
            drop(time_read);
            // Unwrap node_id
            let node_id = node_info.id;
            let node_index;
            let slot;
            let post_node = node_info.post_node;

            if node_id == IdType::MAX {
                println!("Exit signal received, stopping process_completed thread.");
                return;
            }

            print_debug(&format!("Processing Completed {:?}", node_info));

            if post_node {
                // Store Result
                let mut res_lock = shared.node_results.write().unwrap();
                res_lock.set(&node_info, result);
                drop(res_lock);
                continue;
            }

            // Get Id function and validate slot
            slot = Self::process_id_function(&shared, &node_info, &result);

            let current_completed = completed_count_map[slot][node_id as usize];

            // Check if all required nodes of this type are already completed
            let factor = shared.graph.nodes[node_id as usize].factor;
            if current_completed < factor {
                node_index = {
                    if condition_nodes.contains(&node_id) {
                        current_completed
                    } else {
                        node_info.index
                    }
                };
                completed_count_map[slot][node_id as usize] += 1;

                // store result
                print_debug(&format!(
                    "Storing result {:?} for {:?} at index {}",
                    result, node_info, node_index
                ));
                node_info.index = node_index; // Update index in case of condition node
                let mut res_lock = shared.node_results.write().unwrap();
                res_lock.set(&node_info, result);
                drop(res_lock);

                // Increment the completion count for this slot
                let completion_counts = shared.stream_completion_counts.read().unwrap();
                let current_count = completion_counts[slot].fetch_add(1, Ordering::SeqCst) + 1;
                drop(completion_counts);

                // Check if this stream iteration is complete
                if current_count >= shared.total_nodes_per_stream {
                    println!(
                        "Completed iteration at slot {} with {} nodes",
                        slot, current_count,
                    );

                    let new_iteration = Self::process_slot_completion(&shared, slot);
                    // Reset completed_count_map for this slot
                    for node_id in 0..completed_count_map[slot].len() {
                        completed_count_map[slot][node_id] = 0;
                    }
                    // Add initial nodes for new iteration
                    if new_iteration {
                        let init_nodes = Self::init_nodes(&shared, vec![slot]);
                        for node_info in init_nodes {
                            ready_tx.send(node_info).unwrap();
                        }
                    }
                } else {
                    // Add successors to pending
                    let successors: Vec<(IdType, Vec<usize>)> =
                        shared.graph.find_successors(node_id, node_index);
                    print_debug(&format!(
                        "{:?} with index {} has successors: {:?}",
                        node_info, node_index, successors
                    ));
                    for (succ_id, idxs) in successors {
                        for idx in idxs {
                            let succ_info = NodeInfo::new(succ_id, slot, idx);
                            let dep_opt = dependency_map.decrease(&succ_info);
                            if let Some(dep) = dep_opt {
                                if dep == 0 {
                                    if !condition_nodes.contains(&succ_id) {
                                        ready_tx.send(succ_info).unwrap();
                                    } else {
                                        let index = condition_nodes
                                            .iter()
                                            .position(|&x| x == succ_id)
                                            .unwrap();
                                        let arg_idx = &arg_indexes[index];
                                        if Self::conditions_met(&shared, &succ_info, arg_idx) {
                                            ready_tx.send(succ_info).unwrap();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            let time_read = shared.time_buffer.read().unwrap();
            let end_time = time_read.measure_time();
            let duration = time_read.measure_duration(start_time, end_time);
            time_read.add_task_time(shared.slots, "Resolution Thread", duration);
            drop(time_read);
        }
    }
}

// Helper Functions
impl SynRt {
    fn send_to_scheduler(shared: &Arc<SharedData>, node_info: NodeInfo, arg_vec: Vec<CmTypes>) {
        let nodes = {
            if node_info.post_node {
                // Use the static graph for post nodes
                &shared.graph.post_nodes.as_ref().unwrap()
            } else {
                // Use the appropriate graph for this slot
                &shared.graph.nodes
            }
        };

        let node = &nodes[node_info.id as usize];

        let error = format!(
            "Node {} with index {} has no function pointer",
            node_info.id, node_info.index
        );
        let func: CmPtr = node.func_ptr.expect(error.as_str());

        // Schedule Task
        let completed_tx_clone = {
            let tx_lock = shared.completed_tx.read().unwrap();
            tx_lock.as_ref().unwrap().clone()
        };
        let time_buffer_clone = shared.time_buffer.clone();
        let node_name = shared.graph.nodes[node_info.id as usize].name.clone();

        let task = Self::create_task(
            func,
            arg_vec,
            node_info,
            node_name,
            completed_tx_clone,
            time_buffer_clone,
        );
        let scheduler = {
            let scheduler_lock = shared.scheduler.read().unwrap();
            scheduler_lock.as_ref().unwrap().clone()
        };
        scheduler.spawn_task(task);
    }

    fn conditions_met(
        shared: &Arc<SharedData>,
        node_info: &NodeInfo,
        arg_indexes: &Vec<usize>,
    ) -> bool {
        let node = &shared.graph.nodes[node_info.id as usize];
        let mut is_ready = true;

        for arg_idx in arg_indexes {
            let arg = &node.args[*arg_idx];
            let init_condition: &InitCondition = &arg.init_condition.as_ref().unwrap();
            // We assume condition has a single predecessor
            let result =
                Self::collect_arg_result(arg, node_info.index, node_info.slot, None, shared)
                    .unwrap()[0]
                    .clone();

            print_debug(&format!(
                "Evaluating condition {:?} for node {:?} with predecessor result: {:?}",
                init_condition, node_info, result
            ));

            let eval = init_condition.evaluate(result.clone());
            if !eval {
                is_ready = false;
                break;
            }
        }
        is_ready
    }

    fn process_slot_completion(shared: &Arc<SharedData>, slot: usize) -> bool {
        // Complete timing
        let time_read = shared.time_buffer.read().unwrap();
        let _ = time_read.finish_slot_processing(slot);
        drop(time_read);

        let mut new_iteration = false;
        // Increment global completion counter
        let new_counter = shared
            .stream_complete_counter
            .fetch_add(1, Ordering::SeqCst)
            + shared.slots;

        // Check if we should start a new iteration
        if new_counter < shared.max_streams {
            print_debug(&format!("Starting new iteration {}", new_counter));
            new_iteration = true;

            // Release the slot
            Self::release_slot(shared, slot);

            // Reset the completion count for this stream
            let completion_counts = shared.stream_completion_counts.read().unwrap();
            completion_counts[slot].store(0, Ordering::SeqCst);
            drop(completion_counts);

            // Clear completed nodes for this stream to allow restart
            let mut result_lock = shared.node_results.write().unwrap();
            result_lock.reinit_slot(slot);
            drop(result_lock);
        }
        new_iteration
    }

    fn assign_stream_to_available_slot(shared: &Arc<SharedData>, stream: usize) -> usize {
        let mut available_slots = shared.available_stream_slots.write().unwrap();

        // Check if this streams is already mapped to a slot
        let mut av_slot_id: usize = usize::MAX;
        for (slot_id, &real_stream) in available_slots.iter().enumerate() {
            if real_stream == stream {
                print_debug(&format!(
                    "Stream: {} is already assigned to slot {}",
                    stream, slot_id
                ));
                return slot_id;
            } else if real_stream == std::usize::MAX && av_slot_id == std::usize::MAX {
                av_slot_id = slot_id;
            }
        }

        // Assign this stream to the available slot

        if av_slot_id == std::usize::MAX {
            // Find first available slot
            for (slot_id, &real_stream) in available_slots.iter().enumerate() {
                if real_stream == std::usize::MAX {
                    av_slot_id = slot_id;
                    break;
                }
            }
        }

        if av_slot_id == std::usize::MAX {
            panic!("No available stream slots for stream: {}", stream);
        }

        available_slots[av_slot_id] = stream; // Mark as busy
        print_debug(&format!(
            "Assigned stream: {} to available slot {}",
            stream, av_slot_id
        ));
        // Start slot timing
        let time_read = shared.time_buffer.read().unwrap();
        time_read.start_slot_processing(av_slot_id);
        drop(time_read);
        return av_slot_id;
    }

    fn release_slot(shared: &Arc<SharedData>, slot: usize) {
        let mut available_slots = shared.available_stream_slots.write().unwrap();

        let old_stream = available_slots[slot].clone();
        available_slots[slot] = std::usize::MAX; // Mark as available
        print_debug(&format!(
            "Released slot {} (had stream: {})",
            slot, old_stream
        ));
    }

    fn process_id_function(
        shared: &Arc<SharedData>,
        node_info: &NodeInfo,
        result: &CmTypes,
    ) -> usize {
        let mut slot = node_info.slot;

        let id_function_opt = shared.graph.id_function.clone();

        if let Some(id_function) = id_function_opt {
            let msg = "ID function is not set".to_string();
            let func_ptr = id_function.func_ptr.expect(&msg);
            let predecessor = id_function.predecessor;
            // Check if completed node is the predecessor
            if predecessor == node_info.id {
                let arg_vec = Self::parse_args(
                    shared,
                    &id_function.args,
                    node_info.index,
                    slot,
                    Some(result.clone()),
                );

                // Call the id function
                print_debug(&format!("Calling ID function for {:?}", node_info));
                let id_result = func_ptr(arg_vec);

                // Extract stream from the result
                if let Some(new_stream) = id_result.valid_number_to_usize() {
                    // Validate stream range
                    let current_counter = shared.stream_complete_counter.load(Ordering::SeqCst);
                    let max_allowed_stream = current_counter + shared.slots;

                    if new_stream >= max_allowed_stream {
                        panic!(
                                "ID function returned stream {} which exceeds maximum allowed {} (current_counter: {}, slots: {})",
                                new_stream, max_allowed_stream, current_counter, shared.slots
                            );
                    }

                    // Assign streams to an available stream slot
                    slot = Self::assign_stream_to_available_slot(shared, new_stream);
                    print_debug(&format!(
                        "ID function determined stream: {} assigned to slot: {}",
                        new_stream, slot
                    ));
                } else {
                    panic!("ID function did not return a valid number for stream");
                }
            }
        }
        return slot;
    }

    fn create_task(
        func: CmPtr,
        arg_vec: Vec<CmTypes>,
        node_info: NodeInfo,
        node_name: String,
        completed_tx: Sender<(NodeInfo, CmTypes)>,
        time_buf: Arc<RwLock<TimeBufferManager>>,
    ) -> impl FnOnce() {
        let task = move || {
            let time_read = time_buf.read().unwrap();
            let start_time = time_read.measure_time();
            drop(time_read);

            let result = func(arg_vec);

            if !node_info.post_node {
                let time_read = time_buf.read().unwrap();
                let end_time = time_read.measure_time();
                let duration = time_read.measure_duration(start_time, end_time);
                time_read.add_task_time(node_info.slot, &node_name, duration);
                drop(time_read);
            }
            // Send result through channel
            completed_tx.send((node_info, result)).unwrap();
        };
        task
    }

    fn create_node_args(
        shared: &Arc<SharedData>,
        node: &Node,
        node_index: usize,
        slot: usize,
    ) -> Vec<CmTypes> {
        let args = {
            // check if node is in loop_nodes
            // let loop_read = self.loop_nodes.read().unwrap();
            // let mut looping = false;
            // if loop_read.contains(&node.name.clone()) {
            //     // node is in loop_nodes
            //     looping = true;
            // }

            // let loop_opt = node.loop_args.as_ref();

            // if looping && loop_opt.is_some() {
            //     loop_opt.unwrap()
            // } else {
            //     &node.args
            // }
            &node.args
        };

        let arg_vec = Self::parse_args(shared, args, node_index, slot, None);

        arg_vec
    }

    fn parse_args(
        shared: &Arc<SharedData>,
        args: &Vec<Arg>,
        node_index: usize,
        slot: usize,
        custom_res: Option<CmTypes>,
    ) -> Vec<CmTypes> {
        let mut arg_vec = Vec::new();
        for arg in args.iter() {
            // continue if arg is a condition
            if arg.is_condition() {
                continue;
            }

            let result_opt =
                Self::collect_arg_result(arg, node_index, slot, custom_res.clone(), shared);
            if let Some(result) = result_opt {
                arg_vec.extend(result);
            }
        }
        arg_vec
    }

    fn collect_arg_result(
        arg: &Arg,
        node_index: usize,
        slot: usize,
        custom_res: Option<CmTypes>,
        shared: &Arc<SharedData>,
    ) -> Option<Vec<CmTypes>> {
        match &arg.type_ {
            CmTypes::Ref(obj_id) => {
                let obj_id = *obj_id;
                let init_objects = &shared.graph.init_objects.as_ref().unwrap();
                // Argument may be node index
                if obj_id == 0 {
                    // reserved for $index
                    return Some(vec![CmTypes::Usize(node_index)]);
                }
                // Argument may be worker num
                if obj_id == 1 {
                    // reserved for $workers
                    return Some(vec![CmTypes::Usize(shared.workers.load(Ordering::SeqCst))]);
                }

                // object may be either buffer indexed by node_index
                // or just variable indexed by 0
                let obj_vec = &init_objects[obj_id as usize];
                let obj = {
                    if obj_vec.len() > 1 {
                        // If the object is a buffer, get the object according to node_index
                        let index = node_index % obj_vec.len();
                        obj_vec[index].clone()
                    } else {
                        // If the object is a variable, get the first element
                        obj_vec[0].clone()
                    }
                };
                return Some(vec![obj]);
            }
            CmTypes::Res(res_node_id) => {
                if let Some(ref custom_res) = custom_res {
                    return Some(vec![custom_res.clone()]);
                }
                let indices = arg
                    .predecessor
                    .as_ref()
                    .unwrap()
                    .indexes
                    .iter()
                    .map(|&x| {
                        // Get the predecessor node factor
                        let nodes = &shared.graph.nodes;
                        let pred_node: &Node =
                            &nodes[arg.predecessor.as_ref().unwrap().id as usize];
                        let pred_factor = pred_node.factor;

                        // Find the index of the node in the results
                        let new_index = find_pred_index(node_index, x, pred_factor);
                        new_index
                    })
                    .collect::<Vec<usize>>();

                let mut result_vec = Vec::new();
                for dep_idx in indices.iter() {
                    // for each task index, retrieve the
                    // corresponding results
                    // (must exist since they are completed)
                    let res_read = shared.node_results.read().unwrap();
                    let node_info = NodeInfo::new(*res_node_id as IdType, slot, *dep_idx);

                    let result = res_read.get(&node_info).unwrap();
                    result_vec.push(result);
                }
                return Some(result_vec);
            }
            CmTypes::Barrier(_) => {
                // Barrier does not require any arguments
                return None;
            }
            _ => return Some(vec![arg.type_.clone()]),
        }
    }

    fn init_nodes(shared: &Arc<SharedData>, slots: Vec<usize>) -> Vec<NodeInfo> {
        let mut node_infos = Vec::new();
        for slot in slots {
            let initial_nodes = shared.graph.initial_nodes.clone();
            for node_id in initial_nodes {
                let node = &shared.graph.nodes[node_id as usize];
                let node_factor = node.factor;
                let indexes: Vec<usize> = (0..node_factor).collect();
                for index in indexes {
                    let node_info = NodeInfo::new(node_id, slot, index);
                    node_infos.push(node_info);
                }
            }
        }
        node_infos
    }

    fn schedule_post_nodes(&mut self) {
        let nodes = &self.shared.graph.post_nodes;
        if let Some(post_nodes) = nodes {
            let stream_use = self.shared.slots; // initialized +1 in init_results
            for post_node in post_nodes {
                for index in 0..post_node.factor {
                    let mut node_info = NodeInfo::new(post_node.id, stream_use, index);
                    node_info.set_post_node(true);

                    let arg_vec =
                        Self::create_node_args(&self.shared, post_node, index, stream_use);
                    Self::send_to_scheduler(&self.shared, node_info, arg_vec);
                }
                print_debug(&format!("Added post node: {}", post_node.name));
                // Wait until all are completed by checking node_results
                let mut completed_count = 0;
                while completed_count < post_node.factor {
                    sleep(Duration::from_millis(10));
                    completed_count = 0;
                    let results_read = self.shared.node_results.read().unwrap();
                    for i in 0..post_node.factor {
                        let node_info = NodeInfo::new(post_node.id, stream_use, i);
                        if results_read.result_exists(&node_info) {
                            completed_count += 1;
                        }
                    }
                    drop(results_read);
                }
            }
            print_debug("All post-nodes completed");
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
        let time_read = self.shared.time_buffer.read().unwrap();
        time_read.print_stats(bench_name, out_file);
    }
}
