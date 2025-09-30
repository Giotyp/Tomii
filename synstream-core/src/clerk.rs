use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, RwLock};
use std::thread::{sleep, spawn};
use std::time::{Duration, Instant};

use crate::clerk_structs::*;
use crate::debug::print_debug;
use crate::graph::*;
use crate::graph_struct::*;
use crate::scheduler::{Scheduler, SchedulerImpl};
use crate::time_buffer::TimeBuffer;
use synstream_types::*;

#[derive(Clone)]
pub struct Clerk {
    // Keep a copy of the graph
    graph: Graph,
    node_results: Arc<RwLock<Buffer<CmTypes>>>,
    scheduler: Option<Arc<SchedulerImpl>>,
    completed_tx: Option<Sender<(NodeID, CmTypes)>>,
    stream_complete_counter: Arc<AtomicUsize>,
    // Persistent completion counters for each stream
    stream_completion_counts: Arc<RwLock<Vec<AtomicUsize>>>,
    // Track available stream slots (true = available, false = busy)
    // (bool, usize) where usize indicates the real stream_id received
    // from the ID function
    available_stream_slots: Arc<RwLock<Vec<(bool, usize)>>>,
    // Map stream to actual stream slot
    stream_to_slot_mapping: Arc<RwLock<HashMap<usize, usize>>>,
    total_nodes_per_stream: usize,
    slots: usize,
    workers: usize,
    max_streams: usize,
    max_runtime: Option<u64>,
    time_buffer: Arc<RwLock<TimeBuffer>>,
}

impl Clerk {
    pub fn new(
        app_graph: &Graph,
        slots: usize,
        max_streams: usize,
        max_runtime: Option<u64>,
        use_rdtsc: bool,
    ) -> Clerk {
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
            available_write.push((true, std::usize::MAX)); // (available, real stream id)
        }
        drop(available_write);

        // Set the fields of the struct's graphs copy - one for each stream
        // Create an additional graph as a static copy

        Clerk {
            graph: app_graph.clone(),
            node_results: Arc::new(RwLock::new(Buffer::new(CmTypes::None()))),
            scheduler: None,
            completed_tx: None,
            stream_complete_counter: Arc::new(AtomicUsize::new(0)),
            stream_completion_counts,
            available_stream_slots,
            stream_to_slot_mapping: Arc::new(RwLock::new(HashMap::new())),
            total_nodes_per_stream: total_nodes,
            slots,
            workers: 1, // Will be set in run()
            max_streams,
            max_runtime,
            time_buffer: Arc::new(RwLock::new(TimeBuffer::new(slots + 1, use_rdtsc))),
        }
    }

    pub fn run(&mut self, scheduler: SchedulerImpl) {
        // Overwrite workers
        self.workers = scheduler.workers();

        // create completed channel
        let (completed_tx, completed_rx) = std::sync::mpsc::channel::<(NodeID, CmTypes)>();
        self.completed_tx = Some(completed_tx);
        // create checker channel
        let (checker_tx, checker_rx) = std::sync::mpsc::channel::<NodeID>();
        // Store scheduler
        self.scheduler = Some(Arc::new(scheduler));

        // Initialize node_results
        self.init_results(self.slots);

        // Spawn thread to handle set_ready_nodes
        let mut clerk_for_ready = self.clone();
        let ready_handle = spawn(move || {
            clerk_for_ready.set_ready_nodes(checker_rx);
        });
        print_debug("Ready thread spawned");

        // Spawn a thread to handle completed nodes
        let mut clerk_for_completed = self.clone();
        let complete_handle =
            spawn(move || clerk_for_completed.process_completed(completed_rx, checker_tx));
        print_debug("Completion thread spawned");

        // Find and send initial nodes to ready channel
        let slot_vec: Vec<usize> = (0..self.slots).collect();
        self.add_init_nodes(slot_vec);

        // Initiate clerk-thread timing
        let mut time_write = self.time_buffer.write().unwrap();
        time_write.start_slot_processing(self.slots);
        drop(time_write);

        let start_time = Instant::now();
        // Check for max_runtime
        print_debug("Max runtime check started");
        if let Some(max_runtime) = self.max_runtime {
            loop {
                if start_time.elapsed().as_secs() > max_runtime {
                    // set exit signal
                    println!("Max runtime reached, exiting...");
                    // Process post-nodes if any
                    println!("Processing possible post-nodes...");
                    self.schedule_post_nodes();
                    // Close completed channel
                    self.completed_tx
                        .as_ref()
                        .unwrap()
                        .clone()
                        .send((NodeID::new("exit".to_string(), 0, 0), CmTypes::None()))
                        .unwrap();
                    break;
                }
                sleep(Duration::from_secs(2));
            }
        }

        // Wait for threads to finish
        ready_handle.join().unwrap();
        complete_handle.join().unwrap();

        let mut time_write = self.time_buffer.write().unwrap();
        time_write.finish_slot_processing(self.slots);
        drop(time_write);
    }
}

// Execution Threads
impl Clerk {
    fn set_ready_nodes(&mut self, checker_rx: Receiver<NodeID>) {
        // Checks if the node is ready to be scheduled and
        // adds it to the ready_nodes list

        while let Ok(node_id) = checker_rx.recv() {
            let time_read = self.time_buffer.read().unwrap();
            let start_time = time_read.measure_time();
            drop(time_read);

            let node_name = node_id.name.clone();
            let node_index = node_id.index;
            let slot = node_id.slot;

            if node_name == "exit" {
                println!("Exit signal received, stopping set_ready_nodes thread.");
                return;
            }

            let nodes = &self.graph.nodes;
            let node = nodes.get(&node_name).unwrap();

            let arg_vec = self.check_ready_node(node, node_index, slot);

            if !arg_vec.is_empty() {
                let node_id = NodeID::new(node_name.clone(), slot, node_index);
                print_debug(&format!("{:?} is ready to be scheduled", node_id));
                // Schedule Task
                self.send_to_scheduler(node_id, arg_vec);
            }

            let mut time_write = self.time_buffer.write().unwrap();
            let end_time = time_write.measure_time();
            let duration = time_write.measure_duration(start_time, end_time);
            time_write.add_task_time(self.slots, "Ready-Check Thread", duration);
            drop(time_write);
        }
    }

    fn process_completed(
        &mut self,
        completed_rx: Receiver<(NodeID, CmTypes)>,
        checker_tx: Sender<NodeID>,
    ) {
        let nodes_names = {
            let nodes_map = &self.graph.nodes;
            nodes_map.keys().cloned().collect::<Vec<String>>()
        };

        // Create a hasmap to store how many nodes of each type per slot are completed
        let mut completed_count_map: Vec<HashMap<String, usize>> = vec![HashMap::new(); self.slots];
        for name in &nodes_names {
            for slot in 0..self.slots {
                completed_count_map[slot].insert(name.clone(), 0);
            }
        }
        // Process completed nodes
        while let Ok((node_id, result)) = completed_rx.recv() {
            let time_read = self.time_buffer.read().unwrap();
            let start_time = time_read.measure_time();
            drop(time_read);
            // Unwrap node_id
            let node_name = node_id.name.clone();
            let node_index;
            let slot;
            let post_node = node_id.post_node;

            if node_name == "exit" {
                println!("Exit signal received, stopping process_completed thread.");
                return;
            }

            print_debug(&format!("Processing Completed {:?}", node_id));

            if post_node {
                // Store Result
                let mut res_lock = self.node_results.write().unwrap();
                // Add dummy result in case of None return
                let res = {
                    match result {
                        CmTypes::None() => CmTypes::Bool(true),
                        _ => result,
                    }
                };
                res_lock.add_element_index(&node_name, node_id.index, res, self.slots);
                drop(res_lock);
                continue;
            }

            // Get Id function and validate slot
            slot = self.process_id_function(&node_id, &result);

            let current_completed = completed_count_map[slot].get(&node_name).unwrap();

            // Check if all required nodes of this type are already completed
            let factor = {
                let nodes_map = &self.graph.nodes;
                let node = nodes_map.get(&node_name).unwrap();
                node.factor
            };
            if *current_completed < factor {
                node_index = *current_completed;
                let completed_write = completed_count_map[slot].get_mut(&node_name).unwrap();
                *completed_write += 1;

                // store result
                let mut res_lock = self.node_results.write().unwrap();
                res_lock.add_element_index(&node_name, node_index, result, slot);
                drop(res_lock);

                print_debug(&format!(
                    "Completed Node {} with index: {} at slot {}",
                    node_id.name, node_index, slot
                ));

                // Increment the completion count for this slot
                let completion_counts = self.stream_completion_counts.read().unwrap();
                let current_count = completion_counts[slot].fetch_add(1, Ordering::SeqCst) + 1;
                drop(completion_counts);

                // Check if this stream iteration is complete
                if current_count >= self.total_nodes_per_stream {
                    print_debug(&format!(
                        "Completed iteration at slot {} with {} nodes",
                        slot, current_count
                    ));

                    self.process_slot_completion(slot);
                    // Reset completed_count_map for this slot
                    for name in &nodes_names {
                        completed_count_map[slot].insert(name.clone(), 0);
                    }
                } else {
                    // Add successors to pending
                    let successors: Vec<(String, Vec<usize>, bool)> =
                        self.graph.find_successors(&node_name, node_index);
                    for (succ_name, idxs, has_barrier) in successors {
                        // Check for barrier
                        if (has_barrier && self.barrier_resolved(&succ_name, slot)) || !has_barrier
                        {
                            for idx in idxs {
                                let succ_id = NodeID::new(succ_name.clone(), slot, idx);
                                checker_tx.send(succ_id).unwrap();
                            }
                        }
                    }
                }
            }

            let mut time_write = self.time_buffer.write().unwrap();
            let end_time = time_write.measure_time();
            let duration = time_write.measure_duration(start_time, end_time);
            time_write.add_task_time(self.slots, "Completion Thread", duration);
        }
    }
}

// Helper Functions
impl Clerk {
    fn send_to_scheduler(&self, node_id: NodeID, arg_vec: Vec<CmTypes>) {
        let time_read = self.time_buffer.read().unwrap();
        let start_time = time_read.measure_time();
        drop(time_read);

        // Unwrap node_id
        let node_name = node_id.name.clone();
        let node_index = node_id.index;
        let post_node = node_id.post_node;

        // Check for exit condition
        if node_name == "exit" {
            println!("Exit signal received, stopping schedule_nodes thread.");
            return;
        }

        let nodes = {
            if post_node {
                // Use the static graph for post nodes
                &self.graph.post_nodes.as_ref().unwrap()
            } else {
                // Use the appropriate graph for this slot
                &self.graph.nodes
            }
        };

        let node = nodes.get(&node_name.clone()).unwrap();

        let error = format!(
            "Node {} with index {} has no function pointer",
            node_name, node_index
        );
        let func: CmPtr = node.func_ptr.expect(error.as_str());

        // Schedule Task
        let completed_tx_clone = self.completed_tx.as_ref().unwrap().clone();
        let node_id_clone = node_id.clone();
        let time_buffer_clone = self.time_buffer.clone();

        let task = Self::create_task(
            func,
            arg_vec,
            node_id_clone,
            completed_tx_clone,
            time_buffer_clone,
        );
        print_debug(&format!("Sending to Exec {:?}", node_id));
        let scheduler = self.scheduler.as_ref().unwrap().clone();
        scheduler.spawn_task(task);

        let mut time_write = self.time_buffer.write().unwrap();
        let end_time = time_write.measure_time();
        let duration = time_write.measure_duration(start_time, end_time);
        time_write.add_task_time(self.slots, "Scheduler Thread", duration);
    }

    fn check_ready_node(&self, node: &Node, node_index: usize, slot: usize) -> Vec<CmTypes> {
        let mut is_ready = true;
        let arg_vec = self.create_node_args(node, node_index, slot);

        // Check for CmTypes::None() results
        for arg in arg_vec.iter() {
            if let CmTypes::None() = arg {
                is_ready = false;
                break;
            }
        }

        let mut arg_vec_index = 0;
        for arg in node.args.iter() {
            // Check if node has a condition
            let init_condition: Option<&InitCondition> = arg.init_condition.as_ref();
            if init_condition.is_none() {
                // Skip non-condition args and increment arg_vec_index if not a condition arg
                if !arg.is_condition() {
                    arg_vec_index += 1;
                }
                continue;
            }
            let init_condition: &InitCondition = init_condition.unwrap();

            // For condition args, we need to find the corresponding value
            // Conditions can reference either Ref or Res types
            let result = match &arg.type_ {
                CmTypes::Ref(_) | CmTypes::Res(_) => {
                    if arg_vec_index < arg_vec.len() {
                        &arg_vec[arg_vec_index]
                    } else {
                        // This shouldn't happen if parse_args is working correctly
                        is_ready = false;
                        break;
                    }
                }
                _ => {
                    // For other types, use the arg type directly
                    &arg.type_
                }
            };

            let eval = init_condition.evaluate(result.clone());
            if !eval {
                is_ready = false;
                break;
            }

            // Increment arg_vec_index only if this wasn't a pure condition
            if !arg.is_condition() {
                arg_vec_index += 1;
            }
        }

        if is_ready {
            arg_vec
        } else {
            vec![]
        }
    }

    fn process_slot_completion(&mut self, slot: usize) {
        // Complete timing
        let mut time_write = self.time_buffer.write().unwrap();
        time_write.finish_slot_processing(slot);
        drop(time_write);

        // Increment global completion counter
        let new_counter = self.stream_complete_counter.fetch_add(1, Ordering::SeqCst) + self.slots;

        // Check if we should start a new iteration
        if new_counter < self.max_streams {
            print_debug(&format!("Starting new iteration {}", new_counter));

            // Release the slot
            self.release_slot(slot);

            // Reset the completion count for this stream
            let completion_counts = self.stream_completion_counts.read().unwrap();
            completion_counts[slot].store(0, Ordering::SeqCst);
            drop(completion_counts);

            // Clear completed nodes for this stream to allow restart
            let mut result_lock = self.node_results.write().unwrap();
            result_lock.clear_slot(slot);
            drop(result_lock);

            // Re-add nodes for new iteration using the completed slot
            self.add_init_nodes(vec![slot]);
        }
    }

    fn assign_stream_to_available_slot(&mut self, stream: usize) -> usize {
        let mut available_slots = self.available_stream_slots.write().unwrap();
        let mut streams_mapping = self.stream_to_slot_mapping.write().unwrap();

        // Check if this streams is already mapped to a slot
        if let Some(&slot) = streams_mapping.get(&stream) {
            print_debug(&format!(
                "Stream: {} is already assigned to slot {}",
                stream, slot
            ));
            return slot;
        }

        // Find first available slot
        for (slot_id, &(available, _)) in available_slots.iter().enumerate() {
            if available {
                // Assign this stream to the available slot
                streams_mapping.insert(stream, slot_id);
                available_slots[slot_id] = (false, stream); // Mark as busy
                print_debug(&format!(
                    "Assigned stream: {} to available slot {}",
                    stream, slot_id
                ));
                // Start slot timing
                let mut time_write = self.time_buffer.write().unwrap();
                time_write.start_slot_processing(slot_id);
                drop(time_write);
                return slot_id;
            }
        }

        // If no slots available, panic
        panic!("No available stream slots for stream: {}", stream);
    }

    fn release_slot(&mut self, slot: usize) {
        let mut available_slots = self.available_stream_slots.write().unwrap();

        let old_stream = available_slots[slot].1.clone();
        available_slots[slot] = (true, std::usize::MAX); // Mark as available
        print_debug(&format!(
            "Released slot {} (had stream: {})",
            slot, old_stream
        ));
    }

    fn process_id_function(&mut self, node_id: &NodeID, result: &CmTypes) -> usize {
        let mut slot = node_id.slot;

        let id_function_opt = self.graph.id_function.clone();

        if let Some(id_function) = id_function_opt {
            let msg = "ID function is not set".to_string();
            let func_ptr = id_function.func_ptr.expect(&msg);
            let predecessor = &id_function.predecessor;
            // Check if completed node is the predecessor
            if predecessor == &node_id.name {
                let arg_vec =
                    self.parse_args(&id_function.args, node_id.index, slot, Some(result.clone()));

                // Call the id function
                print_debug(&format!("Calling ID function for {:?}", node_id));
                let id_result = func_ptr(arg_vec);

                // Extract stream from the result
                if let Some(new_stream) = id_result.valid_number_to_usize() {
                    // Validate stream range
                    let current_counter = self.stream_complete_counter.load(Ordering::SeqCst);
                    let max_allowed_stream = current_counter + self.slots;

                    if new_stream >= max_allowed_stream {
                        panic!(
                                "ID function returned stream {} which exceeds maximum allowed {} (current_counter: {}, slots: {})",
                                new_stream, max_allowed_stream, current_counter, self.slots
                            );
                    }

                    // Assign streams to an available stream slot
                    slot = self.assign_stream_to_available_slot(new_stream);
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
        node_id: NodeID,
        completed_tx: Sender<(NodeID, CmTypes)>,
        time_buf: Arc<RwLock<TimeBuffer>>,
    ) -> impl FnOnce() {
        let task = move || {
            let time_read = time_buf.read().unwrap();
            let start_time = time_read.measure_time();
            drop(time_read);

            let result = func(arg_vec);

            if !node_id.post_node {
                let mut time_write = time_buf.write().unwrap();
                let end_time = time_write.measure_time();
                let duration = time_write.measure_duration(start_time, end_time);
                time_write.add_task_time(node_id.slot, &node_id.name, duration);
                drop(time_write);
            }
            // Send result through channel
            completed_tx.send((node_id, result)).unwrap();
        };
        task
    }

    fn create_node_args(&self, node: &Node, node_index: usize, slot: usize) -> Vec<CmTypes> {
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

        let arg_vec = self.parse_args(args, node_index, slot, None);

        arg_vec
    }

    fn parse_args(
        &self,
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

            match &arg.type_ {
                CmTypes::Ref(obj_name) => {
                    let init_objects = &self.graph.init_objects.as_ref().unwrap();

                    // Argument may be node index
                    if obj_name == "$index" {
                        arg_vec.push(CmTypes::Usize(node_index));
                        continue;
                    }

                    // Argument may be worker num
                    if obj_name == "$workers" {
                        arg_vec.push(CmTypes::Usize(self.workers));
                        continue;
                    }

                    // object may be either buffer indexed by node_index
                    // or just variable indexed by 0
                    let msg = format!("Object {} not found in init_objects", obj_name);
                    let obj_vec = init_objects.get(obj_name).expect(msg.as_str());
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
                    arg_vec.push(obj);
                }
                CmTypes::Res(res_node) => {
                    if let Some(ref custom_res) = custom_res {
                        arg_vec.push(custom_res.clone());
                        continue;
                    }
                    let indices = arg
                        .predecessor
                        .as_ref()
                        .unwrap()
                        .indexes
                        .iter()
                        .map(|&x| {
                            // Get the predecessor node factor
                            let nodes = &self.graph.nodes;
                            let pred_node: &Node =
                                nodes.get(&arg.predecessor.as_ref().unwrap().name).unwrap();
                            let pred_factor = pred_node.factor;

                            // Find the index of the node in the results
                            let new_index = find_pred_index(node_index, x, pred_factor);
                            new_index
                        })
                        .collect::<Vec<usize>>();

                    for dep_idx in indices.iter() {
                        // for each task index, retrieve the
                        // corresponding results
                        // (must exist since they are completed)
                        let res_read = self.node_results.read().unwrap();

                        let result = res_read.search_node_idx(&res_node, *dep_idx, slot).unwrap();
                        arg_vec.push(result);
                    }
                }
                CmTypes::Barrier(_) => {
                    // Barrier does not require any arguments
                }
                _ => {
                    arg_vec.push(arg.type_.clone());
                }
            }
        }
        arg_vec
    }

    fn barrier_resolved(&self, node_name: &str, slot: usize) -> bool {
        let barrier_nodes = self.graph.get_barriers(node_name);
        // Check if all barrier nodes are resolved
        let results_read = self.node_results.read().unwrap();
        barrier_nodes.iter().all(|(barrier_name, indices)| {
            indices
                .iter()
                .all(|index| results_read.result_exists(barrier_name, *index, slot))
        })
    }

    fn add_init_nodes(&self, slots: Vec<usize>) {
        for slot in slots {
            let initial_nodes = self.graph.initial_nodes.clone();
            for node_name in initial_nodes {
                let node = self
                    .graph
                    .nodes
                    .get(&node_name)
                    .expect("Initial node not found in graph");
                let node_factor = node.factor;
                let indexes: Vec<usize> = (0..node_factor).collect();
                for index in indexes {
                    let node_id = NodeID::new(node_name.clone(), slot, index);
                    print_debug(&format!("Initial Node {:?} is ready", node_id));

                    let arg_vec = self.create_node_args(node, index, slot);

                    self.send_to_scheduler(node_id, arg_vec);
                }
            }
        }
    }

    fn schedule_post_nodes(&mut self) {
        // Use the static graph to get post_nodes_map
        let nodes = &self.graph.post_nodes;
        if let Some(post_nodes) = nodes {
            let stream_use = self.slots; // initialized +1 in init_results
            for post_node in post_nodes.values() {
                for index in 0..post_node.factor {
                    let mut node_id = NodeID::new(post_node.name.clone(), stream_use, index);
                    node_id.set_post_node(true);

                    let arg_vec = self.create_node_args(post_node, index, stream_use);
                    self.send_to_scheduler(node_id, arg_vec);
                }
                print_debug(&format!("Added post node: {}", post_node.name));
                // Wait until all are completed by checking node_results
                let mut completed_count = 0;
                while completed_count < post_node.factor {
                    sleep(Duration::from_millis(10));
                    completed_count = 0;
                    let results_read = self.node_results.read().unwrap();
                    for i in 0..post_node.factor {
                        if results_read.result_exists(&post_node.name, i, stream_use) {
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
        let nodes = &self.graph.nodes;
        let mut node_results_lock = self.node_results.write().unwrap();
        node_results_lock.clear_buffer();
        node_results_lock.init_buffer(&nodes, slots);

        // Initialize post_nodes if any
        let post_nodes_opt = &self.graph.post_nodes;
        if let Some(post_nodes) = post_nodes_opt {
            node_results_lock.add_buffer(&post_nodes);
        }
    }

    pub fn print_statistics(&self, bench_name: &str, out_file: Option<&str>) {
        let time_read = self.time_buffer.read().unwrap();
        time_read.print_stats(bench_name, out_file);
    }
}
