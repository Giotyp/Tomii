use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, RwLock};
use std::thread::{sleep, spawn};
use std::time::{Duration, Instant};

use crate::debug::print_debug;
use crate::graph_struct::*;
use crate::scheduler::{Scheduler, SchedulerImpl};
use crate::time_buffer::TimeBuffer;
use synstream_types::*;

#[derive(Clone)]
pub struct Clerk {
    // Keep a vector of graph copies , one for each stream
    graphs: Arc<RwLock<Vec<Graph>>>,
    pending_nodes: Arc<RwLock<Vec<NodeID>>>,
    removed_cond_nodes: Arc<RwLock<HashMap<(String, usize), Vec<usize>>>>,
    completed_nodes: Arc<RwLock<Vec<NodeID>>>,
    loop_nodes: Arc<RwLock<Vec<String>>>,
    // Atomic variable used to count loop iterations
    node_results: Arc<RwLock<Buffer<CmTypes>>>,
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
        graph: &Graph,
        slots: usize,
        max_streams: usize,
        max_runtime: Option<u64>,
        use_rdtsc: bool,
    ) -> Clerk {
        let nodes_map = graph.nodes_map();
        let post_nodes_map = graph.post_nodes_map();
        let init_objects_opt = graph.init_objects();
        let id_function_opt = graph.id_function();
        let connect_list = graph.connect_list();

        let total_nodes_per_stream = graph.total_nodes_with_conditions();
        print_debug(&format!(
            "Total nodes per stream: {} ",
            total_nodes_per_stream,
        ));

        // Initialize stream completion counters
        let stream_completion_counts = Arc::new(RwLock::new(Vec::new()));
        let mut completion_counts = stream_completion_counts.write().unwrap();
        completion_counts.clear();
        for _ in 0..slots {
            completion_counts.push(AtomicUsize::new(0));
        }
        drop(completion_counts);

        // Initialize available stream slots (all initially busy with initial streams)
        let available_stream_slots = Arc::new(RwLock::new(Vec::new()));
        let mut available_write = available_stream_slots.write().unwrap();
        for _ in 0..slots {
            available_write.push((true, std::usize::MAX)); // (available, real stream id)
        }
        drop(available_write);

        // Set the fields of the struct's graphs copy - one for each stream
        // Create an additional graph as a static copy
        let graphs = Arc::new(RwLock::new(Vec::new()));
        let mut graphs_write = graphs.write().unwrap();
        graphs_write.clear();
        for _ in 0..slots + 1 {
            let mut graph = Graph::new();
            graph.set_nodes(nodes_map.clone());
            graph.set_connect_list(connect_list.clone());
            if let Some(inits) = init_objects_opt.as_ref() {
                graph.set_init_objects(&inits);
            }
            if let Some(id_function) = id_function_opt.as_ref() {
                graph.set_id_function(id_function);
            }
            graph.set_post_nodes(post_nodes_map.cloned());
            graphs_write.push(graph);
        }
        drop(graphs_write);

        Clerk {
            graphs,
            pending_nodes: Arc::new(RwLock::new(Vec::new())),
            removed_cond_nodes: Arc::new(RwLock::new(HashMap::new())),
            completed_nodes: Arc::new(RwLock::new(Vec::new())),
            loop_nodes: Arc::new(RwLock::new(Vec::new())),
            node_results: Arc::new(RwLock::new(Buffer::new())),
            stream_complete_counter: Arc::new(AtomicUsize::new(0)),
            stream_completion_counts,
            available_stream_slots,
            stream_to_slot_mapping: Arc::new(RwLock::new(HashMap::new())),
            total_nodes_per_stream,
            slots,
            workers: 1, // Default to 1 worker -- Determined by scheduler
            max_streams,
            max_runtime,
            time_buffer: Arc::new(RwLock::new(TimeBuffer::new(slots, use_rdtsc))),
        }
    }

    pub fn get_results(&self, stream: usize) -> HashMap<String, Vec<CmTypes>> {
        let node_results_lock = self.node_results.read().unwrap();
        node_results_lock.get_buffer(stream).clone()
    }

    pub fn print_statistics(&self, bench_name: &str, out_file: Option<&str>) {
        let time_read = self.time_buffer.read().unwrap();
        time_read.print_stats(bench_name, out_file);
    }

    pub fn run(&mut self, scheduler: SchedulerImpl) {
        // Overwrite workers
        self.workers = scheduler.workers();

        // create ready channel
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<NodeID>();
        let ready_tx_clone = ready_tx.clone();

        // create completed channel
        let (completed_tx, completed_rx) = std::sync::mpsc::channel::<(NodeID, CmTypes)>();

        let graphs_read = self.graphs.read().unwrap();
        let connect_list = graphs_read[self.slots].connect_list().clone();
        drop(graphs_read);

        // Add graph nodes to pending_nodes
        for connect_nodes in connect_list.iter() {
            let slot_count = std::cmp::min(self.slots, self.max_streams);
            for stream in 0..slot_count {
                self.add_nodes_for_slot(connect_nodes, stream);
            }
        }
        // Initialize node_results
        self.init_results(self.slots);

        // clone a pair of clerks, one for each thread
        let mut clerk_for_ready = self.clone();
        let mut clerk_for_completed = self.clone();
        let mut clerk_for_schedule = self.clone();

        // Spawn thread to handle set_ready_nodes
        let ready_handle = spawn(move || {
            clerk_for_ready.set_ready_nodes(&ready_tx_clone);
        });

        // Spawn thread to handle schedule_nodes
        let schedule_handle = spawn(move || {
            clerk_for_schedule.schedule_nodes(scheduler, completed_tx, &ready_rx);
        });

        // Spawn a thread to handle completed nodes
        let complete_handle = spawn(move || clerk_for_completed.process_completed(completed_rx));

        // Start slot processing
        let mut time_write = self.time_buffer.write().unwrap();
        for slot in 0..self.slots + 1 {
            time_write.start_slot_processing(slot);
        }
        drop(time_write);

        let start_time = Instant::now();
        // Check for max_runtime
        if let Some(max_runtime) = self.max_runtime {
            loop {
                if start_time.elapsed().as_secs() > max_runtime {
                    // set exit signal
                    println!("Max runtime reached, exiting...");
                    // Process post-nodes if any
                    println!("Processing possible post-nodes...");
                    // blocking
                    self.schedule_post_nodes(&ready_tx);
                    // Close ready channel
                    ready_tx
                        .send(NodeID::new("exit".to_string(), 0, 0))
                        .unwrap();
                    // Add exit signal to pending nodes
                    let mut pending_lock = self.pending_nodes.write().unwrap();
                    pending_lock.push(NodeID::new("exit".to_string(), 0, 0));
                    drop(pending_lock);
                    break;
                }
                sleep(Duration::from_millis(20));
            }
        }

        // Wait for threads to finish
        ready_handle.join().unwrap();
        schedule_handle.join().unwrap();
        complete_handle.join().unwrap();

        let mut time_write = self.time_buffer.write().unwrap();
        time_write.finish_slot_processing(self.slots);
        drop(time_write);
    }

    fn add_nodes(&mut self, nodes: &Vec<String>) {
        let graphs_read = self.graphs.read().unwrap();
        // Use the static graph to get nodes_map
        let nodes_map = graphs_read[self.slots].nodes_map();
        let stream_count = std::cmp::min(self.slots, self.max_streams);
        let mut pending_lock = self.pending_nodes.write().unwrap();

        for node_name in nodes.iter() {
            let node = nodes_map.get(node_name).unwrap();
            let factor = node.factor;

            // Add nodes for each stream and each factor
            for stream in 0..stream_count {
                for i in 0..factor {
                    let node_id = NodeID::new(node_name.clone(), stream, i);
                    pending_lock.push(node_id);
                }
            }
            print_debug(&format!(
                "Added {} nodes for {} across {} slots",
                factor * stream_count,
                node_name,
                stream_count
            ));
        }
    }

    fn add_nodes_for_slot(&mut self, nodes: &Vec<String>, slot: usize) {
        let graphs_read = self.graphs.read().unwrap();
        // Use the static graph to get nodes_map
        let nodes_map = graphs_read[self.slots].nodes_map();
        let mut pending_lock = self.pending_nodes.write().unwrap();

        for node_name in nodes.iter() {
            let node = nodes_map.get(node_name).unwrap();
            let factor = node.factor;

            for i in 0..factor {
                let node_id = NodeID::new(node_name.clone(), slot, i);
                pending_lock.push(node_id);
            }

            print_debug(&format!(
                "Added {} nodes for {} at slot: {}",
                factor, node_name, slot
            ));
        }
    }

    fn schedule_post_nodes(&mut self, ready_tx: &Sender<NodeID>) {
        let graphs_read = self.graphs.read().unwrap();
        // Use the static graph to get post_nodes_map
        let nodes_map = graphs_read[self.slots].post_nodes_map();
        if let Some(post_nodes) = nodes_map {
            let stream_use = self.slots; // initialized +1 in init_results
            for node in post_nodes.values() {
                for i in 0..node.factor {
                    let mut node = NodeID::new(node.name.clone(), stream_use, i);
                    node.set_post_node(true);
                    ready_tx.send(node).unwrap();
                }
                print_debug(&format!("Added post node: {}", node.name));
                // Wait until all are completed
                let completed_read = self.completed_nodes.read().unwrap();
                let mut completed_count = completed_read
                    .iter()
                    .filter(|node_id| node_id.name == node.name)
                    .count();
                drop(completed_read);
                while completed_count < node.factor {
                    sleep(Duration::from_millis(10));
                    let completed_read = self.completed_nodes.read().unwrap();
                    completed_count = completed_read
                        .iter()
                        .filter(|node_id| node_id.name == node.name)
                        .count();
                    drop(completed_read);
                }
            }
        }
    }

    fn init_results(&mut self, slots: usize) {
        // Initialize node_results with factor entries
        let graphs_read = self.graphs.read().unwrap();
        // Use the static graph to get nodes_map
        let nodes_map = graphs_read[self.slots].nodes_map();
        let mut node_results_lock = self.node_results.write().unwrap();
        node_results_lock.clear_buffer();
        node_results_lock.init_buffer(nodes_map, CmTypes::None(), slots);

        // Initialize post_nodes if any
        let post_nodes_opt = graphs_read[self.slots].post_nodes_map();
        if let Some(post_nodes) = post_nodes_opt {
            node_results_lock.add_buffer(post_nodes, CmTypes::None());
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
                return slot_id;
            }
        }

        // If no slots available, panic
        panic!("No available stream slots for stream: {}", stream);
    }

    fn release_slot(&mut self, slot: usize) {
        let mut available_slots = self.available_stream_slots.write().unwrap();

        let old_stream = available_slots[slot].1;
        available_slots[slot] = (true, std::usize::MAX); // Mark as available
        print_debug(&format!(
            "Released slot {} (had stream: {})",
            slot, old_stream
        ));
    }

    fn check_slot_completion(&mut self, slot: usize) -> bool {
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

            // Release the slot
            self.release_slot(slot);

            // Complete timing
            let mut time_write = self.time_buffer.write().unwrap();
            time_write.finish_slot_processing(slot);
            drop(time_write);

            // Increment global completion counter
            let new_counter =
                self.stream_complete_counter.fetch_add(1, Ordering::SeqCst) + self.slots;

            // Check if we should start a new iteration
            if new_counter < self.max_streams {
                print_debug(&format!("Starting new iteration {}", new_counter));

                // Reset the completion count for this stream
                let completion_counts = self.stream_completion_counts.read().unwrap();
                completion_counts[slot].store(0, Ordering::SeqCst);
                drop(completion_counts);

                // Clear completed nodes for this stream to allow restart
                let mut completed_lock = self.completed_nodes.write().unwrap();
                completed_lock.retain(|node_id| node_id.slot != slot);
                drop(completed_lock);

                // Re-add nodes for new iteration using the completed slot
                let graphs_read = self.graphs.read().unwrap();
                // Use the static graph to get connect_list
                let connect_list = graphs_read[self.slots].connect_list().clone();
                let nodes_map = graphs_read[self.slots].nodes_map().clone();
                drop(graphs_read);

                for connect_nodes in connect_list.iter() {
                    self.add_nodes_for_slot(connect_nodes, slot);
                }

                // Re-set nodes for slot graph
                let mut graphs_write = self.graphs.write().unwrap();
                graphs_write[slot].set_nodes(nodes_map);
                drop(graphs_write);

                // Restart slot processing
                let mut time_write = self.time_buffer.write().unwrap();
                time_write.start_slot_processing(slot);
                drop(time_write);

                return true;
            }
        }
        false
    }

    fn process_loop(&mut self, node_name: String) {
        let graphs_read = self.graphs.read().unwrap();
        // Use the static graph to get connections
        let connections_opt = graphs_read[self.slots].node_connections(&node_name);
        drop(graphs_read);

        if let Some(connections) = connections_opt {
            self.add_nodes(&connections); // TODO: fix for streams

            // Add connections to loop_nodes
            let mut loop_lock = self.loop_nodes.write().unwrap();
            for node_name in connections.iter() {
                loop_lock.push(node_name.clone());
            }
            drop(loop_lock);

            // Remove added nodes from completed
            let mut completed_lock = self.completed_nodes.write().unwrap();
            let mut remove_nodes_idx = Vec::new();
            for (i, node_id) in completed_lock.iter().enumerate() {
                if connections.contains(&node_id.name) {
                    remove_nodes_idx.push(i);
                }
            }
            // Remove nodes from completed
            let mut removed = 0;
            for i in remove_nodes_idx.iter() {
                completed_lock.remove(*i - removed);
                removed += 1;
            }
        } else {
            panic!("Node {} not found in graph", node_name);
        }
    }

    fn check_ready_node(
        &self,
        node: &Node,
        node_idx: usize,
        slot: usize,
        init_objects: Option<&HashMap<String, Vec<CmTypes>>>,
        nodes_map: &HashMap<String, Node>,
    ) -> (bool, bool, bool) {
        // Check if the node is ready to be executed return (bool, bool)
        // where the first bool indicates if the node is ready, the second
        // bool indicates if the node has conditions and the third bool
        // indicates if all of them are met

        let mut has_conditions = false;
        let mut conditions_met = true;
        let mut preds_ready = true;

        for arg in node.args.iter() {
            // Check Predecessor node
            if let Some(predecessor) = arg.predecessor.as_ref() {
                let compl_read = self.completed_nodes.read().unwrap();
                let mut not_ready = false;
                for index in predecessor.indexes.iter() {
                    // get factor of predecessor node
                    let pred_node: &Node = nodes_map.get(&predecessor.name).unwrap();
                    let pred_factor = pred_node.factor;

                    let adjusted_index = Self::find_index(node_idx, *index, pred_factor);
                    let check_id = NodeID::new(predecessor.name.clone(), slot, adjusted_index);
                    if !compl_read.contains(&check_id) {
                        // predecessor not completed
                        preds_ready = false;
                        not_ready = true;
                        break;
                    }
                }
                if not_ready {
                    break;
                }
            }

            // Check if node has a condition
            let init_condition: Option<&InitCondition> = arg.init_condition.as_ref();
            if init_condition.is_none() {
                continue;
            }
            let init_condition: &InitCondition = init_condition.unwrap();
            has_conditions = true;
            // Check if init_condition is met
            match &arg.type_ {
                CmTypes::Ref(obj_name) => {
                    let objects: &HashMap<String, Vec<CmTypes>> = init_objects.as_ref().unwrap();
                    let obj = objects[obj_name][node_idx].clone();
                    let eval = init_condition.evaluate(obj);
                    if !eval {
                        conditions_met = false;
                        break;
                    }
                }
                CmTypes::Res(node_name) => {
                    let res_read = self.node_results.read().unwrap();
                    let result = res_read
                        .search_node_idx(&node_name, node_idx, slot)
                        .unwrap();
                    let eval = init_condition.evaluate(result);
                    if !eval {
                        conditions_met = false;
                        break;
                    }
                }
                CmTypes::Barrier(node_name) => {
                    let res_read = self.node_results.read().unwrap();
                    let result = res_read
                        .search_node_idx(&node_name, node_idx, slot)
                        .unwrap();
                    let eval = init_condition.evaluate(result);
                    if !eval {
                        conditions_met = false;
                        break;
                    }
                }
                _ => {}
            }
        }
        return (preds_ready, has_conditions, conditions_met);
    }

    fn set_ready_nodes(&mut self, ready_tx: &Sender<NodeID>) {
        // Checks if the node is ready to be scheduled and
        // adds it to the ready_nodes list

        loop {
            let time_read = self.time_buffer.read().unwrap();
            let start_time = time_read.measure_time();
            drop(time_read);

            let mut pending_lock = self.pending_nodes.write().unwrap();
            let mut remove_nodes_idx = Vec::new();
            for (i, node_id) in pending_lock.iter().enumerate() {
                // Unwrap node_id
                let node_name = node_id.name.clone();
                let index = node_id.index;
                let slot = node_id.slot;

                // Check for exit condition
                if node_name == "exit" {
                    println!("Exit signal received, stopping set_ready_nodes thread.");
                    return;
                }

                let graphs_read = self.graphs.read().unwrap();
                // Use the appropriate graph for this slot
                let nodes_map = graphs_read[slot].nodes_map();
                let node = nodes_map.get(&node_name).unwrap();

                let init_objects = graphs_read[slot].init_objects();

                let (preds_ready, has_conditions, conditions_met) =
                    self.check_ready_node(node, index, slot, init_objects, nodes_map);

                if preds_ready {
                    // Predecessors are ready
                    if !has_conditions || (has_conditions && conditions_met) {
                        let node_id = NodeID::new(node_name.clone(), slot, index);
                        // Node is ready to be scheduled
                        print_debug(&format!("{:?} is ready to be scheduled", node_id));
                        ready_tx.send(node_id).unwrap();
                        // mark for removal from pending
                        remove_nodes_idx.push(i);
                    } else if has_conditions && !conditions_met {
                        // mark for removal from pending since conditions
                        // evaluated to false
                        remove_nodes_idx.push(i);
                        // increase removed conditional count hashmap for node
                        let mut removed_cond_lock = self.removed_cond_nodes.write().unwrap();
                        removed_cond_lock
                            .entry((node_name.clone(), slot))
                            .and_modify(|v| v.push(index))
                            .or_insert(vec![index]);
                        drop(removed_cond_lock);
                    }
                }
            }
            // Remove ready nodes from pending
            let mut removed = 0;
            for i in remove_nodes_idx.iter() {
                pending_lock.remove(*i - removed);
                removed += 1;
            }
            drop(pending_lock);

            let mut time_write = self.time_buffer.write().unwrap();
            let end_time = time_write.measure_time();
            let duration = time_write.measure_duration(start_time, end_time);
            time_write.add_task_time(self.slots, "Ready-Check Thread", duration);
            drop(time_write);

            // Sleep for a while to avoid busy waiting
            sleep(Duration::from_millis(10));
        }
    }

    fn change_factor(&mut self, node_name: String, count: usize, node_slot: usize) {
        print_debug(&format!(
            "Changing factor for node {} at slot {} by {}",
            node_name, node_slot, count
        ));

        // change node's factor in the graph of the respective node_slot
        let mut graphs_write = self.graphs.write().unwrap();
        let nodes_map = graphs_write[node_slot].nodes_map();
        let old_factor = nodes_map.get(&node_name).unwrap().factor;
        let factor = old_factor - count;
        graphs_write[node_slot].change_node_factor(&node_name, factor);
        drop(graphs_write);
    }

    fn process_completed(&mut self, completed_rx: Receiver<(NodeID, CmTypes)>) {
        let loop_counter = AtomicUsize::new(0);
        let mut id_slot_counter = vec![0; self.slots];

        let nodes_names = {
            let graphs_read = self.graphs.read().unwrap();
            // Use the static graph to get nodes_map
            let nodes_map = graphs_read[self.slots].nodes_map();
            nodes_map.keys().cloned().collect::<Vec<String>>()
        };

        // Create a hasmap to store how many nodes of each type per slot are completed
        let mut completed_count_map: HashMap<String, Vec<usize>> = HashMap::new();
        for name in nodes_names {
            completed_count_map.insert(name, vec![0; self.slots]);
        }

        // Process completed nodes
        while let Ok((node_id, result)) = completed_rx.recv() {
            let time_read = self.time_buffer.read().unwrap();
            let start_time = time_read.measure_time();
            drop(time_read);
            // Unwrap node_id
            let node_name = node_id.name.clone();
            let mut node_index = node_id.index;
            let mut slot = node_id.slot;
            let post_node = node_id.post_node;

            print_debug(&format!("Processing Completed {:?}", node_id));

            if post_node {
                let mut completed_lock = self.completed_nodes.write().unwrap();
                completed_lock.push(NodeID::new(node_name.clone(), slot, node_index));
                drop(completed_lock);
                sleep(Duration::from_millis(15));
                continue;
            }

            // Get Id function and validate slot
            let (id_function_opt, init_objects_opt) = {
                let graphs_read = self.graphs.read().unwrap();
                (
                    graphs_read[slot].id_function().cloned(),
                    graphs_read[slot].init_objects().cloned(),
                )
            };

            // Check if id_function is set
            if let Some(id_function) = id_function_opt {
                let msg = "ID function is not set".to_string();
                let func_ptr = id_function.func_ptr.expect(&msg);
                let predecessor = &id_function.predecessor;

                // Check if completed node is the predecessor
                if predecessor == &node_name {
                    let arg_vec = Self::parse_args(
                        &id_function.args,
                        node_index,
                        slot,
                        init_objects_opt.as_ref(),
                        &self.node_results,
                        &self.graphs,
                        self.workers,
                        Some(result.clone()), // custom_res
                    );

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

            let current_completed = completed_count_map.get(&node_name).unwrap()[slot];
            print_debug(&format!(
                "Current completed count for node {} at slot {}: {}",
                node_name, slot, current_completed
            ));
            node_index = current_completed;
            completed_count_map.get_mut(&node_name).unwrap()[slot] += 1;

            // Add node to completed with correct slot
            let mut completed_lock = self.completed_nodes.write().unwrap();
            completed_lock.push(NodeID::new(node_name.clone(), slot, node_index));
            drop(completed_lock);

            // store result
            let mut res_lock = self.node_results.write().unwrap();
            res_lock.add_element_index(&node_name, node_index, result, slot);
            drop(res_lock);

            print_debug(&format!(
                "Completed Node {} with index: {} at slot {}",
                node_id.name, node_index, slot
            ));

            // check possible shift indexes
            let nodes_to_shift: Vec<(String, usize, usize)> = {
                let rem_nodes_map = self.removed_cond_nodes.read().unwrap();
                rem_nodes_map
                    .iter()
                    .filter_map(|((name, node_slot), indexes)| {
                        // check if pending for this specific slot
                        let pending = self.pending_nodes.read().unwrap();
                        let is_pending = pending
                            .iter()
                            .any(|node_id| &node_id.name == name && node_id.slot == *node_slot);
                        drop(pending);

                        if !is_pending {
                            Some((name.clone(), *node_slot, indexes.len()))
                        } else {
                            None
                        }
                    })
                    .collect()
            };

            let mut remove_nodes = Vec::new();
            for (name, node_slot, count) in nodes_to_shift {
                print_debug(&format!(
                    "Node {} at slot {} is not pending, shifting indexes",
                    name, node_slot
                ));
                self.change_factor(name.clone(), count, node_slot);
                remove_nodes.push((name.clone(), node_slot));
            }
            // Remove processed nodes from removed_cond_nodes
            let mut removed_cond_lock = self.removed_cond_nodes.write().unwrap();
            for (name, node_slot) in remove_nodes.iter() {
                removed_cond_lock.remove(&(name.clone(), *node_slot));
            }
            drop(removed_cond_lock);

            // check for loop in the node
            let loop_opt = {
                let graphs_read = self.graphs.read().unwrap();
                // Use the appropriate graph for this slot
                let node = graphs_read[slot].node(&node_name);
                node.loop_.clone()
            };

            if let Some(loop_) = loop_opt {
                let loop_name = loop_.name.clone();
                let loop_factor = loop_.factor;
                // check if any current nodes are pending
                let pending = self.pending_nodes.read().unwrap();
                let is_pending = pending.iter().any(|node_id| &node_id.name == &node_name);
                drop(pending);

                let schedule_loop = {
                    let loop_counter = loop_counter.fetch_add(1, Ordering::Relaxed);
                    if loop_counter <= loop_factor {
                        true
                    } else {
                        false
                    }
                };

                // add loop to pending
                if !is_pending && schedule_loop {
                    print_debug(&format!(
                        "Initiating loop from node {} to node {}",
                        node_name, loop_name
                    ));
                    self.process_loop(loop_name.clone());
                    // increment loop count
                }
            }

            // Check if slot iteration is complete and handle restart
            let completed = self.check_slot_completion(slot);
            if completed {
                id_slot_counter[slot] = 0; // Reset index for this slot
            }

            let mut time_write = self.time_buffer.write().unwrap();
            let end_time = time_write.measure_time();
            let duration = time_write.measure_duration(start_time, end_time);
            time_write.add_task_time(self.slots, "Completion Thread", duration);
        }
    }

    fn schedule_nodes(
        &mut self,
        scheduler: SchedulerImpl,
        completed_tx: Sender<(NodeID, CmTypes)>,
        ready_rx: &Receiver<NodeID>,
    ) {
        // Get node and node_index from the channel
        for node_id in ready_rx.iter() {
            let time_read = self.time_buffer.read().unwrap();
            let start_time = time_read.measure_time();
            drop(time_read);

            // Unwrap node_id
            let node_name = node_id.name.clone();
            let node_index = node_id.index;
            let slot = node_id.slot;
            let post_node = node_id.post_node;

            // Check for exit condition
            if node_name == "exit" {
                println!("Exit signal received, stopping schedule_nodes thread.");
                drop(completed_tx);
                return;
            }

            let graphs_read = self.graphs.read().unwrap();

            let nodes_map = {
                if post_node {
                    // Use the static graph for post nodes
                    graphs_read[self.slots].post_nodes_map().unwrap()
                } else {
                    // Use the appropriate graph for this slot
                    graphs_read[slot].nodes_map()
                }
            };

            let node = nodes_map.get(&node_name.clone()).unwrap();
            let init_objects = if post_node {
                graphs_read[self.slots].init_objects()
            } else {
                graphs_read[slot].init_objects()
            };

            let arg_vec = self.create_node_args(node, node_index, slot, init_objects);
            let error = format!(
                "Node {} with index {} has no function pointer",
                node_name, node_index
            );
            let func = node.func_ptr.expect(error.as_str());

            // Schedule Task
            let completed_tx_clone = completed_tx.clone();
            let node_id_clone = node_id.clone();
            let time_buf = self.time_buffer.clone();

            let task = move || {
                let time_read = time_buf.read().unwrap();
                let start_time = time_read.measure_time();
                drop(time_read);

                let result = func(arg_vec);

                if !node_id.post_node {
                    let mut time_write = time_buf.write().unwrap();
                    let end_time = time_write.measure_time();
                    let duration = time_write.measure_duration(start_time, end_time);
                    time_write.add_task_time(node_id_clone.slot, &node_id_clone.name, duration);
                    drop(time_write);
                }
                // Send result through channel
                completed_tx_clone.send((node_id_clone, result)).unwrap();
            };
            print_debug(&format!("Scheduling {:?}", node_id));
            scheduler.spawn_task(task);

            let mut time_write = self.time_buffer.write().unwrap();
            let end_time = time_write.measure_time();
            let duration = time_write.measure_duration(start_time, end_time);
            time_write.add_task_time(self.slots, "Scheduler Thread", duration);
        }
    }

    fn create_node_args(
        &self,
        node: &Node,
        node_index: usize,
        slot: usize,
        init_objects_opt: Option<&HashMap<String, Vec<CmTypes>>>,
    ) -> Vec<CmTypes> {
        let args = {
            // check if node is in loop_nodes
            let loop_read = self.loop_nodes.read().unwrap();
            let mut looping = false;
            if loop_read.contains(&node.name.clone()) {
                // node is in loop_nodes
                looping = true;
            }

            let loop_opt = node.loop_args.as_ref();

            if looping && loop_opt.is_some() {
                loop_opt.unwrap()
            } else {
                &node.args
            }
        };

        let arg_vec = Self::parse_args(
            args,
            node_index,
            slot,
            init_objects_opt,
            &self.node_results,
            &self.graphs,
            self.workers,
            None, // custom_res
        );

        arg_vec
    }

    fn find_index(node_idx: usize, dep_idx: usize, pred_factor: usize) -> usize {
        // Find the index of the node in the results
        if pred_factor == 0 {
            panic!("Predecessor factor is 0 - check your graph configuration");
        }
        let req_idx = node_idx + dep_idx;
        req_idx % pred_factor
    }

    fn parse_args(
        args: &Vec<Arg>,
        node_index: usize,
        slot: usize,
        init_objects_opt: Option<&HashMap<String, Vec<CmTypes>>>,
        node_results: &Arc<RwLock<Buffer<CmTypes>>>,
        graphs: &Arc<RwLock<Vec<Graph>>>,
        workers: usize,
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
                    let init_objects = init_objects_opt.as_ref().unwrap();

                    // Argument may be node index
                    if obj_name == "$index" {
                        arg_vec.push(CmTypes::Usize(node_index));
                        continue;
                    }

                    // Argument may be worker num
                    if obj_name == "$workers" {
                        arg_vec.push(CmTypes::Usize(workers));
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
                            let graphs_read = graphs.read().unwrap();
                            let nodes_map = graphs_read[slot].nodes_map();
                            let pred_node: &Node = nodes_map
                                .get(&arg.predecessor.as_ref().unwrap().name)
                                .unwrap();
                            let pred_factor = pred_node.factor;

                            // Find the index of the node in the results
                            let new_index = Self::find_index(node_index, x, pred_factor);
                            new_index
                        })
                        .collect::<Vec<usize>>();

                    for dep_idx in indices.iter() {
                        // for each task index, retrieve the
                        // corresponding results
                        // (must exist since they are completed)
                        let res_read = node_results.read().unwrap();

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
}

#[derive(Clone, PartialEq)]
struct NodeID {
    name: String,
    slot: usize,
    index: usize,
    post_node: bool,
}

impl NodeID {
    fn new(name: String, slot: usize, index: usize) -> NodeID {
        NodeID {
            name,
            slot,
            index,
            post_node: false,
        }
    }

    fn set_post_node(&mut self, post_node: bool) {
        self.post_node = post_node;
    }
}

impl std::fmt::Debug for NodeID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "NodeID {{ name: {}, index: {}, slot: {}, post_node: {} }}",
            self.name, self.index, self.slot, self.post_node
        )
    }
}

struct Buffer<T> {
    buffer: Vec<HashMap<String, Vec<T>>>,
}

impl<T: Clone> Buffer<T> {
    fn new() -> Buffer<T> {
        Buffer { buffer: Vec::new() }
    }

    fn init_buffer(&mut self, nodes: &HashMap<String, Node>, init_val: T, slots: usize)
    where
        T: Clone,
    {
        if self.buffer.is_empty() {
            // Initialize buffer with empty HashMaps for each stream
            for _ in 0..slots {
                self.buffer.push(HashMap::new());
            }
        }

        // iterate over the nodes map to create a vector for each node
        for (node_name, node) in nodes.iter() {
            let factor = node.factor;
            let new_vec = vec![init_val.clone(); factor];
            // Initialize HashMap for each stream
            for stream in 0..self.buffer.len() {
                self.buffer[stream].insert(node_name.clone(), new_vec.clone());
            }
        }
    }

    fn add_buffer(&mut self, nodes: &HashMap<String, Node>, init_val: T)
    where
        T: Clone,
    {
        // Add a new buffer to self.buffer
        let mut new_buffer = HashMap::new();
        for (node_name, node) in nodes.iter() {
            let factor = node.factor;
            let new_vec = vec![init_val.clone(); factor];
            new_buffer.insert(node_name.clone(), new_vec);
        }
        self.buffer.push(new_buffer);
    }

    fn clear_buffer(&mut self) {
        for buf in self.buffer.iter_mut() {
            buf.clear();
        }
    }

    fn get_buffer(&self, slot: usize) -> &HashMap<String, Vec<T>> {
        &self.buffer[slot]
    }

    fn search_node_idx(&self, node_name: &str, index: usize, slot: usize) -> Option<T>
    where
        T: Clone,
    {
        if let Some(vec) = self.buffer[slot].get(node_name) {
            if index < vec.len() {
                Some(vec[index].clone())
            } else {
                None
            }
        } else {
            None
        }
    }

    fn add_element_index(&mut self, node_name: &str, index: usize, element: T, slot: usize) {
        if let Some(vec) = self.buffer[slot].get_mut(node_name) {
            if index < vec.len() {
                vec[index] = element;
            } else {
                panic!("Index {} out of bounds for node {}", index, node_name);
            }
        } else {
            panic!("Node {} not found in buffer", node_name);
        }
    }
}
