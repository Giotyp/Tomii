use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, RwLock};
use std::thread::{sleep, spawn};
use std::time::{Duration, Instant};

use crate::debug::print_debug;
use crate::graph_struct::*;
use crate::scheduler::{Scheduler, SchedulerImpl};
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
    available_stream_slots: Arc<RwLock<Vec<bool>>>,
    // Map frame_id to actual stream slot
    frame_to_slot_mapping: Arc<RwLock<HashMap<usize, usize>>>,
    total_nodes_per_stream: usize,
    streams: usize,
    workers: usize,
    max_streams: usize,
}

impl Clerk {
    pub fn new() -> Clerk {
        // node_result will be initialized with factor entries
        // when run begins
        let node_results = Arc::new(RwLock::new(Buffer::new()));

        Clerk {
            graphs: Arc::new(RwLock::new(Vec::new())),
            pending_nodes: Arc::new(RwLock::new(Vec::new())),
            removed_cond_nodes: Arc::new(RwLock::new(HashMap::new())),
            completed_nodes: Arc::new(RwLock::new(Vec::new())),
            loop_nodes: Arc::new(RwLock::new(Vec::new())),
            node_results,
            stream_complete_counter: Arc::new(AtomicUsize::new(0)),
            stream_completion_counts: Arc::new(RwLock::new(Vec::new())),
            available_stream_slots: Arc::new(RwLock::new(Vec::new())),
            frame_to_slot_mapping: Arc::new(RwLock::new(HashMap::new())),
            total_nodes_per_stream: 0,
            streams: 1,     // Default to 1 stream
            workers: 1,     // Default to 1 worker
            max_streams: 1, // Default to 1 max stream
        }
    }

    pub fn get_results(&self, stream: usize) -> HashMap<String, Vec<CmTypes>> {
        let node_results_lock = self.node_results.read().unwrap();
        node_results_lock.get_buffer(stream).clone()
    }

    pub fn print_all_results(&self, streams: usize) {
        for stream in 0..streams {
            let results = self.get_results(stream);
            for (node_name, result_vec) in results.iter() {
                println!("Node: {}", node_name);
                for (i, result) in result_vec.iter().enumerate() {
                    println!("    Index {}: {:?}", i, result);
                }
            }
        }
    }

    pub fn run(
        &mut self,
        graph: &Graph,
        scheduler: SchedulerImpl,
        streams: usize,
        max_streams: usize,
        max_runtime: Option<u64>,
    ) {
        // Overwrite streams
        self.streams = streams;
        // Overwrite max_streams
        self.max_streams = max_streams;

        // Overwrite workers
        self.workers = scheduler.workers();

        let nodes_map = graph.nodes_map();
        let post_nodes_map = graph.post_nodes_map();
        let init_objects_opt = graph.init_objects();
        let id_function_opt = graph.id_function();
        let connect_list = graph.connect_list();

        self.total_nodes_per_stream = graph.total_nodes_with_conditions();
        print_debug(&format!(
            "Total nodes per stream: {} ",
            self.total_nodes_per_stream,
        ));

        // Initialize stream completion counters
        let mut completion_counts = self.stream_completion_counts.write().unwrap();
        completion_counts.clear();
        for _ in 0..streams {
            completion_counts.push(AtomicUsize::new(0));
        }
        drop(completion_counts);

        // Initialize available stream slots (all initially busy with initial frames)
        let mut available_slots = self.available_stream_slots.write().unwrap();
        available_slots.clear();
        for _ in 0..streams {
            available_slots.push(false); // false = busy
        }
        drop(available_slots);

        // Initialize frame to slot mapping for initial frames
        let mut frame_mapping = self.frame_to_slot_mapping.write().unwrap();
        frame_mapping.clear();
        for i in 0..streams {
            frame_mapping.insert(i, i); // frame_id 0->slot 0, frame_id 1->slot 1, etc.
        }
        drop(frame_mapping);

        // Set the fields of the struct's graphs copy - one for each stream
        let mut graphs = self.graphs.write().unwrap();
        graphs.clear();
        for _ in 0..streams {
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
            graphs.push(graph);
        }
        drop(graphs);

        // create ready channel
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<NodeID>();
        let ready_tx_clone = ready_tx.clone();

        // Add graph nodes to pending_nodes
        for connect_nodes in connect_list.iter() {
            self.add_nodes(connect_nodes);
        }
        // Initialize node_results
        self.init_results(streams);

        // clone a pair of clerks, one for each thread
        let mut clerk_for_ready = self.clone();
        let mut clerk_for_completed = self.clone();
        let mut clerk_for_schedule = self.clone();

        // Create a process_completed buffer
        let completed_queue = Arc::new(RwLock::new(Vec::new()));
        let queue_process = completed_queue.clone();
        let queue_schedule = completed_queue.clone();

        // Spawn thread to handle set_ready_nodes
        let ready_handle = spawn(move || {
            clerk_for_ready.set_ready_nodes(&ready_tx_clone);
        });

        // Spawn thread to handle schedule_nodes
        let schedule_handle = spawn(move || {
            clerk_for_schedule.schedule_nodes(scheduler, queue_schedule, &ready_rx);
        });

        // Spawn a thread to handle completed nodes
        let complete_handle = spawn(move || clerk_for_completed.process_completed(queue_process));

        let start_time = Instant::now();
        // Check for max_runtime
        if let Some(max_runtime) = max_runtime {
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
                    let mut completed_lock = completed_queue.write().unwrap();
                    completed_lock.push(NodeID::new("exit".to_string(), 0, 0));
                    drop(completed_lock);
                    break;
                }
                sleep(Duration::from_millis(20));
            }
        }

        // Wait for threads to finish
        ready_handle.join().unwrap();
        schedule_handle.join().unwrap();
        complete_handle.join().unwrap();
    }

    fn add_nodes(&mut self, nodes: &Vec<String>) {
        let graphs_read = self.graphs.read().unwrap();
        // Use the first graph to get nodes_map since
        let nodes_map = graphs_read[0].nodes_map();
        let stream_count = self.streams;
        let mut pending_lock = self.pending_nodes.write().unwrap();

        for node_name in nodes.iter() {
            let node = nodes_map.get(node_name).unwrap();
            let factor = node.factor;

            // Add nodes for each stream and each factor
            for frame_id in 0..stream_count {
                for i in 0..factor {
                    let node_id = NodeID::new(node_name.clone(), frame_id, i);
                    pending_lock.push(node_id);
                }
            }
            print_debug(&format!(
                "Added {} nodes for {} across {} streams",
                factor * stream_count,
                node_name,
                stream_count
            ));
        }
    }

    fn schedule_post_nodes(&mut self, ready_tx: &Sender<NodeID>) {
        let graphs_read = self.graphs.read().unwrap();
        // Use the first graph to get post_nodes_map since
        let nodes_map = graphs_read[0].post_nodes_map();
        if let Some(post_nodes) = nodes_map {
            for node in post_nodes.values() {
                for i in 0..node.factor {
                    let mut node = NodeID::new(node.name.clone(), 0, i);
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

    fn init_results(&mut self, streams: usize) {
        // Initialize node_results with factor entries
        let graphs_read = self.graphs.read().unwrap();
        // Use the first graph to get nodes_map since
        let nodes_map = graphs_read[0].nodes_map();
        let mut node_results_lock = self.node_results.write().unwrap();
        node_results_lock.clear_buffer();
        node_results_lock.init_buffer(nodes_map, CmTypes::None(), streams);

        // Initialize post_nodes if any
        let post_nodes_opt = graphs_read[0].post_nodes_map();
        if let Some(post_nodes) = post_nodes_opt {
            node_results_lock.init_buffer(post_nodes, CmTypes::None(), 1);
        }
    }

    fn assign_frame_to_available_slot(&mut self, frame_id: usize) -> usize {
        let mut available_slots = self.available_stream_slots.write().unwrap();
        let mut frame_mapping = self.frame_to_slot_mapping.write().unwrap();

        // Check if this frame is already mapped to a slot
        if let Some(&slot) = frame_mapping.get(&frame_id) {
            return slot;
        }

        // Find first available slot
        for (slot_id, &available) in available_slots.iter().enumerate() {
            if available {
                // Assign this frame to the available slot
                frame_mapping.insert(frame_id, slot_id);
                available_slots[slot_id] = false; // Mark as busy
                print_debug(&format!(
                    "Assigned frame {} to available slot {}",
                    frame_id, slot_id
                ));
                return slot_id;
            }
        }

        // If no slots available, panic
        panic!("No available stream slots for frame {}", frame_id);
    }

    fn release_stream_slot(&mut self, frame_id: usize) {
        let mut available_slots = self.available_stream_slots.write().unwrap();
        let mut frame_mapping = self.frame_to_slot_mapping.write().unwrap();

        if let Some(&slot_id) = frame_mapping.get(&frame_id) {
            available_slots[slot_id] = true; // Mark as available
            frame_mapping.remove(&frame_id);
            print_debug(&format!(
                "Released slot {} (was frame {})",
                slot_id, frame_id
            ));
        }
    }

    fn check_stream_completion(&mut self, stream_id: usize) -> bool {
        // Increment the completion count for this stream
        let completion_counts = self.stream_completion_counts.read().unwrap();
        let current_count = completion_counts[stream_id].fetch_add(1, Ordering::SeqCst) + 1;
        drop(completion_counts);

        // Check if this stream iteration is complete
        if current_count >= self.total_nodes_per_stream {
            print_debug(&format!(
                "Stream {} iteration complete with {} nodes",
                stream_id, current_count
            ));

            // Find which frame_id corresponds to this stream_id and release the slot
            let frame_mapping = self.frame_to_slot_mapping.read().unwrap();
            let mut frame_to_release = None;
            for (&frame_id, &slot_id) in frame_mapping.iter() {
                if slot_id == stream_id {
                    frame_to_release = Some(frame_id);
                    break;
                }
            }
            drop(frame_mapping);

            if let Some(frame_id) = frame_to_release {
                self.release_stream_slot(frame_id);
            }

            // Increment global completion counter
            let new_counter = self.stream_complete_counter.fetch_add(1, Ordering::SeqCst) + 1;

            // Check if we should start a new iteration
            if new_counter < self.max_streams {
                print_debug(&format!("Starting new iteration {}", new_counter));

                // Reset the completion count for this stream
                let completion_counts = self.stream_completion_counts.read().unwrap();
                completion_counts[stream_id].store(0, Ordering::SeqCst);
                drop(completion_counts);

                // Clear completed nodes for this stream to allow restart
                let mut completed_lock = self.completed_nodes.write().unwrap();
                completed_lock.retain(|node_id| node_id.stream != stream_id);
                drop(completed_lock);

                // Re-add nodes for new iteration using the completed stream slot
                let graphs_read = self.graphs.read().unwrap();
                // Use the first graph to get connect_list since
                let connect_list = graphs_read[0].connect_list().clone();
                drop(graphs_read);

                for connect_nodes in connect_list.iter() {
                    self.add_nodes_for_stream(connect_nodes, stream_id);
                }

                return true;
            }
        }
        false
    }

    fn add_nodes_for_stream(&mut self, nodes: &Vec<String>, stream_id: usize) {
        let graphs_read = self.graphs.read().unwrap();
        // Use the first graph to get nodes_map since
        let nodes_map = graphs_read[0].nodes_map();
        let mut pending_lock = self.pending_nodes.write().unwrap();

        for node_name in nodes.iter() {
            let node = nodes_map.get(node_name).unwrap();
            let factor = node.factor;

            for i in 0..factor {
                let node_id = NodeID::new(node_name.clone(), stream_id, i);
                pending_lock.push(node_id);
            }
        }
        print_debug(&format!(
            "Re-added nodes for stream {} iteration",
            stream_id
        ));
    }

    fn process_loop(&mut self, node_name: String) {
        let graphs_read = self.graphs.read().unwrap();
        // Use the first graph to get connections since
        let connections_opt = graphs_read[0].node_connections(&node_name);
        drop(graphs_read);

        if let Some(connections) = connections_opt {
            self.add_nodes(&connections);

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
        stream: usize,
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
                    let check_id = NodeID::new(predecessor.name.clone(), stream, adjusted_index);
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
                        .search_node_idx(&node_name, node_idx, stream)
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
                        .search_node_idx(&node_name, node_idx, stream)
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
            let mut pending_lock = self.pending_nodes.write().unwrap();
            let mut remove_nodes_idx = Vec::new();
            for (i, node_id) in pending_lock.iter().enumerate() {
                // Unwrap node_id
                let node_name = node_id.name.clone();
                let index = node_id.index;
                let stream = node_id.stream;

                // Check for exit condition
                if node_name == "exit" {
                    println!("Exit signal received, stopping set_ready_nodes thread.");
                    return;
                }

                let graphs_read = self.graphs.read().unwrap();
                // Use the appropriate graph for this stream
                let nodes_map = graphs_read[stream].nodes_map();
                let node = nodes_map.get(&node_name).unwrap();

                let init_objects = graphs_read[stream].init_objects();

                let (preds_ready, has_conditions, conditions_met) =
                    self.check_ready_node(node, index, stream, init_objects, nodes_map);

                if preds_ready {
                    // Predecessors are ready
                    if !has_conditions || (has_conditions && conditions_met) {
                        let node_id = NodeID::new(node_name.clone(), stream, index);
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
                            .entry((node_name.clone(), stream))
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

            // Sleep for a while to avoid busy waiting
            sleep(Duration::from_millis(10));
        }
    }

    fn shift_indexes(&mut self, node_name: String, count: usize, node_stream: usize) {
        print_debug(&format!(
            "Shifting indexes for node {} at stream {} by {}",
            node_name, node_stream, count
        ));

        let mut completed_lock = self.completed_nodes.write().unwrap();
        for node_id in completed_lock.iter_mut() {
            let name = &node_id.name;
            let stream = node_id.stream;
            if name != &node_name || stream != node_stream {
                continue;
            }

            if count <= node_id.index {
                let new_index = node_id.index - count;

                print_debug(&format!(
                    "Node {} at stream {} with index {} changed to index {}",
                    name, node_stream, node_id.index, new_index
                ));
                // change result index
                let mut node_results_lock = self.node_results.write().unwrap();
                node_results_lock.change_node_idx(&node_name, node_id.index, new_index, stream);
                drop(node_results_lock);
                // update index
                node_id.index = new_index;
            }
        }
        drop(completed_lock);

        // change node's factor in the graph of the respective node_stream
        let mut graphs_write = self.graphs.write().unwrap();
        let nodes_map = graphs_write[node_stream].nodes_map();
        let old_factor = nodes_map.get(&node_name).unwrap().factor;
        let factor = old_factor - count;
        graphs_write[node_stream].change_node_factor(&node_name, factor);
        drop(graphs_write);
    }

    fn process_completed(&mut self, completed_queue: Arc<RwLock<Vec<NodeID>>>) {
        let loop_counter = AtomicUsize::new(0);
        // Process completed nodes
        loop {
            let mut queue_lock = completed_queue.write().unwrap();
            if queue_lock.is_empty() {
                drop(queue_lock);
                // Sleep for a while to avoid busy waiting
                sleep(Duration::from_millis(15));
                continue;
            }
            let node_id = queue_lock.pop().unwrap();
            // Unwrap node_id
            let node_name = node_id.name.clone();
            let node_index = node_id.index;
            let mut stream = node_id.stream;
            let post_node = node_id.post_node;

            drop(queue_lock);
            print_debug(&format!("Processing Completed {:?}", node_id));

            // Check for exit condition
            if node_name == "exit" {
                println!("Exit signal received, stopping process_completed thread.");
                return;
            }

            // Get Id function and validate stream
            let (id_function_opt, init_objects_opt) = {
                let graphs_read = self.graphs.read().unwrap();
                (
                    graphs_read[stream].id_function().cloned(),
                    graphs_read[stream].init_objects().cloned(),
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
                        stream,
                        init_objects_opt.as_ref(),
                        &self.node_results,
                        &self.graphs,
                        stream,
                        self.workers,
                    );

                    // Call the id function
                    print_debug(&format!("Calling ID function for {:?}", node_id));
                    let id_result = func_ptr(arg_vec);

                    // Extract stream_id from the result
                    if let Some(new_stream_id) = id_result.valid_number_to_usize() {
                        // Validate stream_id range
                        let current_counter = self.stream_complete_counter.load(Ordering::SeqCst);
                        let max_allowed_stream = current_counter + self.streams;

                        if new_stream_id >= max_allowed_stream {
                            panic!(
                                "ID function returned stream_id {} which exceeds maximum allowed {} (current_counter: {}, streams: {})",
                                new_stream_id, max_allowed_stream, current_counter, self.streams
                            );
                        }

                        // Assign frame to an available stream slot
                        stream = self.assign_frame_to_available_slot(new_stream_id);
                        print_debug(&format!(
                            "ID function determined frame_id: {} assigned to slot: {}",
                            new_stream_id, stream
                        ));
                    } else {
                        panic!("ID function did not return a valid number for stream_id");
                    }
                }
            }

            // Add node to completed with correct stream_id
            let mut completed_lock = self.completed_nodes.write().unwrap();
            completed_lock.push(NodeID::new(node_name.clone(), stream, node_index));
            drop(completed_lock);

            if post_node {
                sleep(Duration::from_millis(15));
                continue;
            }

            // check possible shift indexes
            let nodes_to_shift: Vec<(String, usize, usize)> = {
                let rem_nodes_map = self.removed_cond_nodes.read().unwrap();
                rem_nodes_map
                    .iter()
                    .filter_map(|((name, node_stream), indexes)| {
                        // check if pending for this specific stream
                        let pending = self.pending_nodes.read().unwrap();
                        let is_pending = pending
                            .iter()
                            .any(|node_id| &node_id.name == name && node_id.stream == *node_stream);
                        drop(pending);

                        if !is_pending {
                            Some((name.clone(), *node_stream, indexes.len()))
                        } else {
                            None
                        }
                    })
                    .collect()
            };

            let mut remove_nodes = Vec::new();
            for (name, node_stream, count) in nodes_to_shift {
                print_debug(&format!(
                    "Node {} at stream {} is not pending, shifting indexes",
                    name, node_stream
                ));
                self.shift_indexes(name.clone(), count, node_stream);
                remove_nodes.push((name.clone(), node_stream));
            }
            // Remove processed nodes from removed_cond_nodes
            let mut removed_cond_lock = self.removed_cond_nodes.write().unwrap();
            for (name, node_stream) in remove_nodes.iter() {
                removed_cond_lock.remove(&(name.clone(), *node_stream));
            }
            drop(removed_cond_lock);

            // check for loop in the node
            let loop_opt = {
                let graphs_read = self.graphs.read().unwrap();
                // Use the appropriate graph for this stream
                let node = graphs_read[stream].node(&node_name);
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

            // Check if stream iteration is complete and handle restart
            self.check_stream_completion(stream);

            sleep(Duration::from_millis(15));
        }
    }

    fn schedule_nodes(
        &mut self,
        scheduler: SchedulerImpl,
        completed_queue: Arc<RwLock<Vec<NodeID>>>,
        ready_rx: &Receiver<NodeID>,
    ) {
        // Get node and node_index from the channel
        for node_id in ready_rx.iter() {
            // Unwrap node_id
            let node_name = node_id.name.clone();
            let node_index = node_id.index;
            let stream = node_id.stream;
            let post_node = node_id.post_node;

            // Check for exit condition
            if node_name == "exit" {
                println!("Exit signal received, stopping schedule_nodes thread.");
                return;
            }

            let graphs_read = self.graphs.read().unwrap();

            let nodes_map = {
                if post_node {
                    // Use the first graph for post nodes
                    graphs_read[0].post_nodes_map().unwrap()
                } else {
                    // Use the appropriate graph for this stream
                    graphs_read[stream].nodes_map()
                }
            };

            let node = nodes_map.get(&node_name.clone()).unwrap();
            let init_objects = if post_node {
                graphs_read[0].init_objects()
            } else {
                graphs_read[stream].init_objects()
            };

            let arg_vec = self.create_node_args(node, node_index, stream, init_objects);
            let error = format!(
                "Node {} with index {} has no function pointer",
                node_name, node_index
            );
            let func = node.func_ptr.expect(error.as_str());

            // Copy required Arc pointers
            let node_results = self.node_results.clone();
            let completed_queue = completed_queue.clone();
            let name = node_name.clone();

            let task = move || {
                let result = func(arg_vec);
                // store result
                {
                    let mut res_lock = node_results.write().unwrap();
                    res_lock.add_element_index(&name, node_index, result, stream);
                    drop(res_lock);
                }
                // add to completed queue
                {
                    let mut comp_lock = completed_queue.write().unwrap();
                    let mut node_id = NodeID::new(name.clone(), stream, node_index);
                    node_id.set_post_node(post_node);
                    comp_lock.push(node_id);
                    drop(comp_lock);
                }
            };
            print_debug(&format!("Scheduling {:?}", node_id));
            scheduler.spawn_task(task);
        }
    }

    fn create_node_args(
        &self,
        node: &Node,
        node_index: usize,
        stream: usize,
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
            stream,
            init_objects_opt,
            &self.node_results,
            &self.graphs,
            stream,
            self.workers,
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
        stream: usize,
        init_objects_opt: Option<&HashMap<String, Vec<CmTypes>>>,
        node_results: &Arc<RwLock<Buffer<CmTypes>>>,
        graphs: &Arc<RwLock<Vec<Graph>>>,
        stream_id: usize,
        workers: usize,
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
                    let indices = arg
                        .predecessor
                        .as_ref()
                        .unwrap()
                        .indexes
                        .iter()
                        .map(|&x| {
                            // Get the predecessor node factor
                            let graphs_read = graphs.read().unwrap();
                            let nodes_map = graphs_read[stream_id].nodes_map();
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

                        let result = res_read
                            .search_node_idx(&res_node, *dep_idx, stream)
                            .unwrap();
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
    stream: usize,
    index: usize,
    post_node: bool,
}

impl NodeID {
    fn new(name: String, stream: usize, index: usize) -> NodeID {
        NodeID {
            name,
            stream,
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
            "NodeID {{ name: {}, index: {}, stream: {}, post_node: {} }}",
            self.name, self.index, self.stream, self.post_node
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

    fn init_buffer(&mut self, nodes: &HashMap<String, Node>, init_val: T, streams: usize)
    where
        T: Clone,
    {
        if self.buffer.is_empty() {
            // Initialize buffer with empty HashMaps for each stream
            for _ in 0..streams {
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

    fn clear_buffer(&mut self) {
        for buf in self.buffer.iter_mut() {
            buf.clear();
        }
    }

    fn get_buffer(&self, stream: usize) -> &HashMap<String, Vec<T>> {
        &self.buffer[stream]
    }

    fn search_node_idx(&self, node_name: &str, index: usize, stream: usize) -> Option<T>
    where
        T: Clone,
    {
        if let Some(vec) = self.buffer[stream].get(node_name) {
            if index < vec.len() {
                Some(vec[index].clone())
            } else {
                None
            }
        } else {
            None
        }
    }

    fn add_element_index(&mut self, node_name: &str, index: usize, element: T, stream: usize) {
        if let Some(vec) = self.buffer[stream].get_mut(node_name) {
            if index < vec.len() {
                vec[index] = element;
            } else {
                panic!("Index {} out of bounds for node {}", index, node_name);
            }
        } else {
            panic!("Node {} not found in buffer", node_name);
        }
    }

    fn change_node_idx(
        &mut self,
        node_name: &str,
        old_index: usize,
        new_index: usize,
        stream: usize,
    ) {
        // copy data from old index to new index
        if let Some(vec) = self.buffer[stream].get_mut(node_name) {
            if old_index < vec.len() && new_index < vec.len() {
                vec[new_index] = vec[old_index].clone();
            } else {
                panic!("Index out of bounds for node {}", node_name);
            }
        } else {
            panic!("Node {} not found in buffer", node_name);
        }
    }
}
