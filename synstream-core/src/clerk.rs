use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, RwLock};

use crate::graph_struct::*;
use crate::scheduler::{Scheduler, SchedulerImpl};
use synstream_types::*;

#[derive(Clone)]
pub struct Clerk {
    // Keep a graph copy under RwLock lock in case
    // adding nodes is needed
    graph: Arc<RwLock<Graph>>,
    pending_nodes: Arc<RwLock<Vec<(String, usize)>>>,
    completed_nodes: Arc<RwLock<Vec<(String, usize)>>>,
    loop_nodes: Arc<RwLock<Vec<(String, usize)>>>,
    node_results: Arc<RwLock<Buffer<CmTypes>>>,
    debug: bool,
    workers: usize,
}

impl Clerk {
    pub fn new(debug: bool) -> Clerk {
        // node_result will be initialized with factor entries
        // when crawling begins
        let node_results = Arc::new(RwLock::new(Buffer::new()));

        Clerk {
            graph: Arc::new(RwLock::new(Graph::new())),
            pending_nodes: Arc::new(RwLock::new(Vec::new())),
            completed_nodes: Arc::new(RwLock::new(Vec::new())),
            loop_nodes: Arc::new(RwLock::new(Vec::new())),
            node_results,
            debug,
            workers: 1, // Default to 1 worker
        }
    }

    pub fn get_results(&self) -> HashMap<String, Vec<CmTypes>> {
        let node_results_lock = self.node_results.read().unwrap();
        node_results_lock.get_buffer().clone()
    }

    pub fn print_all_results(&self) {
        let results = self.get_results();
        for (node_name, result_vec) in results.iter() {
            println!("Node: {}", node_name);
            for (i, result) in result_vec.iter().enumerate() {
                println!("    Index {}: {:?}", i, result);
            }
        }
    }

    pub fn run(&mut self, graph: &Graph, scheduler: SchedulerImpl, max_runtime: Option<u64>) {
        // Overwrite workers
        self.workers = scheduler.workers();

        let nodes_map = graph.nodes_map();
        let init_objects_opt = graph.init_objects();
        let connect_list = graph.connect_list();

        // Set the fields of the struct's graph copy
        let mut graph = self.graph.write().unwrap();
        graph.set_nodes(nodes_map.clone());
        graph.set_connect_list(connect_list.clone());
        if let Some(inits) = init_objects_opt {
            graph.set_init_objects(inits.clone());
        }
        drop(graph);

        // create ready channel
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<(String, usize)>();

        // Add graph nodes to pending_nodes
        for connect_nodes in connect_list.iter() {
            self.add_nodes(connect_nodes, None);
        }
        // Initialize node_results
        self.init_results();

        // clone a pair of clerks, one for each thread
        let mut clerk_for_ready = self.clone();
        let mut clerk_for_completed = self.clone();
        let mut clerk_for_schedule = self.clone();

        // Create a process_completed buffer
        let completed_queue = Arc::new(RwLock::new(Vec::new()));
        let queue_process = completed_queue.clone();
        let queue_schedule = completed_queue.clone();

        // Spawn thread to handle set_ready_nodes
        let ready_handle = std::thread::spawn(move || {
            clerk_for_ready.set_ready_nodes(&ready_tx);
        });

        // Spawn thread to handle schedule_nodes
        let schedule_handle = std::thread::spawn(move || {
            clerk_for_schedule.schedule_nodes(scheduler, queue_schedule, &ready_rx);
        });

        // Spawn a thread to handle completed nodes
        let complete_handle =
            std::thread::spawn(move || clerk_for_completed.process_completed(queue_process));

        let start_time = std::time::Instant::now();
        // Check for max_runtime
        if let Some(max_runtime) = max_runtime {
            loop {
                if start_time.elapsed().as_secs() > max_runtime {
                    // set exit signal
                    println!("Max runtime reached, exiting...");
                    let mut pending_lock = self.pending_nodes.write().unwrap();
                    pending_lock.push(("exit".to_string(), 0));
                    drop(pending_lock);
                    self.print_debug("pending_lock dropped");
                    let mut completed_lock = completed_queue.write().unwrap();
                    completed_lock.push(("exit".to_string(), 0));
                    drop(completed_lock);
                    self.print_debug("completed_lock dropped");
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }

        // Wait for threads to finish
        ready_handle.join().unwrap();
        schedule_handle.join().unwrap();
        complete_handle.join().unwrap();
    }

    fn add_nodes(&mut self, nodes: &Vec<String>, index: Option<usize>) {
        let graph_read = self.graph.read().unwrap();
        let nodes_map = graph_read.nodes_map();
        let mut pending_lock = self.pending_nodes.write().unwrap();
        for node_name in nodes.iter() {
            let node = nodes_map.get(node_name).unwrap();
            let factor = node.factor;

            if index.is_none() {
                for i in 0..factor {
                    pending_lock.push((node_name.clone(), i));
                }
            } else {
                let i = index.unwrap();
                if i < factor {
                    pending_lock.push((node_name.clone(), i));
                } else {
                    panic!("Index {} out of bounds for node {}", i, node_name);
                }
            }
        }
    }

    fn init_results(&mut self) {
        // Initialize node_results with factor entries
        let graph_read = self.graph.read().unwrap();
        let nodes_map = graph_read.nodes_map();
        let mut node_results_lock = self.node_results.write().unwrap();
        node_results_lock.clear_buffer();
        node_results_lock.init_buffer(nodes_map, CmTypes::None());
    }

    fn process_loop(&mut self, node_name: String, node_index: usize) {
        let graph_read = self.graph.read().unwrap();
        let connections_opt = graph_read.node_connections(&node_name);
        drop(graph_read);

        if let Some(connections) = connections_opt {
            self.add_nodes(&connections, Some(node_index));

            // Add connections to loop_nodes
            let mut loop_lock = self.loop_nodes.write().unwrap();
            for node_name in connections.iter() {
                loop_lock.push((node_name.clone(), node_index));
            }
            drop(loop_lock);

            // Remove added nodes from completed
            let mut completed_lock = self.completed_nodes.write().unwrap();
            let mut remove_nodes_idx = Vec::new();
            for (i, (node_name, index)) in completed_lock.iter().enumerate() {
                if connections.contains(node_name) && *index == node_index {
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
                    if !compl_read.contains(&(predecessor.name.clone(), adjusted_index)) {
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
                    let result = res_read.search_node_idx(&node_name, node_idx).unwrap();
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

    fn set_ready_nodes(&mut self, ready_tx: &Sender<(String, usize)>) {
        // Checks if the node is ready to be scheduled and
        // adds it to the ready_nodes list
        loop {
            let mut pending_lock = self.pending_nodes.write().unwrap();
            let mut remove_nodes_idx = Vec::new();
            for (i, (node_name, index)) in pending_lock.iter().enumerate() {
                // Check for exit condition
                if node_name == "exit" {
                    return;
                }

                let graph_read = self.graph.read().unwrap();
                let nodes_map = graph_read.nodes_map();
                let node = nodes_map.get(node_name).unwrap();

                let init_objects = graph_read.init_objects();

                let (preds_ready, has_conditions, conditions_met) =
                    self.check_ready_node(node, *index, init_objects, nodes_map);

                if preds_ready {
                    // Predecessors are ready
                    if !has_conditions || (has_conditions && conditions_met) {
                        // Node is ready to be scheduled
                        self.print_debug(&format!(
                            "Node {} with index {} is ready to be scheduled",
                            node_name, index
                        ));
                        ready_tx.send((node_name.clone(), *index)).unwrap();
                        // mark for removal from pending
                        remove_nodes_idx.push(i);
                    } else if has_conditions && !conditions_met {
                        // mark for removal from pending since conditions
                        // evaluated to false
                        remove_nodes_idx.push(i);
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
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    fn process_completed(&mut self, completed_queue: Arc<RwLock<Vec<(String, usize)>>>) {
        // Process completed nodes
        loop {
            let mut queue_lock = completed_queue.write().unwrap();
            if queue_lock.is_empty() {
                drop(queue_lock);
                // Sleep for a while to avoid busy waiting
                std::thread::sleep(std::time::Duration::from_millis(15));
                continue;
            }
            let (node_name, node_index) = queue_lock.pop().unwrap();
            drop(queue_lock);
            self.print_debug(&format!(
                "Processing completed node: {} with index {}",
                node_name, node_index
            ));

            // Check for exit condition
            if node_name == "exit" {
                return;
            }

            // Add node to completed
            let mut completed_lock = self.completed_nodes.write().unwrap();
            completed_lock.push((node_name.clone(), node_index));
            drop(completed_lock);

            self.print_debug(&format!(
                "Completed node: {} with index {}",
                node_name, node_index
            ));

            // check for loop in the node
            let graph_read = self.graph.read().unwrap();
            let node = graph_read.node(&node_name);
            let loop_opt = node.loop_.clone();
            drop(graph_read);
            if let Some(loop_name) = loop_opt {
                // add loop to pending
                self.process_loop(loop_name.clone(), node_index);
            }
            std::thread::sleep(std::time::Duration::from_millis(15));
        }
    }

    fn schedule_nodes(
        &mut self,
        scheduler: SchedulerImpl,
        completed_queue: Arc<RwLock<Vec<(String, usize)>>>,
        ready_rx: &Receiver<(String, usize)>,
    ) {
        // Get node and node_index from the channel
        for (node_name, node_index) in ready_rx.iter() {
            let graph_read = self.graph.read().unwrap();

            let nodes_map = graph_read.nodes_map();
            let node = nodes_map.get(&node_name.clone()).unwrap();
            let init_objects = graph_read.init_objects();

            let arg_vec = self.create_node_args(node, node_index, init_objects);
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
                    res_lock.add_element_index(&name, node_index, result);
                    drop(res_lock);
                }
                // add to completed queue
                {
                    let mut comp_lock = completed_queue.write().unwrap();
                    comp_lock.push((name.clone(), node_index));
                    drop(comp_lock);
                }
            };
            self.print_debug(&format!(
                "Scheduling node {} with index {}",
                node_name, node_index
            ));
            scheduler.spawn_task(task);
        }
    }

    fn create_node_args(
        &self,
        node: &Node,
        node_index: usize,
        init_objects_opt: Option<&HashMap<String, Vec<CmTypes>>>,
    ) -> Vec<CmTypes> {
        // Create the arguments vector for given node
        let mut arg_vec: Vec<CmTypes> = Vec::new();

        let args = {
            // check if node is in loop_nodes
            let loop_read = self.loop_nodes.read().unwrap();
            let mut looping = false;
            if loop_read.contains(&(node.name.clone(), node_index)) {
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

        for arg in args.iter() {
            // continue if arg is a condition
            if arg.is_condition() {
                continue;
            }

            match &arg.type_ {
                CmTypes::Ref(obj_name) => {
                    self.print_debug(&format!("Passing arg reference to object: {}", obj_name));
                    let init_objects = init_objects_opt.as_ref().unwrap();

                    // Argument may be node index
                    if obj_name == "$index" {
                        self.print_debug(&format!(
                            "Passing node index: {} for object: {}",
                            node_index, obj_name
                        ));
                        arg_vec.push(CmTypes::Usize(node_index));
                        continue;
                    }

                    // Argument may be worker num
                    if obj_name == "$workers" {
                        self.print_debug(&format!("Passing worker num for object: {}", obj_name));
                        arg_vec.push(CmTypes::Usize(self.workers));
                        continue;
                    }

                    // object may be either buffer indexed by node_index
                    // or just variable indexed by 0
                    let msg = format!("Object {} not found in init_objects", obj_name);
                    let obj_vec = init_objects.get(obj_name).expect(msg.as_str());
                    let obj = {
                        if obj_vec.len() > 1 {
                            // If the object is a buffer, get the object at node_index
                            obj_vec[node_index].clone()
                        } else {
                            // If the object is a variable, get the first element
                            obj_vec[0].clone()
                        }
                    };
                    arg_vec.push(obj);
                }
                CmTypes::Res(res_node) => {
                    self.print_debug(&format!("Passing arg result of node: {}", res_node));
                    let indices = arg
                        .predecessor
                        .as_ref()
                        .unwrap()
                        .indexes
                        .iter()
                        .map(|&x| {
                            // Get the predecessor node factor
                            let graph_read = self.graph.read().unwrap();
                            let nodes_map = graph_read.nodes_map();
                            let pred_node: &Node = nodes_map
                                .get(&arg.predecessor.as_ref().unwrap().name)
                                .unwrap();
                            let pred_factor = pred_node.factor;

                            // Find the index of the node in the results
                            Self::find_index(node_index, x, pred_factor)
                        })
                        .collect::<Vec<usize>>();

                    for dep_idx in indices.iter() {
                        // for each task index, retrieve the
                        // corresponding results
                        // (must exist since they are completed)
                        let res_read = self.node_results.read().unwrap();

                        let result = res_read.search_node_idx(&res_node, *dep_idx).unwrap();
                        arg_vec.push(result);
                    }
                }
                _ => {
                    arg_vec.push(arg.type_.clone());
                }
            }
        }
        arg_vec
    }

    fn find_index(node_idx: usize, dep_idx: usize, pred_factor: usize) -> usize {
        // Find the index of the node in the results
        let req_idx = node_idx + dep_idx;
        req_idx % pred_factor
    }

    fn print_debug(&self, msg: &str) {
        if self.debug {
            println!("{}", msg);
        }
    }
}

struct Buffer<T> {
    buffer: HashMap<String, Vec<T>>,
}

impl<T> Buffer<T> {
    fn new() -> Buffer<T> {
        Buffer {
            buffer: HashMap::new(),
        }
    }

    fn init_buffer(&mut self, nodes: &HashMap<String, Node>, init_val: T)
    where
        T: Clone,
    {
        // iterate over the nodes map to create a vector for each node
        for (node_name, node) in nodes.iter() {
            let factor = node.factor;
            let new_vec = vec![init_val.clone(); factor];
            self.buffer.insert(node_name.clone(), new_vec);
        }
    }

    fn clear_buffer(&mut self) {
        self.buffer.clear();
    }

    fn get_buffer(&self) -> &HashMap<String, Vec<T>> {
        &self.buffer
    }

    fn _search_node(&self, node_name: &str) -> Option<&Vec<T>> {
        self.buffer.get(node_name)
    }

    fn search_node_idx(&self, node_name: &str, index: usize) -> Option<T>
    where
        T: Clone,
    {
        if let Some(vec) = self.buffer.get(node_name) {
            if index < vec.len() {
                Some(vec[index].clone())
            } else {
                None
            }
        } else {
            None
        }
    }

    fn _add_element(&mut self, node_name: &str, element: T) {
        if let Some(vec) = self.buffer.get_mut(node_name) {
            vec.push(element);
        } else {
            panic!("Node {} not found in buffer", node_name);
        }
    }

    fn add_element_index(&mut self, node_name: &str, index: usize, element: T) {
        if let Some(vec) = self.buffer.get_mut(node_name) {
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
