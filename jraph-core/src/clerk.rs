use std::collections::HashMap;
use std::hash::Hash;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};

use crate::cmtypes::*;
use crate::graph_struct::*;
use crate::scheduler::Scheduler;

#[derive(Clone)]
pub struct Clerk {
    // Keep a graph copy under Mutex lock in case
    // adding nodes is needed
    graph: Arc<Mutex<Graph>>,
    pending_nodes: Arc<Mutex<Vec<(String, usize)>>>,
    completed_nodes: Arc<Mutex<Vec<(String, usize)>>>,
    node_results: Arc<Mutex<Buffer<CmTypes>>>,
}

impl Clerk {
    pub fn new() -> Clerk {
        // node_result will be initialized with mult_factor entries
        // when crawling begins
        let node_results = Arc::new(Mutex::new(Buffer::new()));

        Clerk {
            graph: Arc::new(Mutex::new(Graph::new())),
            pending_nodes: Arc::new(Mutex::new(Vec::new())),
            completed_nodes: Arc::new(Mutex::new(Vec::new())),
            node_results,
        }
    }

    pub fn get_results(&self) -> HashMap<String, Vec<CmTypes>> {
        let node_results_lock = self.node_results.lock().unwrap();
        node_results_lock.get_buffer().clone()
    }

    pub fn run(&mut self, graph: &Graph, scheduler: Scheduler, max_runtime: Option<u64>) {
        let nodes_map = graph.nodes_map();
        let init_objects_opt = graph.init_objects();
        let connect_list = graph.connect_list();

        // Set the fields of the struct's graph copy
        let mut graph_lock = self.graph.lock().unwrap();
        graph_lock.set_nodes(nodes_map.clone());

        graph_lock.set_connect_list(connect_list.clone());

        if let Some(init_objects) = init_objects_opt {
            // Clone the init_objects to avoid borrowing issues
            let init_objects = init_objects.clone();
            graph_lock.set_init_objects(init_objects);
        }
        drop(graph_lock);

        // create ready channel
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<(String, usize)>();

        // Add graph nodes to pending_nodes
        for connect_nodes in connect_list.iter() {
            self.add_nodes(nodes_map, connect_nodes);
        }

        // clone a pair of clerks, one for each thread
        let mut clerk_for_ready = self.clone();
        let mut clerk_for_schedule = self.clone();

        // Spawn thread to handle set_ready_nodes
        let ready_handle = std::thread::spawn(move || {
            clerk_for_ready.set_ready_nodes(&ready_tx);
        });

        // Spawn thread to handle schedule_nodes
        let schedule_handle = std::thread::spawn(move || {
            clerk_for_schedule.schedule_nodes(scheduler, &ready_rx);
        });

        let start_time = std::time::Instant::now();
        // Check for max_runtime
        if let Some(max_runtime) = max_runtime {
            loop {
                if start_time.elapsed().as_secs() > max_runtime {
                    // set exit signal
                    println!("Max runtime reached, exiting...");
                    let mut pending_lock = self.pending_nodes.lock().unwrap();
                    pending_lock.push(("exit".to_string(), 0));
                    drop(pending_lock);
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }

        // Wait for threads to finish
        ready_handle.join().unwrap();
        schedule_handle.join().unwrap();
    }

    fn add_nodes(&mut self, nodes_map: &HashMap<String, Node>, nodes: &Vec<String>) {
        let mut pending_lock = self.pending_nodes.lock().unwrap();
        for node_name in nodes.iter() {
            let node = nodes_map.get(node_name).unwrap();
            let mult_factor = node.mult_factor;
            for i in 0..mult_factor {
                pending_lock.push((node_name.clone(), i));
            }
        }
        drop(pending_lock);

        // Initialize node_results with mult_factor entries
        let mut node_results_lock = self.node_results.lock().unwrap();
        node_results_lock.clear_buffer();
        node_results_lock.init_buffer(nodes_map, CmTypes::None());
        drop(node_results_lock);
    }

    fn check_ready_node(
        &self,
        node: &Node,
        index: usize,
        init_objects: Option<&HashMap<String, Vec<CmTypes>>>,
    ) -> (bool, bool, bool) {
        // Check if the node is ready to be executed return (bool, bool)
        // where the first bool indicates if the node is ready, the second
        // bool indicates if the node has conditions and the third bool
        // indicates if all of them are met

        let mut has_conditions = false;
        let mut conditions_met = true;
        let mut preds_ready = true;

        for node in node.args.iter() {
            // Check Predecessor node
            if let Some(predecessor) = node.predecessor.as_ref() {
                let compl_lock = self.completed_nodes.lock().unwrap();
                let mut not_ready = false;
                for index in predecessor.indexes.iter() {
                    if !compl_lock.contains(&(predecessor.name.clone(), *index)) {
                        // predecessor not completed
                        preds_ready = false;
                        not_ready = true;
                        break;
                    }
                }
                if not_ready {
                    drop(compl_lock);
                    break;
                }
                drop(compl_lock);
            }

            // Check if node has a condition
            let init_condition: Option<&InitCondition> = node.init_condition.as_ref();
            if init_condition.is_none() {
                continue;
            }
            let init_condition: &InitCondition = init_condition.unwrap();
            has_conditions = true;
            // Check if init_condition is met
            match &node.type_ {
                CmTypes::Ref(obj_name) => {
                    let objects: &HashMap<String, Vec<CmTypes>> = init_objects.as_ref().unwrap();
                    let obj = objects[obj_name][index].clone();
                    let eval = init_condition.evaluate(obj);
                    if !eval {
                        conditions_met = false;
                        break;
                    }
                }
                CmTypes::Res(node_name) => {
                    let res_lock = self.node_results.lock().unwrap();
                    let result = res_lock.search_node_idx(&node_name, index).unwrap();
                    let eval = init_condition.evaluate(result);
                    if !eval {
                        conditions_met = false;
                        break;
                    }
                    drop(res_lock);
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
            let mut pending_lock = self.pending_nodes.lock().unwrap();
            let mut remove_nodes_idx = Vec::new();
            for (i, (node_name, index)) in pending_lock.iter().enumerate() {
                // Check for exit condition
                if node_name == "exit" {
                    return;
                }

                let graph_lock = self.graph.lock().unwrap();
                let nodes_map = graph_lock.nodes_map();
                let node = nodes_map.get(node_name).unwrap();

                let init_objects = graph_lock.init_objects();

                let (preds_ready, has_conditions, conditions_met) =
                    self.check_ready_node(node, *index, init_objects);

                if preds_ready {
                    // Predecessors are ready
                    if !has_conditions || (has_conditions && conditions_met) {
                        // Node is ready to be scheduled
                        ready_tx.send((node_name.clone(), *index)).unwrap();
                        // mark for removal from pending
                        remove_nodes_idx.push(i);
                    } else if has_conditions && !conditions_met {
                        // mark for removal from pending since conditions
                        // evaluated to false
                        remove_nodes_idx.push(i);
                    }
                }

                drop(graph_lock);
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

    fn schedule_nodes(&mut self, scheduler: Scheduler, ready_rx: &Receiver<(String, usize)>) {
        // Get node and node_index from the channel
        for (node_name, node_index) in ready_rx.iter() {
            let graph_lock = self.graph.lock().unwrap();

            let nodes_map = graph_lock.nodes_map();
            let node = nodes_map.get(&node_name.clone()).unwrap();
            let init_objects = graph_lock.init_objects();

            let arg_vec = self.create_node_args(node, node_index, init_objects);
            let func = node.func_ptr.unwrap();
            drop(graph_lock);

            // Copy required Arc pointers
            let completed_nodes = self.completed_nodes.clone();
            let node_results = self.node_results.clone();

            let task = move || {
                let result = func(arg_vec);
                // store result
                {
                    let mut res_lock = node_results.lock().unwrap();
                    res_lock.add_element_index(&node_name, node_index, result);
                    drop(res_lock);
                }
                // mark completed
                {
                    let mut comp_lock = completed_nodes.lock().unwrap();
                    comp_lock.push((node_name.clone(), node_index));
                    drop(comp_lock);
                }
            };
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
        let mult_factor = node.mult_factor;

        for arg in node.args.iter() {
            // continue if arg is a condition
            if arg.is_condition() {
                continue;
            }

            match &arg.type_ {
                CmTypes::Ref(obj_name) => {
                    let init_objects = init_objects_opt.as_ref().unwrap();
                    let obj = &init_objects[obj_name][node_index];
                    arg_vec.push(obj.clone());
                }
                CmTypes::Res(res_node) => {
                    let indices = arg
                        .predecessor
                        .as_ref()
                        .unwrap()
                        .indexes
                        .iter()
                        .map(|&x| {
                            // Find the index of the node in the results
                            Self::find_index(node_index, x, mult_factor)
                        })
                        .collect::<Vec<usize>>();

                    for dep_idx in indices.iter() {
                        // for each task index, retrieve the
                        // corresponding results
                        // (must exist since they are completed)
                        let res_lock = self.node_results.lock().unwrap();

                        let result = res_lock.search_node_idx(&res_node, *dep_idx).unwrap();
                        drop(res_lock);
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

    fn find_index(node_idx: usize, dep_idx: usize, mult_factor: usize) -> usize {
        // Find the index of the node in the results
        let req_idx: isize = (node_idx as isize) - (dep_idx as isize);
        if req_idx >= 0 {
            req_idx as usize
        } else {
            mult_factor - req_idx.abs() as usize
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
            let mult_factor = node.mult_factor;
            let new_vec = vec![init_val.clone(); mult_factor];
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
