use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};

use crate::cmtypes::*;
use crate::graph_struct::*;
use crate::scheduler::Scheduler;

pub struct Clerk {
    scheduler: Scheduler,
    // Keep a nodes map under Mutex lock in case
    // adding nodes is needed
    nodes_map: Arc<Mutex<HashMap<String, Node>>>,
    pending_nodes: Arc<Mutex<Vec<(String, usize)>>>,
    completed_nodes: Arc<Mutex<Vec<(String, usize)>>>,
    node_results: Arc<Mutex<Buffer<CmTypes>>>,
}

impl Clerk {
    pub fn new(scheduler: Scheduler) -> Clerk {
        // node_result will be initialized with mult_factor entries
        // when crawling begins
        let node_results = Arc::new(Mutex::new(Buffer::new_empty()));

        Clerk {
            scheduler,
            nodes_map: Arc::new(Mutex::new(HashMap::new())),
            pending_nodes: Arc::new(Mutex::new(Vec::new())),
            completed_nodes: Arc::new(Mutex::new(Vec::new())),
            node_results,
        }
    }

    fn check_ready_node(
        &self,
        node: &Node,
        index: usize,
        init_objects: Option<&HashMap<String, Vec<CmTypes>>>,
    ) -> bool {
        // Check if the node is ready to be executed

        for node in node.args.iter() {
            // Check Predecessor node
            if let Some(predecessor) = node.predecessor.as_ref() {
                let compl_lock = self.completed_nodes.lock().unwrap();
                for index in predecessor.indexes.iter() {
                    if !compl_lock.contains(&(predecessor.name.clone(), *index)) {
                        // predecessor not completed
                        return false;
                    }
                }
                drop(compl_lock);
            }

            // Check if node has a condition
            let init_condition: Option<&InitCondition> = node.init_condition.as_ref();
            if init_condition.is_none() {
                continue;
            }
            let init_condition: &InitCondition = init_condition.unwrap();
            // Check if init_condition is met
            match &node.type_ {
                CmTypes::Ref(obj_name) => {
                    let objects: &HashMap<String, Vec<CmTypes>> = init_objects.as_ref().unwrap();
                    let obj = objects[obj_name][index].clone();
                    let eval = init_condition.evaluate(obj);
                    if !eval {
                        return false;
                    }
                }
                CmTypes::Res(node_name) => {
                    let res_lock = self.node_results.lock().unwrap();
                    let result = res_lock.search_node_idx(&node_name, index).unwrap();
                    let eval = init_condition.evaluate(result);
                    if !eval {
                        return false;
                    }
                }
                _ => {}
            }
        }
        return true;
    }

    fn set_ready_nodes(
        &mut self,
        init_objects: Option<&HashMap<String, Vec<CmTypes>>>,
        ready_channel: &Sender<(String, usize)>,
        pending_channel: &Receiver<(String, usize)>,
    ) {
        // Checks if the node is ready to be scheduled and
        // adds it to the ready_nodes list
        for (node_name, index) in pending_channel.iter() {
            let nodes_lock = self.nodes_map.lock().unwrap();
            let node = nodes_lock.get(&node_name).unwrap();
            if self.check_ready_node(node, index, init_objects) {
                ready_channel.send((node_name.clone(), index)).unwrap();
            }
            drop(nodes_lock);
        }
    }

    fn schedule_nodes(
        &mut self,
        ready_channel: &Receiver<(String, usize)>,
        init_objects_opt: Option<&HashMap<String, Vec<CmTypes>>>,
    ) {
        // Get node and node_index from the channel
        let (node_name, node_index) = ready_channel.recv().unwrap();
        let nodes_lock = self.nodes_map.lock().unwrap();
        let node = nodes_lock.get(&node_name.clone()).unwrap();

        let arg_vec = self.create_node_args(node, node_index, init_objects_opt);
        let func = node.func_ptr.unwrap();
        drop(nodes_lock);

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
        self.scheduler.spawn_task(task);
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
    fn new(graph: &Graph) -> Buffer<T> {
        let buffer = graph
            .nodes_map()
            .keys()
            .map(|name| (name.clone(), Vec::new()))
            .collect();
        Buffer { buffer }
    }

    fn new_empty() -> Buffer<T> {
        Buffer {
            buffer: HashMap::new(),
        }
    }

    fn search_node(&self, node_name: &str) -> Option<&Vec<T>> {
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

    fn add_element(&mut self, node_name: &str, element: T) {
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
