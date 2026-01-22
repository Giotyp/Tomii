use core::panic;
use rapidhash::RapidHashMap;
use std::collections::HashSet;
use std::sync::Arc;

use crate::graph_struct::*;
use crate::{debug::print_debug, IdType};
use synstream_types::*;

/// Graph structure
#[derive(Clone)]
pub struct Graph {
    pub nodes: Vec<Node>,
    pub initial_nodes: Vec<IdType>,
    pub successors: Vec<Vec<IdType>>,
    pub condition_nodes: HashSet<IdType>,
    pub post_nodes: Option<Vec<Node>>,
    pub init_objects: Option<Vec<Vec<CmTypes>>>,
    pub obj_id_map: RapidHashMap<String, usize>,
    pub network_config: Option<Arc<GraphNetworkConfig>>,
}

impl GraphStruct for Graph {
    fn add_node(&mut self, node: Node) {
        // assert that node.id === self.nodes.len()
        assert!(node.id as usize == self.nodes.len());

        let mut has_preds = false;
        // Analyze predecessors
        for arg in &node.args {
            if let Some(pred) = &arg.predecessor {
                // Includes both result predecessors and condition predecessors
                // so that the last one will trigger the node execution

                if !has_preds {
                    has_preds = true;
                }
                // Add predecessor to successors list
                while self.successors.len() <= pred.id as usize {
                    self.successors.push(Vec::new());
                }

                if !self.successors[pred.id as usize].contains(&node.id) {
                    self.successors[pred.id as usize].push(node.id);
                }
            }
        }
        if !has_preds {
            // Check if this node has $network arguments (waits for network injection)
            let has_network_arg = node.args.iter().any(|arg| {
                matches!(arg.type_, CmTypes::Ref(2)) // $network uses Ref(2)
            });

            if !has_network_arg {
                print_debug(|| {
                    format!(
                        "Adding initial node: {} with id {} and factor {}",
                        node.name, node.id, node.factor
                    )
                });
                self.initial_nodes.push(node.id);
            } else {
                print_debug(|| {
                    format!(
                        "Skipping initial node (has $network arg): {} with id {} and factor {}",
                        node.name, node.id, node.factor
                    )
                });
            }
        }
        if Self::has_condition(&node.args) {
            self.condition_nodes.insert(node.id);
        }
        self.nodes.push(node);
    }

    fn add_post_node(&mut self, node: Node) {
        if let Some(post_nodes) = &mut self.post_nodes {
            assert!(node.id as usize == post_nodes.len());
            post_nodes.push(node);
        } else {
            let mut post_nodes = Vec::new();
            assert!(node.id == 0);
            post_nodes.push(node);
            self.post_nodes = Some(post_nodes);
        }
    }

    fn find_successors(&self, node_id: IdType) -> &Vec<IdType> {
        if node_id as usize >= self.successors.len() {
            panic!(
                "Node id {} out of bounds for successors with length {}",
                node_id,
                self.successors.len()
            );
        }
        &self.successors[node_id as usize]
    }

    fn dependency_count_vec(&self) -> Vec<usize> {
        // Return a vector with the dependency count for each node
        let mut dep_count_vec: Vec<usize> = Vec::new();
        for node in &self.nodes {
            let mut dep_count = 0;
            let mut preds_seen: Vec<IdType> = Vec::new();

            // first check barriers
            for arg in &node.args {
                if arg.type_.is_barrier() {
                    if let Some(pred) = &arg.predecessor {
                        if !preds_seen.contains(&pred.id) {
                            preds_seen.push(pred.id);
                            dep_count += pred.indexes.len();
                        }
                    }
                }
            }

            for arg in &node.args {
                if !arg.type_.is_barrier() {
                    if let Some(pred) = &arg.predecessor {
                        if !preds_seen.contains(&pred.id) {
                            preds_seen.push(pred.id);
                            dep_count += pred.indexes.len();
                        }
                    }
                }
            }
            dep_count_vec.push(dep_count);
        }
        dep_count_vec
    }
}

impl Graph {
    pub fn new() -> Graph {
        Graph {
            nodes: Vec::new(),
            initial_nodes: Vec::new(),
            successors: Vec::new(),
            condition_nodes: HashSet::new(),
            post_nodes: None,
            init_objects: None,
            obj_id_map: RapidHashMap::default(),
            network_config: None,
        }
    }

    pub fn set_nodes(&mut self, nodes: Vec<Node>) {
        self.nodes = nodes;
    }

    pub fn set_init_objects(&mut self, init_objects: &Vec<Vec<CmTypes>>) {
        self.init_objects = Some(init_objects.clone());
    }

    pub fn set_post_nodes(&mut self, post_nodes: Option<Vec<Node>>) {
        self.post_nodes = post_nodes;
    }

    pub fn get_condition_indexes(&self) -> Vec<Vec<usize>> {
        let mut condition_indexes: Vec<Vec<usize>> = Vec::new();
        for cond_id in self.condition_nodes.iter() {
            let node = &self.nodes[*cond_id as usize];
            let condition_arg_indexes: Vec<usize> = node
                .args
                .iter()
                .enumerate()
                .filter_map(|(idx, arg)| arg.init_condition.as_ref().map(|_| idx))
                .collect();

            if !condition_arg_indexes.is_empty() {
                condition_indexes.push(condition_arg_indexes);
            }
        }
        condition_indexes
    }

    pub fn has_barrier(&self, node_id: IdType) -> bool {
        let node = &self.nodes[node_id as usize];
        for arg in &node.args {
            if arg.type_.is_barrier() {
                return true;
            }
        }
        false
    }

    pub fn has_condition(args: &Vec<Arg>) -> bool {
        for arg in args {
            if arg.init_condition.is_some() {
                return true;
            }
        }
        false
    }

    pub fn get_pred_indexes(&self, node_id: IdType, pred_id: IdType) -> Vec<isize> {
        let node = &self.nodes[node_id as usize];
        let args = &node.args;
        let mut pred_idxs = Vec::new();
        for arg in args {
            if arg.type_.is_barrier() {
                if let Some(pred) = &arg.predecessor {
                    if pred.id == pred_id {
                        return pred.indexes.clone();
                    }
                }
            }

            if let Some(pred) = &arg.predecessor {
                if pred.id == pred_id {
                    pred_idxs = pred.indexes.clone();
                }
            }
        }
        pred_idxs
    }

    pub fn set_network_config(&mut self, config: &GraphNetworkConfig) {
        self.network_config = Some(Arc::new(config.clone()));
    }

    pub fn network_config(&self) -> Option<Arc<GraphNetworkConfig>> {
        self.network_config.clone()
    }
}

// Display functions
impl Graph {
    pub fn print_init_objects(&self) {
        if let Some(init_objects) = &self.init_objects {
            println!("Initialized Objects:");
            for (id, obj) in init_objects.iter().enumerate() {
                println!("  {}: {:?}", id, obj);
            }
        } else {
            println!("No initialized objects.");
        }
    }

    pub fn print_graph(&self) {
        println!("Graph:");
        for node in &self.nodes {
            println!("  {}: {:?} ({:?})", node.id, node.name, node.factor);
        }
        if let Some(post_nodes) = &self.post_nodes {
            println!("Post Nodes:");
            for node in post_nodes {
                println!("  {}: {:?} ({:?})", node.id, node.name, node.factor);
            }
        } else {
            println!("No post nodes.");
        }
        println!("Initial Nodes: {:?}", self.initial_nodes);
        println!("Successors: {:?}", self.successors);
    }
}
