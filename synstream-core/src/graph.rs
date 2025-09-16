use crate::debug::print_debug;
use crate::graph_struct::*;
use std::collections::HashMap;
use synstream_types::*;

/// Graph structure
#[derive(Clone)]
pub struct Graph {
    pub nodes: HashMap<String, Node>,
    pub initial_nodes: Vec<String>,
    successors: HashMap<String, Vec<String>>,
    // Map of (successor_name, predecessor_name) to predecessor indexes
    pred_idxs: HashMap<(String, String), Vec<isize>>,
    pub id_function: Option<IdFunction>,
    pub post_nodes: Option<HashMap<String, Node>>,
    pub init_objects: Option<HashMap<String, Vec<CmTypes>>>,
}

impl GraphStruct for Graph {
    fn add_node(&mut self, node: Node) {
        self.nodes.insert(node.name.clone(), node.clone());

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
                let successors_list = self
                    .successors
                    .entry(pred.name.clone())
                    .or_insert_with(Vec::new);
                if !successors_list.contains(&node.name) {
                    successors_list.push(node.name.clone());
                }

                // Add predecessor indexes for result predecessors
                if arg.type_.is_result() {
                    let key = (node.name.clone(), pred.name.clone());
                    let indexes = self.pred_idxs.entry(key).or_insert_with(Vec::new);
                    for idx in pred.indexes.iter() {
                        if !indexes.contains(idx) {
                            indexes.push(*idx);
                        }
                    }
                }
            }
        }
        if !has_preds {
            print_debug(&format!(
                "Adding initial node: {} with factor {}",
                node.name, node.factor
            ));
            self.initial_nodes.push(node.name.clone());
        }
    }

    fn add_post_node(&mut self, node: Node) {
        if let Some(post_nodes) = &mut self.post_nodes {
            post_nodes.insert(node.name.clone(), node);
        } else {
            let mut post_nodes = HashMap::new();
            post_nodes.insert(node.name.clone(), node);
            self.post_nodes = Some(post_nodes);
        }
    }

    fn find_successors(
        &self,
        node_name: &str,
        node_index: usize,
    ) -> Vec<(String, Vec<usize>, bool)> {
        let mut next_nodes: Vec<(String, Vec<usize>, bool)> = Vec::new();
        let successor_names = match self.successors.get(node_name) {
            Some(successors) => successors,
            None => &Vec::new(),
        };
        for successor in successor_names {
            // If successor has barrier, all indexes are returns
            if self.has_barrier(successor) {
                let factor = self.nodes.get(successor).expect("Node not found").factor;
                let all_indexes: Vec<usize> = (0..factor).collect();
                next_nodes.push((successor.to_string(), all_indexes, true));
                continue;
            }

            // If successor does not have barrier, find specific indexes
            let indexes = self
                .pred_idxs
                .get(&(successor.to_string(), node_name.to_string()))
                .unwrap();
            // Adjust index
            for dep_idx in indexes {
                let pred_factor = self.nodes.get(node_name).expect("Node not found").factor;
                let mut succ_factor = self.nodes.get(successor).expect("Node not found").factor;

                if pred_factor > succ_factor {
                    succ_factor = pred_factor;
                }

                let succ_indexes =
                    calculate_succ_indexes(pred_factor, succ_factor, node_index, *dep_idx);
                next_nodes.push((successor.to_string(), succ_indexes, false));
            }
        }
        next_nodes
    }

    fn total_executed_nodes(&self) -> usize {
        let mut total = 0;
        let condition_predecessors = self.get_condition_predecessors();

        // First pass: identify all nodes without conditions
        for (_node_name, node) in &self.nodes {
            let mut has_condition = false;
            for arg in &node.args {
                if let Some(_) = &arg.init_condition {
                    has_condition = true;
                    break;
                }
            }
            if !has_condition {
                total += node.factor;
            }
        }

        // Second pass: for each group of nodes with the same condition,
        // add the factor only once (use the predecessor's factor)
        for condition_key in condition_predecessors.keys() {
            // Use predecessor's factor
            if let Some(&pred_factor) = condition_predecessors.get(condition_key) {
                total += pred_factor;
            }
        }
        total
    }
}

impl Graph {
    pub fn new() -> Graph {
        Graph {
            nodes: HashMap::new(),
            initial_nodes: Vec::new(),
            successors: HashMap::new(),
            pred_idxs: HashMap::new(),
            id_function: None,
            post_nodes: None,
            init_objects: None,
        }
    }

    pub fn set_nodes(&mut self, nodes: HashMap<String, Node>) {
        self.nodes = nodes;
    }

    pub fn set_init_objects(&mut self, init_objects: &HashMap<String, Vec<CmTypes>>) {
        self.init_objects = Some(init_objects.clone());
    }

    pub fn set_id_function(&mut self, id_function: &IdFunction) {
        self.id_function = Some(id_function.clone());
    }

    pub fn set_post_nodes(&mut self, post_nodes: Option<HashMap<String, Node>>) {
        self.post_nodes = post_nodes;
    }

    pub fn get_condition_predecessors(&self) -> HashMap<String, usize> {
        let mut condition_predecessors: HashMap<String, usize> = HashMap::new();
        for (_node_name, node) in &self.nodes {
            for arg in &node.args {
                if let Some(_) = &arg.init_condition {
                    // Check what type of condition this is
                    match &arg.type_ {
                        CmTypes::Res(pred_name) => {
                            // For Res conditions, use the predecessor name
                            if let Some(pred_node) = self.nodes.get(pred_name) {
                                // Store the predecessor's factor
                                condition_predecessors.insert(pred_name.clone(), pred_node.factor);
                            }
                        }
                        _ => {}
                    }
                    break;
                }
            }
        }
        condition_predecessors
    }

    pub fn has_barrier(&self, node_name: &str) -> bool {
        if let Some(node) = self.nodes.get(node_name) {
            for arg in &node.args {
                if arg.type_.is_barrier() {
                    return true;
                }
            }
        }
        false
    }

    pub fn get_barriers(&self, node_name: &str) -> Vec<(String, Vec<usize>)> {
        let mut barriers = Vec::new();
        if let Some(node) = self.nodes.get(node_name) {
            for arg in &node.args {
                if arg.type_.is_barrier() {
                    if let Some(pred) = &arg.predecessor {
                        let pred_usize = pred.indexes.iter().map(|&i| i as usize).collect();
                        barriers.push((pred.name.clone(), pred_usize));
                    }
                }
            }
        }
        barriers
    }
}

// Display functions
impl Graph {
    pub fn print_init_objects(&self) {
        if let Some(init_objects) = &self.init_objects {
            println!("Initialized Objects:");
            for (name, obj) in init_objects {
                println!("  {}: {:?}", name, obj);
            }
        } else {
            println!("No initialized objects.");
        }
    }

    pub fn print_graph(&self) {
        println!("Graph:");
        for (name, node) in &self.nodes {
            println!("  {}: {:?} ({:?})", name, node.name, node.factor);
        }
        if let Some(post_nodes) = &self.post_nodes {
            println!("Post Nodes:");
            for (name, node) in post_nodes {
                println!("  {}: {:?} ({:?})", name, node.name, node.factor);
            }
        } else {
            println!("No post nodes.");
        }
        println!("Initial Nodes: {:?}", self.initial_nodes);
        println!("Total Executed Nodes: {}", self.total_executed_nodes());
        println!("Successors: {:?}", self.successors);
        println!("Predecessor Indexes: {:?}", self.pred_idxs);
    }
}
