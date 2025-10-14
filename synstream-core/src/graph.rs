use crate::graph_struct::*;
use crate::{debug::print_debug, IdType};
use synstream_types::*;

/// Graph structure
#[derive(Clone)]
pub struct Graph {
    pub nodes: Vec<Node>,
    pub initial_nodes: Vec<IdType>,
    successors: Vec<Vec<IdType>>,
    pub id_function: Option<IdFunction>,
    pub post_nodes: Option<Vec<Node>>,
    pub init_objects: Option<Vec<Vec<CmTypes>>>,
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
            print_debug(&format!(
                "Adding initial node: {} with id {} and factor {}",
                node.name, node.id, node.factor
            ));
            self.initial_nodes.push(node.id);
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

    fn find_successors(&self, node_id: IdType, node_index: usize) -> Vec<(IdType, Vec<usize>)> {
        let mut next_nodes: Vec<(IdType, Vec<usize>)> = Vec::new();

        if node_id as usize >= self.successors.len() {
            return next_nodes;
        }

        let successor_ids = self.successors[node_id as usize].clone();

        for succ_id in successor_ids {
            // If succ_id has barrier, all indexes are returns
            if self.has_barrier(succ_id) {
                let factor = self.nodes[succ_id as usize].factor;
                let all_indexes: Vec<usize> = (0..factor).collect();
                next_nodes.push((succ_id, all_indexes));
                continue;
            }

            // If succ_id does not have barrier, find specific indexes
            let indexes = self.get_pred_indexes(succ_id, node_id);
            // Adjust index
            for dep_idx in indexes {
                let pred_factor = self.nodes[node_id as usize].factor;
                let mut succ_factor = self.nodes[succ_id as usize].factor;

                if pred_factor > succ_factor {
                    succ_factor = pred_factor;
                }

                let succ_indexes =
                    calculate_succ_indexes(pred_factor, succ_factor, node_index, dep_idx);
                next_nodes.push((succ_id, succ_indexes));
            }
        }
        next_nodes
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

    fn total_executed_nodes(&self) -> usize {
        self.nodes.iter().map(|n| n.factor).sum()
    }
}

impl Graph {
    pub fn new() -> Graph {
        Graph {
            nodes: Vec::new(),
            initial_nodes: Vec::new(),
            successors: Vec::new(),
            id_function: None,
            post_nodes: None,
            init_objects: None,
        }
    }

    pub fn set_nodes(&mut self, nodes: Vec<Node>) {
        self.nodes = nodes;
    }

    pub fn set_init_objects(&mut self, init_objects: &Vec<Vec<CmTypes>>) {
        self.init_objects = Some(init_objects.clone());
    }

    pub fn set_id_function(&mut self, id_function: &IdFunction) {
        self.id_function = Some(id_function.clone());
    }

    pub fn set_post_nodes(&mut self, post_nodes: Option<Vec<Node>>) {
        self.post_nodes = post_nodes;
    }

    pub fn get_condition_predecessors(&self) -> usize {
        let mut total = 0;
        for node in &self.nodes {
            for arg in &node.args {
                if let Some(_) = &arg.init_condition {
                    // Check what type of condition this is
                    match &arg.type_ {
                        CmTypes::Res(pred_id) => {
                            // For Res conditions, use the predecessor name
                            let pred_node = &self.nodes[*pred_id];
                            // Store the predecessor's factor
                            total += pred_node.factor;
                        }
                        _ => {}
                    }
                    break;
                }
            }
        }
        total
    }

    pub fn get_condition_nodes(&self) -> (Vec<IdType>, Vec<Vec<usize>>) {
        let mut condition_nodes: Vec<IdType> = Vec::new();
        let mut arg_indexes: Vec<Vec<usize>> = Vec::new();

        for node in &self.nodes {
            let condition_arg_indexes: Vec<usize> = node
                .args
                .iter()
                .enumerate()
                .filter_map(|(idx, arg)| arg.init_condition.as_ref().map(|_| idx))
                .collect();

            if !condition_arg_indexes.is_empty() {
                condition_nodes.push(node.id);
                arg_indexes.push(condition_arg_indexes);
            }
        }

        (condition_nodes, arg_indexes)
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

    pub fn get_pred_indexes(&self, node_id: IdType, pred_id: IdType) -> Vec<isize> {
        let node = &self.nodes[node_id as usize];
        let args = &node.args;
        for arg in args {
            if let Some(pred) = &arg.predecessor {
                if pred.id == pred_id {
                    return pred.indexes.clone();
                }
            }
        }
        Vec::new()
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
        println!("Total Executed Nodes: {}", self.total_executed_nodes());
        println!("Successors: {:?}", self.successors);
    }
}
