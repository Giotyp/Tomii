#![allow(dead_code)]

use crate::debug::print_debug;
use std::collections::HashMap;
use synstream_types::*;

/// Comparison operators
#[derive(Clone, Debug)]
pub enum CondOp {
    Eq,
    Neq,
    Gt,
    Lt,
}

impl CondOp {
    pub fn from_str(op: &str) -> Option<CondOp> {
        match op {
            "Eq" => Some(CondOp::Eq),
            "Neq" => Some(CondOp::Neq),
            "Gt" => Some(CondOp::Gt),
            "Lt" => Some(CondOp::Lt),
            _ => None,
        }
    }
}

/// Node Initialization  (Optional) Condition
#[derive(Clone, Debug)]
pub struct InitCondition {
    pub operation: CondOp,
    pub eval_value: CmTypes,
}

impl InitCondition {
    pub fn evaluate(&self, arg_value: CmTypes) -> bool {
        // Evaluate against arg_value that is decided during runtime

        match self.operation {
            CondOp::Eq => {
                if arg_value == self.eval_value {
                    return true;
                }
            }
            CondOp::Neq => {
                if arg_value != self.eval_value {
                    return true;
                }
            }
            _ => {
                // Handle other operations (Gt, Lt) as needed
                // Currently returns false
                return false;
            }
        }
        false
    }
}

#[derive(Clone)]
pub struct Predecessor {
    pub name: String,
    pub indexes: Vec<usize>,
}

#[derive(Clone)]
pub struct Arg {
    pub value: Option<String>,
    pub type_: CmTypes,
    // Optional condition for initialization
    pub init_condition: Option<InitCondition>,
    pub predecessor: Option<Predecessor>,
}

impl Arg {
    pub fn is_condition(&self) -> bool {
        self.init_condition.is_some()
    }
}

#[derive(Clone)]
pub struct Loop {
    pub name: String,
    pub factor: usize,
}

#[derive(Clone)]
pub struct Node {
    pub name: String,
    pub args: Vec<Arg>,
    pub loop_args: Option<Vec<Arg>>,
    // Variable that defines the number of times
    // the node is initiated
    pub factor: usize,
    pub func_ptr: Option<CmPtr>,
    // Optional node to loop after execution
    pub loop_: Option<Loop>,
}

impl Node {
    pub fn condition_args(&self) -> Vec<&Arg> {
        let mut cond_args: Vec<&Arg> = Vec::new();
        for arg in &self.args {
            if arg.is_condition() {
                cond_args.push(arg);
            }
        }
        cond_args
    }

    pub fn predecessor_names(&self) -> Vec<String> {
        let mut pred_names: Vec<String> = Vec::new();
        for arg in &self.args {
            if let Some(pred) = &arg.predecessor {
                pred_names.push(pred.name.clone());
            }
        }
        pred_names
    }
}

#[derive(Clone)]
pub struct IdFunction {
    pub func_ptr: Option<CmPtr>,
    pub predecessor: String,
    pub args: Vec<Arg>,
}

#[derive(Clone)]
pub struct Graph {
    nodes: HashMap<String, Node>,
    id_function: Option<IdFunction>,
    post_nodes: Option<HashMap<String, Node>>,
    // keep a list of nodes that are connected
    connect_list: Vec<Vec<String>>,
    // buffer list that need to be registere in connect_list
    // in case the json description is not in order
    buffer_list: Vec<(String, Vec<String>)>,
    init_objects: Option<HashMap<String, Vec<CmTypes>>>,
}

/// Node functions
impl Graph {
    pub fn node_names(&self) -> Vec<String> {
        self.nodes.keys().cloned().collect()
    }

    pub fn node(&self, node_name: &str) -> &Node {
        &self.nodes[node_name]
    }

    pub fn nodes_map(&self) -> &HashMap<String, Node> {
        &self.nodes
    }

    pub fn post_nodes_map(&self) -> Option<&HashMap<String, Node>> {
        self.post_nodes.as_ref()
    }

    pub fn node_mut(&mut self, node_name: &str) -> &mut Node {
        self.nodes.get_mut(node_name).unwrap()
    }

    pub fn add_node(&mut self, node: Node) {
        let node_name = node.name.clone();
        let predecessors = node.predecessor_names();
        self.nodes.insert(node_name.clone(), node);
        // update connections
        self.update_connections(&node_name, predecessors);
    }

    pub fn add_post_node(&mut self, node: Node) {
        if let Some(post_nodes) = &mut self.post_nodes {
            post_nodes.insert(node.name.clone(), node);
        } else {
            let mut post_nodes = HashMap::new();
            post_nodes.insert(node.name.clone(), node);
            self.post_nodes = Some(post_nodes);
        }
    }

    pub fn total_nodes(&self) -> usize {
        self.nodes.values().map(|node| node.factor).sum()
    }

    pub fn total_nodes_with_conditions(&self) -> usize {
        let mut total = 0;
        let mut condition_predecessors: HashMap<String, usize> = HashMap::new();
        let mut nodes_with_same_condition: HashMap<String, Vec<String>> = HashMap::new();

        // First pass: identify all nodes with conditions and group them by predecessor
        for (node_name, node) in &self.nodes {
            let mut has_condition = false;

            for arg in &node.args {
                if let Some(_) = &arg.init_condition {
                    has_condition = true;

                    // Check what type of condition this is
                    match &arg.type_ {
                        CmTypes::Ref(obj_name) => {
                            // For Ref conditions, use the object name as the key
                            nodes_with_same_condition
                                .entry(format!("ref:{}", obj_name))
                                .or_insert_with(Vec::new)
                                .push(node_name.clone());
                        }
                        CmTypes::Res(pred_name) => {
                            // For Res conditions, use the predecessor name
                            if let Some(pred_node) = self.nodes.get(pred_name) {
                                let key = format!("pred:{}", pred_name);
                                nodes_with_same_condition
                                    .entry(key.clone())
                                    .or_insert_with(Vec::new)
                                    .push(node_name.clone());

                                // Store the predecessor's factor
                                condition_predecessors.insert(key, pred_node.factor);
                            }
                        }
                        _ => {}
                    }
                    break;
                }
            }

            if !has_condition {
                total += node.factor;
            }
        }

        // Second pass: for each group of nodes with the same condition,
        // add the factor only once (use the predecessor's factor)
        for (condition_key, node_names) in &nodes_with_same_condition {
            if condition_key.starts_with("pred:") {
                // Use predecessor's factor
                if let Some(&pred_factor) = condition_predecessors.get(condition_key) {
                    total += pred_factor;
                }
            } else if condition_key.starts_with("ref:") {
                // For reference conditions, use the factor of the first node in the group
                if let Some(first_node_name) = node_names.first() {
                    if let Some(first_node) = self.nodes.get(first_node_name) {
                        total += first_node.factor;
                    }
                }
            }
        }
        total
    }

    pub fn analyze_conditional_nodes(&self) -> (Vec<String>, HashMap<String, Vec<String>>) {
        let mut unconditional_nodes = Vec::new();
        let mut conditional_groups: HashMap<String, Vec<String>> = HashMap::new();

        for (node_name, node) in &self.nodes {
            let mut has_condition = false;

            for arg in &node.args {
                if let Some(_) = &arg.init_condition {
                    has_condition = true;

                    let group_key = match &arg.type_ {
                        CmTypes::Ref(obj_name) => format!("ref:{}", obj_name),
                        CmTypes::Res(pred_name) => format!("pred:{}", pred_name),
                        CmTypes::Barrier(pred_name) => format!("barrier:{}", pred_name),
                        _ => "unknown".to_string(),
                    };

                    conditional_groups
                        .entry(group_key)
                        .or_insert_with(Vec::new)
                        .push(node_name.clone());
                    break; // Only consider first condition
                }
            }

            if !has_condition {
                unconditional_nodes.push(node_name.clone());
            }
        }

        (unconditional_nodes, conditional_groups)
    }

    pub fn connect_list(&self) -> &Vec<Vec<String>> {
        &self.connect_list
    }

    pub fn node_connections(&self, node_name: &str) -> Option<Vec<String>> {
        for (i, connected_nodes) in self.connect_list.iter().enumerate() {
            if connected_nodes.contains(&node_name.to_string()) {
                // Get vector beggining from offset i
                return Some(connected_nodes[i..].to_vec());
            }
        }
        None
    }

    fn update_connections(&mut self, node_name: &str, predecessors: Vec<String>) {
        if self.connect_list.is_empty() || predecessors.is_empty() {
            self.connect_list.push(vec![node_name.to_string()]);
        } else {
            // check buffer list first
            let buffed_length = self.buffer_list.len();
            for _ in 0..buffed_length {
                let (name, preds) = self.buffer_list.pop().unwrap();
                self.add_connect_list(&name, preds);
            }
            // check for given node
            self.add_connect_list(node_name, predecessors);
        }
    }

    fn add_connect_list(&mut self, node_name: &str, predecessors: Vec<String>) {
        for connected_nodes in &mut self.connect_list {
            for pred in &predecessors {
                if connected_nodes.contains(pred) {
                    connected_nodes.push(node_name.to_string());
                    return;
                }
            }
        }
        // predecessor not yet inserted
        // add to the buffer list
        let buf_node = (node_name.to_string(), predecessors);
        if !self.buffer_list.contains(&buf_node) {
            self.buffer_list.push(buf_node);
        }
    }
}

/// Utility functions
impl Graph {
    pub fn new() -> Graph {
        Graph {
            nodes: HashMap::new(),
            id_function: None,
            post_nodes: None,
            connect_list: Vec::new(),
            buffer_list: Vec::new(),
            init_objects: None,
        }
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn init_objects(&self) -> Option<&HashMap<String, Vec<CmTypes>>> {
        self.init_objects.as_ref()
    }

    pub fn id_function(&self) -> Option<&IdFunction> {
        self.id_function.as_ref()
    }

    pub fn set_init_objects(&mut self, init_objects: &HashMap<String, Vec<CmTypes>>) {
        self.init_objects = Some(init_objects.clone());
    }

    pub fn set_nodes(&mut self, nodes: HashMap<String, Node>) {
        self.nodes = nodes;
    }

    pub fn set_id_function(&mut self, id_function: &IdFunction) {
        self.id_function = Some(id_function.clone());
    }

    pub fn set_post_nodes(&mut self, post_nodes: Option<HashMap<String, Node>>) {
        self.post_nodes = post_nodes;
    }

    pub fn set_connect_list(&mut self, connect_list: Vec<Vec<String>>) {
        self.connect_list = connect_list;
    }

    pub fn change_node_factor(&mut self, node_name: &str, factor: usize) {
        if let Some(node) = self.nodes.get_mut(node_name) {
            print_debug(&format!(
                "Changing factor of node {} from {} to {}",
                node_name, node.factor, factor
            ));
            node.factor = factor;
        } else {
            panic!("Node {} not found in the graph", node_name);
        }
    }
}

// Display functions
impl Graph {
    /// Pretty-print every node’s fields in a flat list.
    pub fn print_graph(&self) {
        println!("Graph:");
        for node_name in self.node_names() {
            let node = &self.nodes[&node_name];
            println!("  Node: {}", node.name);
            println!("    Mult-Factor: {}", node.factor);
            println!("    Args: ");
            for arg in &node.args {
                println!("     Value: {:?}", arg.value);
                println!("     Type: {:?}", arg.type_);
                if let Some(init_cond) = &arg.init_condition {
                    println!("     Init Condition: {:?}", init_cond);
                    println!("      Operation: {:?}", init_cond.operation);
                    println!("      Eval Value: {:?}", init_cond.eval_value);
                }
                if let Some(pred) = &arg.predecessor {
                    println!("     Predecessor: {:?}", pred.name);
                    println!("      Name: {:?}", pred.name);
                    println!("      Indexes: {:?}", pred.indexes);
                }
            }
        }
    }

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

    pub fn print_conditional_analysis(&self) {
        let (unconditional_nodes, conditional_groups) = self.analyze_conditional_nodes();

        println!("Node Analysis:");
        println!("  Unconditional nodes: {:?}", unconditional_nodes);
        println!("  Conditional groups:");

        for (group_key, node_names) in &conditional_groups {
            if group_key.starts_with("pred:") {
                let pred_name = &group_key[5..]; // Remove "pred:" prefix
                if let Some(pred_node) = self.nodes.get(pred_name) {
                    println!(
                        "    {}: {:?} (predecessor factor: {})",
                        group_key, node_names, pred_node.factor
                    );
                }
            } else {
                println!("    {}: {:?}", group_key, node_names);
            }
        }

        println!("  Total nodes (simple): {}", self.total_nodes());
        println!(
            "  Total nodes (with conditions): {}",
            self.total_nodes_with_conditions()
        );
    }
}
