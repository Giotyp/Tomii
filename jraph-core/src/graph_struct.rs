#![allow(dead_code)]

use crate::cmtypes::*;
use std::collections::HashMap;

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
                // if at least one evaluation fails, return false
                if arg_value != self.eval_value {
                    return false;
                }
            }
            CondOp::Neq => {
                if arg_value == self.eval_value {
                    return false;
                }
            }
            _ => {
                // Handle other operations (Gt, Lt) as needed
                // Currently returns false
                return false;
            }
        }
        // If all evaluations pass, return true
        true
    }
}

struct Predecessor {
    pub name: String,
    pub indexes: usize,
}

#[derive(Clone)]
pub struct Arg {
    pub value: Option<String>,
    pub type_: CmTypes,
    // Optional condition for initialization
    pub init_condition: Option<InitCondition>,
    pub predecessor: Option<Predecessor>,
}

#[derive(Clone)]
pub struct Node {
    pub name: String,
    pub args: Vec<Arg>,
    // Variable that defines the number of times
    // the node is initiated
    pub mult_factor: usize,
    pub func_ptr: Option<CmPtr>,
}

pub struct Graph {
    nodes: HashMap<String, Node>,
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

    pub fn node_mut(&mut self, node_name: &str) -> &mut Node {
        self.nodes.get_mut(node_name).unwrap()
    }

    pub fn add_node(&mut self, node: Node) {
        let node_name = node.name.clone();
        self.nodes.insert(node_name.clone(), node);
    }

    pub fn total_nodes(&self) -> usize {
        self.nodes.values().map(|node| node.mult_factor()).sum()
    }
}

/// Utility functions
impl Graph {
    pub fn new() -> Graph {
        Graph {
            nodes: HashMap::new(),
            init_objects: None,
        }
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn init_objects(&self) -> Option<&HashMap<String, Vec<CmTypes>>> {
        self.init_objects.as_ref()
    }

    pub fn set_init_objects(&mut self, init_objects: HashMap<String, Vec<CmTypes>>) {
        self.init_objects = Some(init_objects);
    }
}

// Display functions
impl Graph {
    /// Generates a tree-style DOT where each edge is predecessor -> node
    /// and every node is declared by its name.
    pub fn generate_dot(&self) -> String {
        let mut dot = String::from("digraph Tree {\n");
        dot.push_str("    node [shape=ellipse];\n\n");
        // declare all nodes, so even isolated ones appear
        for node_name in self.node_names() {
            dot.push_str(&format!("    \"{}\";\n", node_name));
        }
        dot.push('\n');
        // emit edges from each predecessor to this node
        for (node_name, node) in &self.nodes {
            let node_names = node.predecessors.keys().cloned().collect::<Vec<_>>();
            for pred in node_names {
                dot.push_str(&format!("    \"{}\" -> \"{}\";\n", pred, node_name));
            }
        }
        dot.push_str("}\n");
        dot
    }

    /// Pretty-print every node’s fields in a flat list.
    pub fn print_graph(&self) {
        println!("Graph:");
        for node_name in self.node_names() {
            let node = &self.nodes[&node_name];
            println!("  Node: {}", node.name());
            println!("    Mult-Factor: {}", node.mult_factor());
            if let Some(init_cond) = node.init_condition() {
                println!("    Init Condition: ");
                println!("      Args: {:?}", init_cond.args());
                println!("      Operations: {:?}", init_cond.operations());
                println!("      Values: {:?}", init_cond.values());
            }
            println!("    Task: {}", node.task().function_name());
            println!("      Args: {:?}", node.task().args());
            if let Some(ref_tasks) = node.task().ref_tasks() {
                println!("      Ref Tasks: {:?}", ref_tasks);
            }
            println!("    Pred Indexes: {:?}\n", node.predecessors);
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
}
