use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct Task {
    args: i32,
    output: bool,
}

impl Task {
    pub fn new(args: i32, output: bool) -> Task {
        Task { args, output }
    }

    pub fn args(&self) -> i32 {
        self.args
    }

    pub fn output(&self) -> bool {
        self.output
    }
}

pub struct Node {
    name: String,
    task: Task,
    successors: Vec<Arc<RwLock<Node>>>,
    dependents: Vec<Arc<RwLock<Node>>>,
}

impl Node {
    pub fn new(name: String, task: Task) -> Node {
        Node {
            name,
            task,
            successors: Vec::new(),
            dependents: Vec::new(),
        }
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn task(&self) -> &Task {
        &self.task
    }

    pub fn add_successor(&mut self, successor: Arc<RwLock<Node>>) {
        self.successors.push(successor);
    }

    pub fn add_dependent(&mut self, dependent: Arc<RwLock<Node>>) {
        self.dependents.push(dependent);
    }
}

pub struct Stage {
    nodes: HashMap<String, Arc<RwLock<Node>>>,
}

impl Stage {
    pub fn new() -> Stage {
        Stage {
            nodes: HashMap::new(),
        }
    }

    pub fn node(&self, node_name: &str) -> Option<&Arc<RwLock<Node>>> {
        self.nodes.get(node_name)
    }

    pub fn add_node(&mut self, node: Arc<RwLock<Node>>) {
        let node_name = node.read().unwrap().name().clone();
        self.nodes.insert(node_name, node);
    }
}

pub struct Graph {
    stages: Vec<Stage>,
}

impl Graph {
    pub fn new() -> Graph {
        Graph { stages: Vec::new() }
    }

    pub fn stage(&self, stage_no: usize) -> &Stage {
        &self.stages[stage_no]
    }

    pub fn add_stage(&mut self, stage: Stage) {
        self.stages.push(stage);
    }

    pub fn generate_dot(&self) -> String {
        let mut dot = String::from("digraph {\n");
    
        for stage in self.stages.iter() {
            for node in stage.nodes.values() {
                let node_read = node.read().unwrap();
                for successor in &node_read.successors {
                    let successor_name = successor.read().unwrap().name().clone();
                    dot.push_str(&format!("    \"{}\" -> \"{}\";\n", node_read.name, successor_name));
                }
            }
        }
    
        dot.push_str("}\n");
        dot
    }
}
