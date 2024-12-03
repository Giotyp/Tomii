use shared::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct Task {
    args: Vec<CmTypes>,
    function_path: String,
    function_name: String,
}

impl Task {
    pub fn new(args: Vec<CmTypes>, function_path: String, function_name: String) -> Task {
        Task {
            args,
            function_path,
            function_name,
        }
    }

    pub fn args(&self) -> &Vec<CmTypes> {
        &self.args
    }

    pub fn function_path(&self) -> &String {
        &self.function_path
    }

    pub fn function_name(&self) -> &String {
        &self.function_name
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
}

// Display functions
impl Graph {
    pub fn generate_dot(&self) -> String {
        let mut dot = String::from("digraph {\n");

        for stage in self.stages.iter() {
            for node in stage.nodes.values() {
                let node_read = node.read().unwrap();
                for successor in &node_read.successors {
                    let successor_name = successor.read().unwrap().name().clone();
                    dot.push_str(&format!(
                        "    \"{}\" -> \"{}\";\n",
                        node_read.name, successor_name
                    ));
                }
            }
        }

        dot.push_str("}\n");
        dot
    }

    pub fn print_graph(&self) {
        println!("Graph: ");
        for (stage_no, stage) in self.stages.iter().enumerate() {
            println!("  Stage {}: ", stage_no);
            for node in stage.nodes.values() {
                let node_read = node.read().unwrap();
                println!("      Node: {}", node_read.name);
                println!(
                    "          Task: {}::{}",
                    node_read.task.function_path, node_read.task.function_name
                );
                println!("              Args: {:?}", node_read.task.args);
                println!(
                    "          Successors: {:?}",
                    node_read
                        .successors
                        .iter()
                        .map(|s| s.read().unwrap().name().clone())
                        .collect::<Vec<String>>()
                );
                println!(
                    "          Dependents: {:?}",
                    node_read
                        .dependents
                        .iter()
                        .map(|d| d.read().unwrap().name().clone())
                        .collect::<Vec<String>>()
                );
            }
        }
    }
}
