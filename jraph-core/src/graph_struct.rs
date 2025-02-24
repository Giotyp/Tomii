#![allow(dead_code)]

use shared::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct Task {
    args: Vec<CmTypes>,
    function_path: String,
    function_name: String,
    func_ptr: Option<CmPtr>,
}

impl Task {
    pub fn new(args: Vec<CmTypes>, function_path: String, function_name: String, func_ptr: Option<CmPtr>) -> Task {
        Task {
            args,
            function_path,
            function_name,
            func_ptr
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

    pub fn func_ptr(&self) -> Option<CmPtr> {
        self.func_ptr
    }
}

pub struct Node {
    name: String,
    task: Task,
    mult_factor: usize,
    successors_index: Vec<String>,
    successors: Vec<Arc<RwLock<Node>>>,
    dependents: Vec<Arc<RwLock<Node>>>,
}

impl Node {
    pub fn new(name: String, task: Task, mult_factor: usize) -> Node {
        Node {
            name,
            task,
            mult_factor,
            successors_index: Vec::new(),
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

    pub fn mult_factor(&self) -> usize {
        self.mult_factor
    }

    pub fn successors_index(&self) -> &Vec<String> {
        &self.successors_index
    }

    pub fn add_successor_index(&mut self, successor_index: String) {
        self.successors_index.push(successor_index);
    }

    pub fn add_successor(&mut self, successor: Arc<RwLock<Node>>) {
        self.successors.push(successor);
    }

    pub fn successors_names(&self) -> Vec<String> {
        self.successors
            .iter()
            .map(|s| s.read().unwrap().name().clone())
            .collect()
    }

    pub fn add_dependent(&mut self, dependent: Arc<RwLock<Node>>) {
        self.dependents.push(dependent);
    }

    pub fn dependents_names(&self) -> Vec<String> {
        self.dependents
            .iter()
            .map(|d| d.read().unwrap().name().clone())
            .collect()
    }
}

pub struct Stage {
    nodes: HashMap<String, Arc<RwLock<Node>>>,
    node_names: Vec<String>,
}

impl Stage {
    pub fn new() -> Stage {
        Stage {
            nodes: HashMap::new(),
            node_names: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn node_names(&self) -> &Vec<String> {
        &self.node_names
    }

    pub fn node(&self, node_name: &str) -> Option<&Arc<RwLock<Node>>> {
        self.nodes.get(node_name)
    }

    pub fn add_node(&mut self, node: Arc<RwLock<Node>>) {
        let node_name = node.read().unwrap().name().clone();
        self.nodes.insert(node_name.clone(), node);
        self.node_names.push(node_name);
    }
}

pub struct Graph {
    stages: Vec<Stage>,
}

impl Graph {
    pub fn new() -> Graph {
        Graph { stages: Vec::new() }
    }

    pub fn len(&self) -> usize {
        self.stages.len()
    }

    pub fn stage(&self, stage_no: usize) -> &Stage {
        &self.stages[stage_no]
    }

    pub fn add_stage(&mut self, stage: Stage) {
        self.stages.push(stage);
    }

    pub fn node_info(&self, stage_no: usize, node_name: &str) -> HashMap<String, String> {
        let node = self.stage(stage_no).node(node_name).unwrap();
        let node = node.read().unwrap();
        let task = node.task();

        let mult_factor = node.mult_factor();
        let succ_index = node.successors_index();
        let succ_names = node.successors_names();
        let dep_names = node.dependents_names();
        let function_name = task.function_name();
        let function_path = task.function_path();
        let args_enum = task.args();
        let args_vec: Vec<String> = args_enum.iter().map(|x| x.to_string()).collect();

        let info = HashMap::from([
            ("mult_factor".to_string(), mult_factor.to_string()),
            ("function_path".to_string(), function_path.clone()),
            ("function_name".to_string(), function_name.clone()),
            ("successors_index".to_string(), succ_index.join(", ")),
            ("successors_names".to_string(), succ_names.join(", ")),
            ("dependents_names".to_string(), dep_names.join(", ")),
            ("args".to_string(), args_vec.join(", "))
        ]);
        info
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
                  "          Mult-Factor: {}",
                  node_read.mult_factor
              );
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
