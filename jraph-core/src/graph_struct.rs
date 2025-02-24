#![allow(dead_code)]

use shared::*;
use std::collections::HashMap;

pub struct Task {
    args: Vec<CmTypes>,
    function_name: String,
    func_ptr: Option<CmPtr>,
}

impl Task {
    pub fn new(
        args: Vec<CmTypes>,
        function_name: String,
        func_ptr: Option<CmPtr>,
    ) -> Task {
        Task {
            args,
            function_name,
            func_ptr,
        }
    }

    pub fn args(&self) -> &Vec<CmTypes> {
        &self.args
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
    successors_index: HashMap<String, Vec<usize>>,
    successors: Vec<(String, usize)>,
    dependents: Vec<(String, usize)>,
}

impl Node {
    pub fn new(name: String, task: Task, mult_factor: usize) -> Node {
        Node {
            name,
            task,
            mult_factor,
            successors_index: HashMap::new(),
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

    pub fn successors(&self) -> &Vec<(String, usize)> {
        &self.successors
    }

    pub fn successors_index(&self) -> &HashMap<String, Vec<usize>> {
        &self.successors_index
    }

    pub fn dependents(&self) -> &Vec<(String, usize)> {
        &self.dependents
    }

    pub fn add_successor(&mut self, successor: String, stage_no: usize) {
        self.successors.push((successor, stage_no));
    }

    pub fn add_successor_index(&mut self, successor_name: String, successor_index: usize) {
        self.successors_index
            .entry(successor_name)
            .or_insert(Vec::new())
            .push(successor_index);
    }

    pub fn add_dependent(&mut self, dependent: String, stage_no: usize) {
        self.dependents.push((dependent, stage_no));
    }

}

pub struct Stage {
    nodes: HashMap<String, Node>,
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

    pub fn node(&self, node_name: &str) -> &Node {
        &self.nodes[node_name]
    }

    pub fn node_mut(&mut self, node_name: &str) -> &mut Node {
        self.nodes.get_mut(node_name).unwrap()
    }

    pub fn add_node(&mut self, node: Node) {
        let node_name = node.name.clone();
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

    pub fn stage_mut(&mut self, stage_no: usize) -> &mut Stage {
        &mut self.stages[stage_no]
    }

    pub fn add_stage(&mut self, stage: Stage) {
        self.stages.push(stage);
    }
}

// Display functions
impl Graph {
    pub fn generate_dot(&self) -> String {
        let mut dot = String::from("digraph {\n");

        for (stage_idx, stage) in self.stages.iter().enumerate() {
            for (node_name, node) in &stage.nodes {
                for (successor_name, _) in &node.successors {
                    dot.push_str(&format!(
                        "    \"Stage{}::{}\" -> \"Stage{}::{}\";\n",
                        stage_idx, node_name, stage_idx + 1, successor_name
                    ));
                }
            }
        }

        dot.push_str("}\n");
        dot
    }

    pub fn print_graph(&self) {
        println!("Graph:");
        for (stage_no, stage) in self.stages.iter().enumerate() {
            println!("  Stage {}: ", stage_no);
            for node_name in stage.node_names() {
                let node = &stage.nodes[node_name];
                println!("      Node: {}", node.name);
                println!("          Mult-Factor: {}", node.mult_factor);
                println!("          Task: {}", node.task.function_name);
                println!("              Args: {:?}", node.task.args);
                println!("          Successors: {:?}", node.successors);
                println!("          Successors Index: {:?}", node.successors_index);
                println!("          Dependents: {:?}", node.dependents);
            }
        }
    }
}
