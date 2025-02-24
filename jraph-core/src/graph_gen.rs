use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::fs::File;
use std::io::Read;

use crate::graph_struct::*;
use crate::func_reg::*;
use shared::*;
use serde::Deserialize;
use serde_json;

#[derive(Debug, Deserialize)]
struct TaskJson {
    arg_types: Vec<String>,
    args: Vec<String>,
    function_path: String,
    function_name: String,
}

#[derive(Debug, Deserialize)]
struct NodeJson {
    name: String,
    task: TaskJson,
    mult_factor: usize,
    successors: String,
    successors_index: String
}

#[derive(Debug, Deserialize)]
struct StageJson {
    nodes: Vec<NodeJson>,
}

#[derive(Debug, Deserialize)]
struct GraphJson {
    stages: Vec<StageJson>,
}

#[derive(Debug, Deserialize)]
struct RootJson {
    graph: GraphJson,
}

fn parse_task(task_json: &TaskJson) -> Task {
    let mut args = Vec::new();
    for (arg_type, arg) in task_json.arg_types.iter().zip(task_json.args.iter()) {
        args.push(string_to_primitive(arg_type.clone(), arg.clone()).unwrap());
    }

    // read environment variable to determine if the function is in python
    let func_path = std::env::var("FUNC_PATH").unwrap();
    let python: bool = func_path == "python";

    let mut func_ptr: Option<CmPtr> = None;
    if !python {
      func_ptr = get_func(&task_json.function_name); 
    }
    Task::new(args, task_json.function_path.clone(), task_json.function_name.clone(), func_ptr)
}

pub fn from_json(graph_json: &str) -> Result<Graph, serde_json::Error> {

    let mut file = File::open(graph_json).unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let root: RootJson = serde_json::from_str(&contents)?;
    
    let mut graph = Graph::new();

    let mut node_stages: HashMap<String, usize> = HashMap::new();

    for (stage_no, stage_json) in root.graph.stages.iter().enumerate() {
        let mut stage = Stage::new();

        for node_json in stage_json.nodes.iter() {
            let task = parse_task(&node_json.task);
            let mult_factor: usize = node_json.mult_factor;
            let node = Arc::new(RwLock::new(Node::new(node_json.name.clone(), task, mult_factor)));
            stage.add_node(node);
            node_stages.insert(node_json.name.clone(), stage_no);
        }

        graph.add_stage(stage);
    }

    for (stage_no, stage_json) in root.graph.stages.iter().enumerate() {
        for node_json in stage_json.nodes.iter() {
            let node_name = node_json.name.clone();

            if !node_json.successors.is_empty() {
                for successor_name in node_json.successors.split(",") {
                    let successor_name = successor_name.trim();
                    let succ_stage = *node_stages.get(successor_name).unwrap();

                    let succ_node = graph.stage(succ_stage).node(successor_name).unwrap();
                    let node = graph.stage(stage_no).node(&node_name).unwrap();

                    node.write().unwrap().add_successor(succ_node.clone());
                    succ_node.write().unwrap().add_dependent(node.clone());
                }
                for successor_index in node_json.successors_index.split(",") {

                  let node = graph.stage(stage_no).node(&node_name).unwrap();

                  node.write().unwrap().add_successor_index(successor_index.to_string());
              }
            }
        }
    }

    Ok(graph)
}
