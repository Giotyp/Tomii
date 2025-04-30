use std::collections::HashMap;
use std::fs::File;
use std::io::Read;

use crate::cmtypes::*;
use crate::func_reg::*;
use crate::graph_struct::*;
use crate::obj_gen::init_objects;
use serde::Deserialize;
use serde_json;

#[derive(Debug, Deserialize)]
struct SuccessorsJson {
    task: String,
    indexes: String,
}

#[derive(Debug, Deserialize)]
struct TaskJson {
    arg_types: Vec<String>,
    args: Vec<String>,
    ref_tasks: Option<Vec<String>>,
    function_name: String,
}

#[derive(Debug, Deserialize)]
struct NodeJson {
    name: String,
    task: TaskJson,
    mult_factor: usize,
    successors: Vec<SuccessorsJson>,
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
        args.push(string_to_cmtype(arg_type.clone(), arg.clone()).unwrap());
    }

    let ref_tasks_opt = {
        if let Some(ref_tasks) = &task_json.ref_tasks {
            Some(ref_tasks.clone())
        } else {
            None
        }
    };

    // read environment variable to determine if the function is in python
    let func_path = std::env::var("FUNC_PATH").unwrap();
    let python: bool = func_path == "python";

    let mut func_ptr: Option<CmPtr> = None;
    if !python {
        func_ptr = get_func(&task_json.function_name);
    }
    Task::new(
        args,
        ref_tasks_opt,
        task_json.function_name.clone(),
        func_ptr,
    )
}

pub fn from_json(graph_json: &str) -> Result<Graph, serde_json::Error> {
    let mut file = File::open(graph_json).unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();

    // Parse JSON file with defined structure
    let root: RootJson = serde_json::from_str(&contents)?;

    // Create a new Graph
    let mut graph = Graph::new();

    let mut node_stages: HashMap<String, usize> = HashMap::new();

    for (stage_no, stage_json) in root.graph.stages.iter().enumerate() {
        // Create a new Stage
        let mut stage = Stage::new();

        // Iterate through parsed JSON Nodes and populate stage
        for node_json in stage_json.nodes.iter() {
            let task = parse_task(&node_json.task);
            let mult_factor: usize = node_json.mult_factor;
            let node = Node::new(node_json.name.clone(), task, mult_factor);
            stage.add_node(node);
            node_stages.insert(node_json.name.clone(), stage_no);
        }
        // Add stage to graph
        graph.add_stage(stage);
    }

    for (stage_no, stage_json) in root.graph.stages.iter().enumerate() {
        // Iterate through stages to add successors/dependents
        for node_json in stage_json.nodes.iter() {
            // Get node name
            let node_name = node_json.name.clone();

            if !node_json.successors.is_empty() {
                // Iterate through successors
                for succ_json in node_json.successors.iter() {
                    let succ_task = succ_json.task.clone();

                    // Add successor task name to node
                    let succ_stage = *node_stages.get(&succ_task).unwrap();
                    let node = graph.stage_mut(stage_no).node_mut(&node_name);
                    node.add_successor(succ_task.clone(), succ_stage);

                    // Find successor node and add current node as dependent
                    let succ_node = graph.stage_mut(succ_stage).node_mut(&succ_task);
                    succ_node.add_dependent(node_name.clone(), stage_no);

                    // Add successor indexes to node
                    let succ_indexes = succ_json.indexes.clone();

                    // 1st case: exact indexes ',' separated
                    if succ_indexes.contains(',') {
                        for successor_index in succ_indexes.split(",") {
                            // strip to remove whitespace
                            let successor_index = successor_index.trim();
                            let node = graph.stage_mut(stage_no).node_mut(&node_name);
                            node.add_successor_index(
                                succ_task.clone(),
                                successor_index.parse::<usize>().unwrap(),
                            );
                        }
                    }
                    // 2nd case: range indexes '-' separated
                    else if succ_indexes.contains('-') {
                        let range: Vec<&str> = succ_indexes.split("-").collect();
                        let start = range[0].parse::<usize>().unwrap();
                        let end = range[1].parse::<usize>().unwrap();
                        for i in start..end + 1 {
                            let node = graph.stage_mut(stage_no).node_mut(&node_name);
                            node.add_successor_index(succ_task.clone(), i);
                        }
                    } else {
                        // single successor
                        let node = graph.stage_mut(stage_no).node_mut(&node_name);
                        node.add_successor_index(
                            succ_task.clone(),
                            succ_indexes.parse::<usize>().unwrap(),
                        );
                    }
                }
            }
        }
    }

    // Check for initializations in the graph
    let init_objects = match init_objects(graph_json) {
        Ok(init_objects) => Some(init_objects),
        Err(_) => None,
    };
    // Set the initialized objects in the graph
    if let Some(init_objects) = init_objects {
        graph.set_init_objects(init_objects);
    }

    Ok(graph)
}

pub fn re_init_objects(graph: &mut Graph, graph_json: &str) {
    // Check for initializations in the graph
    let init_objects = match init_objects(graph_json) {
        Ok(init_objects) => Some(init_objects),
        Err(_) => None,
    };
    // Set the initialized objects in the graph
    if let Some(init_objects) = init_objects {
        graph.set_init_objects(init_objects);
    }
}
