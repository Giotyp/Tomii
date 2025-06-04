use std::fs::File;
use std::io::Read;

use crate::cmtypes::*;
use crate::graph_struct::*;
use crate::obj_gen::init_objects;
use crate::registry::get_func;
use serde::Deserialize;
use serde_json;

#[derive(Debug, Deserialize)]
struct ConditionJson {
    operation: String,
    value: String,
    value_type: String,
}

#[derive(Debug, Deserialize)]
struct PredJson {
    name: String,
    indexes: String,
}

#[derive(Debug, Deserialize)]
struct ArgJson {
    #[serde(rename = "type")]
    type_: String,
    value: Option<String>,
    condition: Option<ConditionJson>,
    predecessor: Option<PredJson>,
}

#[derive(Debug, Deserialize)]
struct NodeJson {
    name: String,
    factor: Option<usize>,
    function_name: String,
    #[serde(rename = "loop")]
    loop_: Option<String>,
    loop_args: Option<Vec<ArgJson>>,
    args: Vec<ArgJson>,
}

#[derive(Debug, Deserialize)]
struct GraphFile {
    nodes: Vec<NodeJson>,
}

fn parse_arg(arg_json: &ArgJson) -> Arg {
    let arg_value_opt = arg_json.value.clone();

    // Check if the argument has a condition
    let condition: Option<InitCondition> = {
        if let Some(condition_json) = &arg_json.condition {
            Some(parse_condition(condition_json))
        } else {
            None
        }
    };

    let predecessor: Option<Predecessor> = {
        // Check if the argument has a predecessor
        if let Some(pred_json) = &arg_json.predecessor {
            Some(parse_predecessor(pred_json))
        } else {
            None
        }
    };

    let arg_cmtype = {
        let type_json = arg_json.type_.clone();
        if predecessor.is_some() {
            let name = predecessor.as_ref().unwrap().name.clone();
            string_to_cmtype(type_json.clone(), name).unwrap()
        } else {
            if arg_value_opt.is_some() {
                string_to_cmtype(type_json.clone(), arg_value_opt.clone().unwrap()).unwrap()
            } else {
                // This should not happen
                CmTypes::None()
            }
        }
    };

    let arg = Arg {
        value: arg_value_opt,
        type_: arg_cmtype,
        init_condition: condition,
        predecessor,
    };
    arg
}

fn parse_predecessor(pred_json: &PredJson) -> Predecessor {
    let pred_name = pred_json.name.clone();
    let mut index_vec = Vec::new();
    let indexes = pred_json.indexes.clone();

    // 1st case: exact indexes ',' separated
    if indexes.contains(',') {
        for predecessor_index in indexes.split(",") {
            // strip to remove whitespace
            let predecessor_index = predecessor_index.trim();
            index_vec.push(predecessor_index.parse::<usize>().unwrap());
        }
    }
    // 2nd case: range indexes '-' separated
    else if indexes.contains('-') {
        let range: Vec<&str> = indexes.split("-").collect();
        let start = range[0].parse::<usize>().unwrap();
        let end = range[1].parse::<usize>().unwrap();
        for i in start..end + 1 {
            index_vec.push(i);
        }
    } else {
        // single predecessor
        index_vec.push(indexes.parse::<usize>().unwrap());
    }

    Predecessor {
        name: pred_name,
        indexes: index_vec,
    }
}

fn parse_condition(condition_json: &ConditionJson) -> InitCondition {
    let operation = {
        let op = CondOp::from_str(&condition_json.operation);
        if op.is_some() {
            op.unwrap()
        } else {
            panic!("Invalid operation: {}", condition_json.operation);
        }
    };

    let eval_value = string_to_cmtype(
        condition_json.value_type.clone(),
        condition_json.value.clone(),
    )
    .unwrap();

    InitCondition {
        operation,
        eval_value,
    }
}

pub fn from_json(graph_json: &str) -> Result<Graph, serde_json::Error> {
    let mut file = File::open(graph_json).unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();

    // Parse JSON file with defined structure
    let graph_parsed: GraphFile = serde_json::from_str(&contents)?;

    // Create a new Graph
    let mut graph = Graph::new();

    for node_json in &graph_parsed.nodes {
        let mut args = Vec::new();
        let mut loop_args_vec = Vec::new();

        for arg_json in &node_json.args {
            args.push(parse_arg(arg_json));
        }

        if let Some(loop_args_json) = &node_json.loop_args {
            for arg_json in loop_args_json {
                loop_args_vec.push(parse_arg(arg_json));
            }
        }

        let loop_args = {
            if loop_args_vec.is_empty() {
                None
            } else {
                Some(loop_args_vec)
            }
        };

        let func_ptr = get_func(&node_json.function_name);

        let factor = match node_json.factor {
            Some(factor) => factor,
            None => 1,
        };

        let node = Node {
            name: node_json.name.clone(),
            args,
            loop_args,
            factor: factor,
            func_ptr,
            loop_: node_json.loop_.clone(),
        };

        graph.add_node(node);
    }

    // Check for initializations in the graph
    let init_objects = match init_objects(graph_json) {
        Ok(init_objects) => Some(init_objects),
        Err(e) => {
            eprintln!("Error parsing initial objects: {}", e);
            None
        }
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
