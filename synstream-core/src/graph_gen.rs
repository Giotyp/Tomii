use core::panic;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;

use crate::func_reg::*;
use crate::graph::*;
use crate::graph_struct::*;
use crate::json_structs::*;
use crate::network::{NetworkConfig, SocketType};
use crate::obj_gen::init_objects;
use crate::prelude::*;
use rapidhash::{HashMapExt, RapidHashMap};
use serde_json;
use std::sync::atomic::Ordering::SeqCst;
use synstream_types::*;

fn parse_arg(
    arg_json: &ArgJson,
    init_objects: &Vec<Vec<CmTypes>>,
    obj_id_map: &RapidHashMap<String, usize>,
    name_to_id: &RapidHashMap<String, IdType>,
) -> Arg {
    let arg_value_opt = arg_json.value.as_deref();

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
            Some(parse_predecessor(
                pred_json,
                init_objects,
                obj_id_map,
                &name_to_id,
            ))
        } else {
            None
        }
    };

    let arg_cmtype = {
        let type_json = &arg_json.type_;
        if predecessor.is_some() {
            let id = predecessor.as_ref().unwrap().id;
            string_to_cmtype(type_json.to_string(), id.to_string()).unwrap()
        } else {
            if let Some(arg_value) = arg_value_opt {
                if let Some(obj_id) = obj_id_map.get(arg_value) {
                    string_to_cmtype(type_json.to_string(), obj_id.to_string()).unwrap()
                } else {
                    string_to_cmtype(type_json.to_string(), arg_value.to_string()).unwrap()
                }
            } else {
                // This should not happen
                CmTypes::None
            }
        }
    };

    let arg = Arg {
        type_: arg_cmtype,
        init_condition: condition,
        predecessor,
    };
    arg
}

fn parse_predecessor(
    pred_json: &PredJson,
    init_objects: &Vec<Vec<CmTypes>>,
    obj_id_map: &RapidHashMap<String, usize>,
    name_to_id: &RapidHashMap<String, IdType>,
) -> Predecessor {
    let pred_name = &pred_json.name;
    let pred_id = *name_to_id.get(pred_name).unwrap();

    let mut index_vec = Vec::new();
    let indexes = &pred_json.indexes;

    // 1st case: exact indexes ',' separated
    if indexes.contains(',') {
        for predecessor_index in indexes.split(",") {
            // strip to remove whitespace
            let predecessor_index = predecessor_index.trim();
            index_vec.push(predecessor_index.parse::<isize>().unwrap());
        }
    }
    // 2nd case: range indexes '-' separated
    else if indexes.contains('-') {
        let range: Vec<&str> = indexes.split("-").collect();
        let start = range[0].parse::<isize>().unwrap();
        let end = {
            match range[1].parse::<isize>() {
                Ok(end) => end + 1,
                Err(_) => {
                    // If the second part of the range is not a number, it might be a reference
                    if let Some(obj_id) = obj_id_map.get(range[1]) {
                        let ref_val = &init_objects[*obj_id];
                        ref_val[0].valid_number_to_usize().unwrap() as isize
                    } else {
                        panic!("Invalid range in predecessor: {}", indexes);
                    }
                }
            }
        };
        for i in start..end {
            index_vec.push(i);
        }
    } else {
        // single predecessor
        index_vec.push(indexes.parse::<isize>().unwrap());
    }

    Predecessor {
        id: pred_id,
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
        condition_json.value_type.to_string(),
        condition_json.value.to_string(),
    )
    .unwrap();

    InitCondition {
        operation,
        eval_value,
    }
}

pub fn from_json(graph_json: &str, workers: usize) -> Result<Graph, serde_json::Error> {
    let mut file = File::open(graph_json).unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();

    // Parse JSON file with defined structure
    let graph_parsed: GraphFile = serde_json::from_str(&contents)?;

    // Check for initializations in the graph
    let (init_vec, obj_id_map) = match init_objects(&graph_parsed.initializations, workers) {
        Ok((init_vec, obj_id_map)) => (init_vec, obj_id_map),
        Err(e) => {
            panic!("Error parsing initial objects: {}", e);
        }
    };

    // Create a new Graph
    let mut graph = Graph::new();
    let mut name_to_id: RapidHashMap<String, IdType> = RapidHashMap::new();

    for node_json in graph_parsed.nodes.iter() {
        let mut args = Vec::new();
        let mut loop_args_vec = Vec::new();

        for arg_json in &node_json.args {
            args.push(parse_arg(arg_json, &init_vec, &obj_id_map, &name_to_id));
        }

        if let Some(loop_args_json) = &node_json.loop_args {
            for arg_json in loop_args_json {
                loop_args_vec.push(parse_arg(arg_json, &init_vec, &obj_id_map, &name_to_id));
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

        let factor = match &node_json.factor {
            Some(factor) => factor.resolve(&init_vec, &obj_id_map, workers),
            None => 1,
        };

        let loop_ = {
            if let Some(loop_json) = &node_json.loop_ {
                Some(Loop {
                    name: loop_json.name.clone(),
                    factor: loop_json
                        .factor
                        .as_ref()
                        .map_or(1, |f| f.resolve(&init_vec, &obj_id_map, workers)),
                })
            } else {
                None
            }
        };

        let node_count = NodeCount.fetch_add(1, SeqCst);
        name_to_id.insert(node_json.name.clone(), node_count);

        let node = Node {
            name: node_json.name.clone(),
            args,
            id: node_count as IdType,
            loop_args,
            factor: factor,
            func_ptr,
            loop_,
            nx: false,
        };

        graph.add_node(node.clone());
    }

    for post_node_json in graph_parsed.post_nodes.unwrap_or_default().iter() {
        let mut args = Vec::new();
        for arg_json in &post_node_json.args {
            args.push(parse_arg(arg_json, &init_vec, &obj_id_map, &name_to_id));
        }

        let func_ptr = get_func(&post_node_json.function_name);

        let factor = match &post_node_json.factor {
            Some(factor) => factor.resolve(&init_vec, &obj_id_map, workers),
            None => 1,
        };

        println!("Adding post-node: {}", post_node_json.name);

        let post_node_count = PostNodeCount.fetch_add(1, SeqCst);

        let node = Node {
            name: post_node_json.name.clone(),
            args,
            id: post_node_count,
            loop_args: None,
            factor,
            func_ptr,
            loop_: None,
            nx: false,
        };

        graph.add_post_node(node);
    }

    // Parse ID function if present
    if let Some(id_function_json) = &graph_parsed.id_function {
        let func_ptr = get_func(&id_function_json.function_name);

        let predecessor = &id_function_json.predecessor;
        let pred_id = name_to_id
            .get(predecessor)
            .expect("ID function predecessor not found")
            .clone();

        let mut args = Vec::new();
        for arg_json in &id_function_json.args {
            args.push(parse_arg(arg_json, &init_vec, &obj_id_map, &name_to_id));
        }

        let id_function = IdFunction {
            func_ptr,
            predecessor: pred_id,
            args,
        };

        graph.set_id_function(&id_function);
    }

    // Set the initialized objects in the graph
    graph.set_init_objects(&init_vec);
    graph.obj_id_map = obj_id_map.clone();

    // Parse network configuration if present
    if let Some(network_config_json) = &graph_parsed.network_config {
        // Resolve factors for num_sockets and packet_length
        let num_sockets = network_config_json
            .num_sockets
            .resolve(&init_vec, &obj_id_map, workers);
        let packet_length =
            network_config_json
                .packet_length
                .resolve(&init_vec, &obj_id_map, workers);

        // Validate first_processing_node exists
        if !name_to_id.contains_key(&network_config_json.first_processing_node) {
            panic!(
                "Network config error: first_processing_node '{}' not found in graph nodes",
                network_config_json.first_processing_node
            );
        }

        // Parse socket type
        let socket_type = match network_config_json.socket_type.to_lowercase().as_str() {
            "udp" => SocketType::Udp,
            other => panic!(
                "Unsupported socket type '{}'. Only 'udp' is currently supported.",
                other
            ),
        };

        // Create NetworkConfig (sockets will be initialized in runtime)
        #[allow(deprecated)]
        let network_config = NetworkConfig {
            socket_type,
            num_sockets,
            packet_length,
            buffer_depth: network_config_json.buffer_depth,
            socket_refs: network_config_json.socket_refs.clone(),
            socket_range_ref: network_config_json.socket_range_ref.clone(),
            frame_id_offset: network_config_json.frame_id_offset,
            frame_id_length: network_config_json.frame_id_length,
            extract_packet: network_config_json.extract_packet.clone(),
            first_processing_node: network_config_json.first_processing_node.clone(),
            socket_initializer: network_config_json.socket_initializer.clone(),
        };

        println!("Network configuration parsed:");
        println!("  Socket type: {:?}", network_config.socket_type);
        println!("  Number of sockets: {}", network_config.num_sockets);
        println!("  Packet length: {} bytes", network_config.packet_length);
        println!("  Buffer depth: {} packets", network_config.buffer_depth);
        if let Some(ref refs) = network_config.socket_refs {
            println!("  Socket refs: {:?}", refs);
        } else if let Some(ref range_ref) = network_config.socket_range_ref {
            println!("  Socket range ref: {}", range_ref);
        } else if let Some(ref init) = network_config.socket_initializer {
            println!("  Socket initializer (DEPRECATED): {}", init);
        }
        if let (Some(offset), Some(length)) = (network_config.frame_id_offset, network_config.frame_id_length) {
            println!("  Frame ID: offset={}, length={} bytes", offset, length);
        }
        println!("  Extract packet function: {}", network_config.extract_packet);
        println!(
            "  First processing node: {}",
            network_config.first_processing_node
        );

        graph.network_config = Some(Arc::new(network_config));
    } else {
        println!("No network_config found - skipping network receiver setup");
    }

    Ok(graph)
}
