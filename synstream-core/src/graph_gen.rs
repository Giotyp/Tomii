use core::panic;
use std::fs::File;
use std::io::Read;

use crate::func_reg::*;
use crate::graph::*;
use crate::graph_struct::*;
use crate::json_structs::*;
use crate::network::SocketType;
use crate::obj_gen::init_objects;
use crate::prelude::*;
use rapidhash::{HashMapExt, RapidHashMap};
use serde_json;
use std::sync::atomic::Ordering::SeqCst;
use synstream_types::*;

fn parse_arg(
    arg_json: &ArgJson,
    init_objects: &[Vec<CmTypes>],
    obj_id_map: &RapidHashMap<String, usize>,
    name_to_id: &RapidHashMap<String, IdType>,
) -> Arg {
    let arg_value_opt = arg_json.value.as_deref();

    let condition: Option<InitCondition> = arg_json.condition.as_ref().map(parse_condition);

    let predecessor: Option<Predecessor> = arg_json
        .predecessor
        .as_ref()
        .map(|pred_json| parse_predecessor(pred_json, init_objects, obj_id_map, name_to_id));

    let arg_cmtype = {
        let type_json = &arg_json.type_;
        if let Some(ref pred) = predecessor {
            string_to_cmtype(type_json.to_string(), pred.id.to_string()).unwrap()
        } else if let Some(arg_value) = arg_value_opt {
            if let Some(obj_id) = obj_id_map.get(arg_value) {
                string_to_cmtype(type_json.to_string(), obj_id.to_string()).unwrap()
            } else {
                string_to_cmtype(type_json.to_string(), arg_value.to_string()).unwrap()
            }
        } else {
            // This should not happen
            CmTypes::None
        }
    };

    Arg {
        type_: arg_cmtype,
        init_condition: condition,
        predecessor,
    }
}

fn parse_predecessor(
    pred_json: &PredJson,
    init_objects: &[Vec<CmTypes>],
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
    let operation = CondOp::from_str(&condition_json.operation)
        .unwrap_or_else(|| panic!("Invalid operation: {}", condition_json.operation));

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

    // Set the initialized objects in the graph
    graph.set_init_objects(&init_vec);
    graph.obj_id_map = obj_id_map.clone();

    // Parse network configuration if present
    if let Some(network_config_json) = &graph_parsed.network_config {
        // Parse socket type
        let socket_type = match network_config_json.socket_type.to_lowercase().as_str() {
            "udp" => SocketType::Udp,
            other => panic!(
                "Unsupported socket type '{}'. Only 'udp' is currently supported.",
                other
            ),
        };

        // Resolve SimpleArgJson fields to concrete types
        let num_sockets = network_config_json
            .num_sockets
            .resolve(&init_vec, &obj_id_map, workers);
        let packet_length =
            network_config_json
                .packet_length
                .resolve(&init_vec, &obj_id_map, workers);
        let stream_packets =
            network_config_json
                .stream_packets
                .resolve(&init_vec, &obj_id_map, workers);
        let address: String = {
            let given_address = &network_config_json.address;
            if obj_id_map.contains_key(given_address) {
                let obj_id = obj_id_map.get(given_address).unwrap();
                init_vec[*obj_id][0]
                    .as_string()
                    .expect("Network address must be a String type in init_objects")
            } else {
                given_address.clone()
            }
        };
        let start_port = network_config_json
            .start_port
            .resolve(&init_vec, &obj_id_map, workers);

        // Resolve extract_packet_func to function pointer
        let extract_packet_func = get_func(&network_config_json.extract_packet_func);
        // Resolve id_function to function pointer
        let id_function = get_func(&network_config_json.id_function);

        let graph_network_config = GraphNetworkConfig {
            socket_type,
            num_sockets,
            packet_length,
            stream_packets,
            buffer_depth: network_config_json.buffer_depth,
            address,
            start_port,
            extract_packet_func,
            id_function,
        };

        println!("Network configuration parsed:");
        println!("  Socket type: {:?}", graph_network_config.socket_type);
        println!("  Number of sockets: {}", graph_network_config.num_sockets);
        println!("  Packet length: {}", graph_network_config.packet_length);
        println!(
            "  Buffer depth: {} packets",
            graph_network_config.buffer_depth
        );
        println!("  Address: {}", graph_network_config.address);
        println!("  Start port: {}", graph_network_config.start_port);
        println!(
            "  Extract packet function: {}",
            network_config_json.extract_packet_func
        );

        graph.set_network_config(&graph_network_config);
    } else {
        println!("No network_config found - skipping network receiver setup");
    }

    // If network_config is present in graph, we reserve id:0 for network
    if let Some(network_config) = graph.network_config().as_ref() {
        let node_count = NodeCount.fetch_add(1, SeqCst);
        name_to_id.insert("$network".to_string(), node_count);
        let net_node = Node {
            name: "$network".to_string(),
            args: Vec::new(),
            id: node_count as IdType,
            loop_args: None,
            factor: network_config.stream_packets,
            func_ptr: None,
            loop_: None,
            condition: None,
        };
        graph.add_node(net_node);
    }

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

        let loop_ = node_json.loop_.as_ref().map(|loop_json| Loop {
            name: loop_json.name.clone(),
            factor: loop_json
                .factor
                .as_ref()
                .map_or(1, |f| f.resolve(&init_vec, &obj_id_map, workers)),
        });

        // Parse node-level condition if present
        let condition = if let Some(cond_json) = &node_json.condition {
            let func_name = &cond_json.function;
            let cond_func_ptr = get_func(func_name)
                .unwrap_or_else(|| panic!("Condition function '{}' not found", func_name));
            let op_str = &cond_json.operation;
            let cond_operation = CondOp::from_str(op_str)
                .unwrap_or_else(|| panic!("Invalid condition operation: {}", op_str));

            // Parse condition value
            let cond_value =
                string_to_cmtype(cond_json.value_type.clone(), cond_json.value.clone())
                    .unwrap_or_else(|_| {
                        panic!("Failed to parse condition value: {}", cond_json.value)
                    });

            // Parse condition args
            let mut cond_args = Vec::new();
            for arg_json in &cond_json.args {
                cond_args.push(parse_arg(arg_json, &init_vec, &obj_id_map, &name_to_id));
            }

            Some(NodeCondition {
                operation: cond_operation,
                eval_value: cond_value,
                func_ptr: cond_func_ptr,
                args: cond_args,
            })
        } else {
            None
        };

        let node_count = NodeCount.fetch_add(1, SeqCst);
        name_to_id.insert(node_json.name.clone(), node_count);

        let node = Node {
            name: node_json.name.clone(),
            args,
            id: node_count as IdType,
            loop_args,
            factor,
            func_ptr,
            loop_,
            condition,
        };

        graph.add_node(node);
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
            condition: None,
        };

        graph.add_post_node(node);
    }

    Ok(graph)
}
