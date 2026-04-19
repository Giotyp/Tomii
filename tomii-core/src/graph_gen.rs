use crate::IdType;
use std::fs::File;
use std::io::Read;

use crate::debug::print_debug;
use crate::func_reg::*;
use crate::graph::*;
use crate::graph_struct::*;
use crate::json_structs::*;
use crate::network::SocketType;
use crate::obj_gen::init_objects;
use rapidhash::{HashMapExt, RapidHashMap};
use serde_json;
use tomii_types::*;

fn parse_arg(
    arg_json: &ArgJson,
    init_objects: &[Vec<CmTypes>],
    obj_id_map: &RapidHashMap<String, usize>,
    name_to_id: &RapidHashMap<String, IdType>,
    workers: usize,
) -> Result<Arg, crate::TomiiError> {
    let arg_value_opt = arg_json.value.as_deref();

    let condition: Option<InitCondition> = arg_json
        .condition
        .as_ref()
        .map(parse_condition)
        .transpose()?;

    let predecessor: Option<Predecessor> = arg_json
        .predecessor
        .as_ref()
        .map(|pred_json| {
            parse_predecessor(pred_json, init_objects, obj_id_map, name_to_id, workers)
        })
        .transpose()?;

    let arg_cmtype = {
        let type_json = &arg_json.type_;
        if let Some(ref pred) = predecessor {
            string_to_cmtype(type_json.to_string(), pred.id.to_string())?
        } else if let Some(arg_value) = arg_value_opt {
            if let Some(obj_id) = obj_id_map.get(arg_value) {
                string_to_cmtype(type_json.to_string(), obj_id.to_string())?
            } else {
                string_to_cmtype(type_json.to_string(), arg_value.to_string())?
            }
        } else {
            CmTypes::None
        }
    };

    Ok(Arg {
        type_: arg_cmtype,
        init_condition: condition,
        predecessor,
    })
}

fn parse_predecessor(
    pred_json: &PredJson,
    init_objects: &[Vec<CmTypes>],
    obj_id_map: &RapidHashMap<String, usize>,
    name_to_id: &RapidHashMap<String, IdType>,
    workers: usize,
) -> Result<Predecessor, crate::TomiiError> {
    let pred_name = &pred_json.name;
    let pred_id = *name_to_id
        .get(pred_name)
        .ok_or_else(|| -> crate::TomiiError {
            format!("Unknown predecessor node '{}'", pred_name).into()
        })?;

    let mut index_vec = Vec::new();
    let indexes = &pred_json.indexes;

    // 1st case: exact indexes ',' separated
    if indexes.contains(',') {
        for predecessor_index in indexes.split(",") {
            let predecessor_index = predecessor_index.trim();
            let val = predecessor_index
                .parse::<isize>()
                .map_err(|_| -> crate::TomiiError {
                    format!(
                        "Invalid index '{}' in predecessor '{}'",
                        predecessor_index, pred_name
                    )
                    .into()
                })?;
            index_vec.push(val);
        }
    }
    // 2nd case: range indexes '-' separated
    else if indexes.contains('-') {
        let range: Vec<&str> = indexes.split("-").collect();
        let start = match range[0].parse::<isize>() {
            Ok(val) => val,
            Err(_) => {
                let obj_id = obj_id_map
                    .get(range[0])
                    .ok_or_else(|| -> crate::TomiiError {
                        format!(
                            "Invalid range start '{}' in predecessor '{}'",
                            range[0], pred_name
                        )
                        .into()
                    })?;
                let ref_val = &init_objects[*obj_id];
                ref_val[0]
                    .valid_number_to_usize()
                    .ok_or_else(|| -> crate::TomiiError {
                        format!(
                            "Range start '{}' is not a valid non-negative integer",
                            range[0]
                        )
                        .into()
                    })? as isize
            }
        };
        let end = match range[1].parse::<isize>() {
            Ok(end) => end + 1,
            Err(_) => {
                let obj_id = obj_id_map
                    .get(range[1])
                    .ok_or_else(|| -> crate::TomiiError {
                        format!(
                            "Invalid range end '{}' in predecessor '{}'",
                            range[1], pred_name
                        )
                        .into()
                    })?;
                let ref_val = &init_objects[*obj_id];
                ref_val[0]
                    .valid_number_to_usize()
                    .ok_or_else(|| -> crate::TomiiError {
                        format!(
                            "Range end '{}' is not a valid non-negative integer",
                            range[1]
                        )
                        .into()
                    })? as isize
            }
        };
        for i in start..end {
            index_vec.push(i);
        }
    } else {
        // single predecessor - try literal integer, then reference
        match indexes.parse::<isize>() {
            Ok(val) => index_vec.push(val),
            Err(_) => {
                let obj_id =
                    obj_id_map
                        .get(indexes.as_str())
                        .ok_or_else(|| -> crate::TomiiError {
                            format!("Invalid index '{}' in predecessor '{}'", indexes, pred_name)
                                .into()
                        })?;
                let ref_val = &init_objects[*obj_id];
                let val =
                    ref_val[0]
                        .valid_number_to_usize()
                        .ok_or_else(|| -> crate::TomiiError {
                            format!(
                                "Index ref '{}' is not a valid non-negative integer",
                                indexes
                            )
                            .into()
                        })? as isize;
                index_vec.push(val);
            }
        }
    }

    // Resolve group_by if present
    let group_by = pred_json
        .group_by
        .as_ref()
        .map(|gb| gb.resolve(init_objects, obj_id_map, workers));

    Ok(Predecessor {
        id: pred_id,
        indexes: index_vec,
        group_by,
    })
}

fn parse_condition(condition_json: &ConditionJson) -> Result<InitCondition, crate::TomiiError> {
    let operation =
        CondOp::parse(&condition_json.operation).ok_or_else(|| -> crate::TomiiError {
            format!("Invalid condition operation '{}'", condition_json.operation).into()
        })?;

    let eval_value = string_to_cmtype(
        condition_json.value_type.to_string(),
        condition_json.value.to_string(),
    )?;

    Ok(InitCondition {
        operation,
        eval_value,
    })
}

/// The result of parsing a graph JSON file.
///
/// `init_objects` is separated from `Graph` because it contains live Rust values
/// (`CmTypes::Any` wrapping heap-allocated objects) that should not be part of the
/// pure-description `Graph` type.
///
/// Call [`GraphSpec::compile`] to produce a [`GraphCompiled`] IR, then pass that to
/// [`crate::runtime::TomiiRtBuilder::new`].
pub struct GraphSpec {
    pub graph: Graph,
    /// Materialized initialization objects, indexed by `$ref` IDs embedded in `Arg` values.
    pub init_objects: Vec<Vec<CmTypes>>,
}

impl GraphSpec {
    /// Compile the parsed graph into a fully precomputed [`GraphCompiled`] IR.
    ///
    /// This resolves function pointers, pre-builds the node cache, predecessor routing
    /// tables, and dependency counts.  The result is immutable and can be passed directly
    /// to [`crate::runtime::TomiiRtBuilder::new`].
    ///
    /// Graph transformation passes (fusion, pruning, partitioning, etc.) should operate
    /// on `self.graph` *before* calling `compile` — the compilation step rebuilds all
    /// derived tables from the final topology.
    pub fn compile(self, scheduler: &crate::scheduler::SchedulerImpl) -> GraphCompiled {
        let node_cache =
            crate::runtime::build_node_cache(&self.graph, &self.init_objects, scheduler);
        let (pred_index_filter, pred_group_by, pred_succ_1to1_offset) =
            crate::runtime::build_predecessor_tables(&self.graph);

        let total_tasks: usize = node_cache
            .iter()
            .filter(|nc| !nc.is_initial && !nc.is_condition)
            .map(|nc| nc.factor)
            .sum();
        let total_cond_tasks: usize = node_cache
            .iter()
            .filter(|nc| nc.is_condition)
            .map(|nc| nc.factor)
            .sum();
        let dependency_count_vec = self.graph.dependency_count_vec();
        let max_factor = node_cache.iter().map(|n| n.factor).max().unwrap_or(1);
        let num_nodes = self.graph.nodes.len();

        GraphCompiled {
            graph: self.graph,
            node_cache,
            pred_index_filter,
            pred_group_by,
            pred_succ_1to1_offset,
            total_tasks,
            total_cond_tasks,
            init_objects: self.init_objects,
            dependency_count_vec,
            max_factor,
            num_nodes,
        }
    }
}

/// Fully compiled graph IR — all precomputed tables ready for runtime construction.
///
/// Produced by [`GraphSpec::compile`]. Consumed by [`crate::runtime::TomiiRtBuilder::new`].
///
/// This is the "graph IR" referred to in the Τομί architecture: the output of the
/// graph compiler, distinct from the parsed topology (`GraphSpec`) and from the mutable
/// runtime state (`SharedData`).  It is immutable after construction.
pub struct GraphCompiled {
    /// Original graph topology — still accessed at runtime by hot-path functions.
    pub graph: Graph,
    /// Per-node precomputed cache (function pointers, arg caches, flags, priorities).
    pub node_cache: Vec<crate::runtime::NodeCacheEntry>,
    /// Per-(succ, pred) index range filter: which predecessor instances drive a successor.
    pub pred_index_filter: Vec<Vec<Option<(usize, usize)>>>,
    /// Per-(succ, pred) group_by divisor for grouped barriers.
    pub pred_group_by: Vec<Vec<Option<usize>>>,
    /// Per-(succ, pred) offset for 1:1 equal-factor non-barrier `$res` edges.
    pub pred_succ_1to1_offset: Vec<Vec<Option<isize>>>,
    /// Sum of factors for all non-initial, non-condition nodes.
    pub total_tasks: usize,
    /// Sum of factors for all condition nodes.
    pub total_cond_tasks: usize,
    /// Materialized initialization objects, indexed by `$ref` IDs in `Arg` values.
    pub init_objects: Vec<Vec<CmTypes>>,
    /// Per-node total dependency counts; used to initialize the resolution state.
    pub dependency_count_vec: Vec<usize>,
    /// Maximum factor across all nodes; used to size the resolution state.
    pub max_factor: usize,
    /// Number of nodes; used to size the resolution state.
    pub num_nodes: usize,
}

pub fn from_json(graph_json: &str, workers: usize) -> Result<GraphSpec, crate::TomiiError> {
    let mut file = File::open(graph_json).map_err(|e| -> crate::TomiiError {
        format!("Cannot open graph file '{}': {}", graph_json, e).into()
    })?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    from_json_str(&contents, workers)
}

/// Parse a graph directly from a JSON string (no file I/O).
///
/// Useful for testing and for embedders that supply graph definitions at runtime.
pub fn from_json_str(contents: &str, workers: usize) -> Result<GraphSpec, crate::TomiiError> {
    let graph_parsed: GraphFile = serde_json::from_str(contents)?;

    let (init_vec, obj_id_map) = init_objects(&graph_parsed.initializations, workers)?;

    let mut graph = Graph::new();
    let mut name_to_id: RapidHashMap<String, IdType> = RapidHashMap::new();

    if let Some(nc_json) = &graph_parsed.network_config {
        let nc = parse_network_config(nc_json, &init_vec, &obj_id_map, &name_to_id, workers)?;
        graph.set_network_config(&nc);
    } else {
        tracing::debug!("no network_config found, skipping network receiver setup");
    }

    let mut node_counter: IdType = 0;

    // Reserve id:0 for the virtual $network node when a network config is present.
    #[cfg(feature = "network")]
    if let Some(network_config) = graph.network_config().as_ref() {
        let node_id = node_counter;
        node_counter += 1;
        name_to_id.insert("$network".to_string(), node_id);
        graph.add_node(Node {
            name: "$network".to_string(),
            args: Vec::new(),
            id: node_id,
            loop_args: None,
            factor: network_config.stream_packets,
            group_size: None,
            func_name: String::new(), // virtual node — no function
            loop_: None,
            condition: None,
            priority: NodePriority::default(),
            use_workers: None,
        });
    }

    for node_json in graph_parsed.nodes.iter() {
        let node = parse_single_node(node_json, &init_vec, &obj_id_map, &name_to_id, workers)?;
        let node_id = node_counter;
        node_counter += 1;
        name_to_id.insert(node_json.name.clone(), node_id);
        graph.add_node(Node {
            id: node_id,
            ..node
        });
    }

    for node in parse_post_nodes(
        &graph_parsed.post_nodes.unwrap_or_default(),
        &init_vec,
        &obj_id_map,
        &name_to_id,
        workers,
    )? {
        graph.add_post_node(node);
    }

    Ok(GraphSpec {
        graph,
        init_objects: init_vec,
    })
}

/// Parse the `network_config` JSON block into a [`GraphNetworkConfig`].
fn parse_network_config(
    nc_json: &NetworkConfigJson,
    init_vec: &[Vec<tomii_types::CmTypes>],
    obj_id_map: &RapidHashMap<String, usize>,
    name_to_id: &RapidHashMap<String, IdType>,
    workers: usize,
) -> Result<GraphNetworkConfig, crate::TomiiError> {
    let socket_type = match nc_json.socket_type.to_lowercase().as_str() {
        "udp" => SocketType::Udp,
        other => {
            return Err(format!(
                "Unsupported socket type '{}'. Only 'udp' is currently supported.",
                other
            )
            .into())
        }
    };

    let num_sockets = nc_json.num_sockets.resolve(init_vec, obj_id_map, workers);
    let packet_length = nc_json.packet_length.resolve(init_vec, obj_id_map, workers);
    let stream_packets = nc_json
        .stream_packets
        .resolve(init_vec, obj_id_map, workers);
    let address: String = {
        let given = &nc_json.address;
        if let Some(&obj_id) = obj_id_map.get(given) {
            init_vec[obj_id][0]
                .as_string()
                .ok_or_else(|| -> crate::TomiiError {
                    "Network address must be a String type in init_objects".into()
                })?
        } else {
            given.clone()
        }
    };
    let start_port = nc_json.start_port.resolve(init_vec, obj_id_map, workers);
    let extract_packet_func = get_func(&nc_json.extract_packet_func);
    let id_function = get_func(&nc_json.id_function);
    let index_function = {
        let func_ptr = get_func(&nc_json.index_function.function);
        let args = nc_json
            .index_function
            .args
            .iter()
            .map(|a| parse_arg(a, init_vec, obj_id_map, name_to_id, workers))
            .collect::<Result<_, _>>()?;
        Some(IndexFunction { func_ptr, args })
    };

    let nc = GraphNetworkConfig {
        socket_type,
        num_sockets,
        packet_length,
        stream_packets,
        buffer_depth: nc_json.buffer_depth,
        address,
        start_port,
        extract_packet_func,
        id_function,
        index_function,
    };

    tracing::info!(
        socket_type = ?nc.socket_type,
        num_sockets = nc.num_sockets,
        packet_length = nc.packet_length,
        buffer_depth = nc.buffer_depth,
        address = %nc.address,
        start_port = nc.start_port,
        extract_packet_func = %nc_json.extract_packet_func,
        "network configuration parsed"
    );

    Ok(nc)
}

/// Parse a single node JSON entry into a [`Node`] with `id` set to 0.
/// The caller is responsible for assigning the real ID and registering the name.
fn parse_single_node(
    node_json: &NodeJson,
    init_vec: &[Vec<tomii_types::CmTypes>],
    obj_id_map: &RapidHashMap<String, usize>,
    name_to_id: &RapidHashMap<String, IdType>,
    workers: usize,
) -> Result<Node, crate::TomiiError> {
    let args: Vec<Arg> = node_json
        .args
        .iter()
        .map(|a| parse_arg(a, init_vec, obj_id_map, name_to_id, workers))
        .collect::<Result<_, _>>()?;

    let loop_args: Option<Vec<Arg>> = node_json
        .loop_args
        .as_ref()
        .map(|la| {
            la.iter()
                .map(|a| parse_arg(a, init_vec, obj_id_map, name_to_id, workers))
                .collect::<Result<_, _>>()
        })
        .transpose()?;

    let factor = node_json
        .factor
        .as_ref()
        .map_or(1, |f| f.resolve(init_vec, obj_id_map, workers));
    let group_size = node_json
        .group_size
        .as_ref()
        .map(|gs| gs.resolve(init_vec, obj_id_map, workers));
    let loop_ = node_json.loop_.as_ref().map(|lj| Loop {
        name: lj.name.clone(),
        factor: lj
            .factor
            .as_ref()
            .map_or(1, |f| f.resolve(init_vec, obj_id_map, workers)),
    });

    let condition = node_json
        .condition
        .as_ref()
        .map(|cond_json| -> Result<NodeCondition, crate::TomiiError> {
            let cond_operation =
                CondOp::parse(&cond_json.operation).ok_or_else(|| -> crate::TomiiError {
                    format!("Invalid condition operation '{}'", cond_json.operation).into()
                })?;
            let cond_value =
                string_to_cmtype(cond_json.value_type.clone(), cond_json.value.clone()).map_err(
                    |e| -> crate::TomiiError {
                        format!(
                            "Failed to parse condition value '{}': {}",
                            cond_json.value, e
                        )
                        .into()
                    },
                )?;
            let cond_args: Vec<Arg> = cond_json
                .args
                .iter()
                .map(|a| parse_arg(a, init_vec, obj_id_map, name_to_id, workers))
                .collect::<Result<_, _>>()?;
            Ok(NodeCondition {
                operation: cond_operation,
                eval_value: cond_value,
                func_name: cond_json.function.clone(),
                args: cond_args,
            })
        })
        .transpose()?;

    let priority = node_json
        .priority
        .as_deref()
        .map(NodePriority::parse)
        .unwrap_or_default();
    let use_workers = node_json
        .use_workers
        .as_ref()
        .map(|spec_str| {
            crate::WorkerRangeSpec::parse(spec_str).map_err(|e| -> crate::TomiiError {
                format!("Invalid use_workers spec '{}': {}", spec_str, e).into()
            })
        })
        .transpose()?;

    Ok(Node {
        name: node_json.name.clone(),
        args,
        id: 0, // caller assigns real ID
        loop_args,
        factor,
        group_size,
        func_name: node_json.function.clone(),
        loop_,
        condition,
        priority,
        use_workers,
    })
}

/// Parse all post-node JSON entries into [`Node`] values with assigned IDs.
fn parse_post_nodes(
    post_nodes_json: &[NodeJson],
    init_vec: &[Vec<tomii_types::CmTypes>],
    obj_id_map: &RapidHashMap<String, usize>,
    name_to_id: &RapidHashMap<String, IdType>,
    workers: usize,
) -> Result<Vec<Node>, crate::TomiiError> {
    post_nodes_json
        .iter()
        .enumerate()
        .map(|(i, pn_json)| {
            print_debug(|| format!("Adding post-node: {}", pn_json.name));
            let mut node = parse_single_node(pn_json, init_vec, obj_id_map, name_to_id, workers)?;
            node.id = i as IdType;
            Ok(node)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_json_missing_file_returns_err() {
        let result = from_json("/nonexistent/path/graph.json", 1);
        match result {
            Err(e) => assert!(
                e.to_string().contains("Cannot open graph file"),
                "unexpected error: {e}"
            ),
            Ok(_) => ::core::panic!("expected Err, got Ok"),
        }
    }

    #[test]
    fn from_json_invalid_json_returns_err() {
        let path = "/tmp/tomii_test_invalid.json";
        std::fs::write(path, "not valid json {{").unwrap();
        let result = from_json(path, 1);
        std::fs::remove_file(path).ok();
        assert!(result.is_err(), "expected Err for invalid JSON");
    }
}
