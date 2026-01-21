use rapidhash::RapidHashMap;
use serde::Deserialize;
use synstream_types::*;

// Graph File Structure
#[derive(Debug, Deserialize)]
pub struct GraphFile {
    pub initializations: Vec<InitJson>,
    pub nodes: Vec<NodeJson>,
    pub post_nodes: Option<Vec<NodeJson>>,
    pub id_function: Option<IdFunctionJson>,
    pub network_config: Option<NetworkConfigJson>,
}

// Network Configuration
#[derive(Debug, Deserialize)]
pub struct NetworkConfigJson {
    pub socket_type: String,
    pub num_sockets: Factor,
    pub packet_length: Factor,
    #[serde(default = "default_buffer_depth")]
    pub buffer_depth: usize,

    // NEW: Socket reference methods (mutually exclusive)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub socket_refs: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub socket_range_ref: Option<String>,

    // NEW: Fixed-offset frame ID extraction
    pub frame_id_offset: Option<usize>,
    pub frame_id_length: Option<usize>,

    // NEW: User-defined function to parse raw packet bytes into structured data
    pub extract_packet: String,

    pub first_processing_node: String,

    // DEPRECATED: Backward compatibility with legacy user functions
    #[serde(skip_serializing_if = "Option::is_none")]
    #[deprecated(note = "Use socket_refs or socket_range_ref instead")]
    pub socket_initializer: Option<String>,
}

fn default_buffer_depth() -> usize {
    128
}

// Node structures
#[derive(Debug, Deserialize)]
pub struct ConditionJson {
    pub operation: String,
    pub value: String,
    pub value_type: String,
}

#[derive(Debug, Deserialize)]
pub struct PredJson {
    pub name: String,
    pub indexes: String,
}

#[derive(Debug, Deserialize)]
pub struct ArgJson {
    #[serde(rename = "type")]
    pub type_: String,
    pub value: Option<String>,
    pub condition: Option<ConditionJson>,
    pub predecessor: Option<PredJson>,
}

#[derive(Debug, Deserialize)]
pub struct LoopJson {
    pub name: String,
    pub factor: Option<Factor>,
}

#[derive(Debug, Deserialize)]
pub struct NodeJson {
    pub name: String,
    pub factor: Option<Factor>,
    pub function_name: String,
    #[serde(rename = "loop")]
    pub loop_: Option<LoopJson>,
    pub loop_args: Option<Vec<ArgJson>>,
    pub args: Vec<ArgJson>,
}

// Initializations
#[derive(Debug, Deserialize)]
pub struct ArgInit {
    #[serde(rename = "type")]
    pub type_: String,
    pub value: String,
}

#[derive(Debug, Deserialize)]
pub struct InitJson {
    pub name: String,
    pub factor: Option<Factor>,
    pub args: Vec<ArgInit>,
    pub function_name: Option<String>,
}

// ID Function for streaming support
#[derive(Debug, Deserialize)]
pub struct IdFunctionJson {
    pub function_name: String,
    pub predecessor: String,
    pub args: Vec<ArgJson>,
}

// Factor struct
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Factor {
    Number(usize),
    Ref(String),
}

impl Factor {
    pub fn resolve(
        &self,
        init_objects: &Vec<Vec<CmTypes>>,
        obj_id_map: &RapidHashMap<String, usize>,
        workers: usize,
    ) -> usize {
        match self {
            Factor::Number(num) => *num,
            Factor::Ref(ref_name) => {
                if let Some(obj_id) = obj_id_map.get(ref_name) {
                    let ref_val = &init_objects[*obj_id];
                    let usize_res = ref_val[0].valid_number_to_usize();
                    if let Some(usize_val) = usize_res {
                        return usize_val;
                    } else {
                        panic!(
                            "Variable '{}' found but does not contain a valid number",
                            ref_name
                        );
                    }
                }

                // Check if ref_name is $workers
                if ref_name == "$workers" {
                    return workers;
                }
                panic!(
                    "Variable '{}' not found or does not contain a number",
                    ref_name
                );
            }
        }
    }

    pub fn search(
        &self,
        init_objects: &Vec<Vec<CmTypes>>,
        obj_id_map: &RapidHashMap<String, usize>,
        workers: usize,
    ) -> usize {
        match self {
            Factor::Number(num) => *num,
            Factor::Ref(ref_name) => {
                // Check if ref_name is $workers
                if ref_name == "$workers" {
                    return workers;
                }

                let obj_id = obj_id_map.get(ref_name).unwrap();

                if obj_id > &init_objects.len() {
                    panic!(
                        "Variable '{}' not found or does not contain a number",
                        ref_name
                    );
                }

                let usize_res = &init_objects[*obj_id][0].valid_number_to_usize();
                if let Some(usize_val) = usize_res {
                    return *usize_val;
                } else {
                    panic!(
                        "Variable '{}' found but does not contain a valid number",
                        ref_name
                    );
                }
            }
        }
    }
}
