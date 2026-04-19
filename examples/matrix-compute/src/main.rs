use matcomp::validation;
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct GraphFile {
    pub initializations: Vec<InitJson>,
}
#[derive(Debug, Deserialize)]
pub struct InitJson {
    pub name: String,
    pub args: Vec<ArgInit>,
    pub function: Option<String>,
}
#[derive(Debug, Deserialize)]
pub struct ArgInit {
    #[serde(rename = "type")]
    pub type_: String,
    pub value: String,
}

fn parse_graph(script_dir: &str) -> (usize, usize) {
    let graph_path = PathBuf::from(script_dir).join("graph.json");
    let graph_content = fs::read_to_string(&graph_path).expect("Failed to read graph.json");
    let graph: GraphFile =
        serde_json::from_str(&graph_content).expect("Failed to parse graph.json");

    let mut buf_size = 0;
    let mut num_nodes = 0;

    for init in graph.initializations {
        if init.name == "buf_size" {
            if let Some(arg) = init.args.first() {
                buf_size = arg.value.parse().expect("Failed to parse buf_size");
            }
        } else if init.name == "num_nodes" {
            if let Some(arg) = init.args.first() {
                num_nodes = arg.value.parse().expect("Failed to parse num_nodes");
            }
        }
    }

    if buf_size == 0 || num_nodes == 0 {
        panic!("Error: Could not find buf_size or num_nodes in graph.json");
    }
    (buf_size, num_nodes)
}

pub fn main() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let (buf_size, num_nodes) = parse_graph(manifest_dir);
    validation::validate(buf_size, num_nodes, manifest_dir);
}
