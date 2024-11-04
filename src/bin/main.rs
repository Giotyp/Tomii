use jraph::graph_gen::from_json;
use cst_macros::import_and_call;

fn main() {
    let graph_file = "src/graph.json";

    let graph = from_json(graph_file).unwrap();
    // println!("{}", graph.generate_dot());
    graph.print_graph();

    // import_and_call!("src/funcs.rs", "dummy");
}
