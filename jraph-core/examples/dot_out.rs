use jraph_core::graph_gen::from_json;

fn main() {
    let graph_file = "examples/graphs/graph.json";

    let graph = from_json(graph_file).unwrap();
    let dot_out = graph.generate_dot();
    println!("{}", dot_out);
}
