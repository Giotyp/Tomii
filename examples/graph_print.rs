use jraph::graph_gen::from_json;

fn main() {
    let graph_file = "examples/graphs/graph.json";

    let graph = from_json(graph_file).unwrap();
    graph.print_graph();
}
