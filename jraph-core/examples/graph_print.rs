use jraph_core::graph_gen::from_json;

fn main() {
    let graph_file = "/home/george/JRaph/jraph-core/examples/graphs/graph.json";

    let graph = from_json(graph_file).unwrap();
    graph.print_graph();
}
