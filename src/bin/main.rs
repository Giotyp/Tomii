use jraph::graph_gen::from_json;

fn main() {
    let graph_file = "src/graph.json";

    let graph = from_json(graph_file).unwrap();
    println!("{}", graph.generate_dot());
}
