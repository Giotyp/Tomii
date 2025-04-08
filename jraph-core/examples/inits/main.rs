use jraph_core::obj_gen::init_objects;

fn main() {
    let graph_file = "/home/george/JRaph/jraph-core/examples/graphs/graph.json";

    let obj_map = init_objects(graph_file).unwrap();
    println!("Object map: {:?}", obj_map);
}