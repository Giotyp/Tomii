use jraph_core::clerk::Clerk;
use jraph_core::graph_gen::from_json;
use jraph_core::scheduler::Scheduler;

fn main() {
    let graph_file = "/home/george/JRaph/jraph-core/examples/graphs/dyn.json";

    let graph = from_json(graph_file).unwrap();
    let mut clerk = Clerk::new();

    let workers = 1;
    let core_offset = 0;
    let scheduler = Scheduler::new(core_offset, workers);

    clerk.run(&graph, scheduler, Some(3));

    // Get results
    let results = clerk.get_results();
    for (node_name, result) in results {
        println!("Node: {}, Result: {:?}", node_name, result);
    }
}
