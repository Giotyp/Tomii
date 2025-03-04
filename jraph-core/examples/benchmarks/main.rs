use jraph_core::executor::Executor;
use jraph_core::graph_gen::from_json;
use shared::CmTypes;

use jraph_core::utils_rdtsc::*;

fn main() {
    let graph1_file = "/home/george/JRaph/jraph-core/examples/graphs/bench1.json";
    let graph2_file = "/home/george/JRaph/jraph-core/examples/graphs/bench2.json";
    let graph3_file = "/home/george/JRaph/jraph-core/examples/graphs/bench3.json";

    let graphs = vec![graph1_file, graph2_file, graph3_file];

    for i in 0..graphs.len() {
        let graph = from_json(graphs[i]).unwrap();
        let mult_factor = graph.stage(0).node("FFT").mult_factor();

        let mut results: Vec<CmTypes> = Vec::new();
        let executor = Executor::new(0, 4);
        let duration = executor.execute(&graph, &mut results);

        //println!("Results Len: {:?}", results.len());
        for res in results.iter() {
            let len = match res {
                CmTypes::DMatrixC32(x) => x.data.len(),
                _ => 0,
            };
            if len != 10000 {
                println!("Invalid result length");
            }
        }

        let time = cycles_to_ms(duration);
        println!(
            "Bench {} Execution Time for {} tasks: {:.4?} ms",
            i + 1,
            mult_factor,
            time
        );
    }
}
