mod bindings;
mod functions;
mod validation;

use jraph_core::executor::Executor;
use jraph_core::graph_gen::from_json;
use nalgebra::DMatrix;
use num_complex::Complex32;
use shared::CmTypes;

use crate::validation::*;
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

        let val = {
            match i {
                0 => validate1(mult_factor),
                1 => validate2(mult_factor),
                2 => validate3(mult_factor),
                _ => Vec::new(),
            }
        };

        for i in 0..mult_factor {
            let valid: &DMatrix<Complex32> = &val[i];
            let res = {
                match &results[i] {
                    CmTypes::DMatrixC32(x) => x,
                    _ => panic!("Invalid result type"),
                }
            };
            let res_ref: &DMatrix<Complex32> = &res;
            assert_eq!(valid, res_ref);
        }

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
