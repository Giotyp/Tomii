mod bindings;
mod functions;
mod validation;

use std::sync::{Arc, Mutex};
use std::time::Instant;

use jraph_core::executor::Executor;
use jraph_core::graph_gen::from_json;
use jraph_core::time_buffer::TimeBuffer;
use nalgebra::DMatrix;
use num_complex::Complex32;
use shared::CmTypes;

use crate::validation::*;

fn main() {
    let graph1_file = "/home/george/JRaph/jraph-core/examples/graphs/bench1.json";
    let graph2_file = "/home/george/JRaph/jraph-core/examples/graphs/bench2.json";
    let graph3_file = "/home/george/JRaph/jraph-core/examples/graphs/bench3.json";

    let graphs = vec![graph1_file, graph2_file, graph3_file];

    let repeat = 100;

    let core_offset = 12;
    let workers = 10;
    println!("Using {} workers", workers);

    for i in 0..graphs.len() {
        let graph = from_json(graphs[i]).unwrap();
        let mult_factor = graph.stage(0).node("FFT").mult_factor();

        let mut results: Vec<CmTypes> = Vec::new();
        let mut results_mat: Vec<DMatrix<Complex32>> = Vec::new();
        let executor = Executor::new(core_offset, workers);

        let mut timebuf = TimeBuffer::new(workers, repeat);
        timebuf.init_task("FFT-Comp");
        timebuf.init_task("CmRetrieve");
        timebuf.init_task("VecMat-Comp");
        timebuf.init_task("CGEMM-Comp");
        timebuf.init_task("Stage2-Clone");

        let arc_timebuf = Arc::new(Mutex::new(timebuf));
        for run_idx in 0..repeat {
            results.clear();
            results_mat.clear();
            let duration = executor.execute(&graph, &mut results, arc_timebuf.clone(), run_idx);

            let val = {
                match i {
                    0 => validate1(mult_factor),
                    1 => validate2(mult_factor),
                    2 => validate3(mult_factor),
                    _ => Vec::new(),
                }
            };

            // retrieve buffers
            let t1_ret = Instant::now();
            for i in 0..mult_factor {
                let res: Arc<DMatrix<Complex32>> = {
                    match &results[i] {
                        CmTypes::DMatrixC32(x) => x.clone(),
                        _ => panic!("Invalid result type"),
                    }
                };
                let res_ref: &DMatrix<Complex32> = &res;
                results_mat.push(res_ref.clone());
            }
            let t2_ret = Instant::now();
            let mut tb = arc_timebuf.lock().unwrap();
            tb.add_time("CmRetrieve", run_idx, 0, t2_ret - t1_ret);

            for i in 0..mult_factor {
                let res = &results_mat[i];
                let valid = &val[i];
                assert_eq!(res, valid);
            }

            tb.add_total_time(run_idx, duration);
            drop(tb);
        }
        let tb = arc_timebuf.lock().unwrap();
        let bench = &format!("Bench-{}", i + 1);
        tb.print_stats(bench, None);
    }
}
