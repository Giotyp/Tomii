mod bindings;
mod functions;
mod validation;

use std::sync::{Arc, Mutex};

use jraph_core::executor::Executor;
use jraph_core::graph_gen::from_json;
use jraph_core::time_buffer::TimeBuffer;
use jraph_core::utils_rdtsc::rdtsc;
use nalgebra::DMatrix;
use num_complex::Complex32;
use shared::CmTypes;

use crate::validation::*;

fn main() {
    let graph1_file = "/home/george/JRaph/jraph-core/examples/graphs/bench1.json";
    let graph2_file = "/home/george/JRaph/jraph-core/examples/graphs/bench2.json";
    let graph3_file = "/home/george/JRaph/jraph-core/examples/graphs/bench3.json";

    let graphs = vec![graph1_file, graph2_file, graph3_file];

    let repeat = 50;

    for i in 0..graphs.len() {
        let graph = from_json(graphs[i]).unwrap();
        let mult_factor = graph.stage(0).node("FFT").mult_factor();

        let mut results: Vec<CmTypes> = Vec::new();
        let mut results_mat: Vec<DMatrix<Complex32>> = Vec::new();
        let executor = Executor::new(2, 1);

        let mut timebuf = TimeBuffer::new();
        timebuf.init_task("Total", repeat);
        timebuf.init_task("FFT-Comp", repeat);
        timebuf.init_task("CmRetrieve", repeat);
        timebuf.init_task("VecMat-Comp", repeat);
        timebuf.init_task("CGEMM-Comp", repeat);

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
            let t1_ret = rdtsc();
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
            let t2_ret = rdtsc();
            let mut tb = arc_timebuf.lock().unwrap();
            tb.add_time("CmRetrieve", run_idx, t2_ret - t1_ret);
            drop(tb);

            for i in 0..mult_factor {
                let res = &results_mat[i];
                let valid = &val[i];
                assert_eq!(res, valid);
            }

            let mut tb = arc_timebuf.lock().unwrap();
            tb.add_time("Total", run_idx, duration);
            drop(tb);
        }
        let tb = arc_timebuf.lock().unwrap();
        let avg_total = tb.task_average("Total", "ms");
        let avg_fft = tb.task_average("FFT-Comp", "ms");
        let avg_vecmat_comp = tb.task_average("VecMat-Comp", "ms");
        let avg_cgemm_comp = tb.task_average("CGEMM-Comp", "ms");
        let avg_results = tb.task_average("CmRetrieve", "ms");
        drop(tb);
        println!(
            "Bench {} Average Total Time({}) for {} tasks: {:.4?} ms",
            i + 1,
            repeat,
            mult_factor,
            avg_total
        );
        println!(
            "FFT-Comp: {:.4?} ms, VecMat-Comp: {:.4?} ms, CGEMM-Comp: {:.4?} ms, CmRetrieve: {:.4?} ms",
            avg_fft,  avg_vecmat_comp, avg_cgemm_comp, avg_results
        );
    }
}
