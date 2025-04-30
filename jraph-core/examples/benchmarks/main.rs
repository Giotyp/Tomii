mod bindings;
mod functions;
mod validation;

use std::sync::{Arc, Mutex};

use jraph_core::cmtypes::CmTypes;
use jraph_core::graph_gen::{from_json, re_init_objects};
use jraph_core::scheduler::Scheduler;
use jraph_core::time_buffer::TimeBuffer;
use nalgebra::DMatrix;
use num_complex::Complex32;

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

    for i in 2..graphs.len() {
        let mut graph = from_json(graphs[i]).unwrap();

        let mult_factor = graph.stage(0).node("FFT").mult_factor();
        let fft_size_cm = graph.init_objects().unwrap().get("fft_size").unwrap();
        let fft_size = match &fft_size_cm[0] {
            CmTypes::Usize(size) => *size,
            _ => panic!("Invalid argument type"),
        };

        let mut scheduler = Scheduler::new(core_offset, workers);

        let mut timebuf = TimeBuffer::new(workers, repeat);

        // Initiate timers for each stage-node
        for stage_no in 0..graph.len() {
            let stage = graph.stage(stage_no);
            for node_name in stage.node_names() {
                let time_key = format!("{}-{}", stage_no, node_name);
                timebuf.init_task(&time_key);
            }
        }

        let arc_timebuf = Arc::new(Mutex::new(timebuf));

        let val = {
            match i {
                0 => validate1(mult_factor, fft_size),
                1 => validate2(mult_factor, fft_size),
                2 => validate3(mult_factor, fft_size),
                _ => Vec::new(),
            }
        };

        for run_idx in 0..repeat {
            let duration = scheduler.schedule(&graph, arc_timebuf.clone(), run_idx);

            let results = scheduler.get_stage_results(graph.len() - 1)["Cgemm"].clone();

            for (i, cm) in results.iter().enumerate() {
                match cm {
                    CmTypes::DMatrixC32(arc_mat) => {
                        let expected: &DMatrix<Complex32> = &val[i];
                        assert_eq!(arc_mat.as_ref(), expected);
                    }
                    other => panic!("Invalid variant at slot {}: {:?}", i, other),
                }
            }

            let mut tb = arc_timebuf.lock().unwrap();
            tb.add_total_time(run_idx, duration);
            drop(tb);

            // Reinitialize objects for next run
            re_init_objects(&mut graph, graphs[i]);
        }
        let timebuf = arc_timebuf.lock().unwrap();
        let bench = &format!("Bench-{}", i + 1);
        timebuf.print_stats(bench, None);

        let times_name = format!(
            "examples/benchmarks/results/worker_raw_JRaph_Sleep_{}.txt",
            bench
        );
        timebuf.export_worker_times(bench, &times_name);
    }
}
