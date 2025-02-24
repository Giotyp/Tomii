mod bindings;

use jraph_core::executor::Executor;
use jraph_core::func_reg::*;
use jraph_core::graph_gen::from_json;
use shared::CmTypes;

use bindings::*;
use jraph_core::utils_rdtsc::*;

fn _test_get_func() {
    let fft_size = 16;
    let mut fft_struct = Fft::new(fft_size);

    let mut fft_buf = generate_set_complex_float_array(fft_size);
    // print 10 elements of fft_buf
    println!("Before FFT: {:?}", fft_buf[0..5].to_vec());

    fft_struct.computefft(&mut fft_buf);

    println!("\nAfter FFT: {:?}", fft_buf[0..5].to_vec());

    let arg_vec = vec![CmTypes::VecC32(fft_buf)];
    let name = "vec_to_mat";
    let vec_to_mat_f = get_func(&name).unwrap();

    let vecmat = vec_to_mat_f(arg_vec);

    let res = match vecmat {
        CmTypes::DMatrixC32(x) => x,
        _ => panic!("Invalid return type"),
    };

    let arg_vec = vec![
        CmTypes::DMatrixC32(res.clone()),
        CmTypes::DMatrixC32(res.clone()),
    ];
    let name = "blas_cgemm";
    let blas_cgemm_f = get_func(&name).unwrap();

    let cmat = blas_cgemm_f(arg_vec);
    let res = match cmat {
        CmTypes::DMatrixC32(x) => x,
        _ => panic!("Invalid return type"),
    };
    // print 10 elements of first row
    println!("\nCgemm: {:?}", res.data.as_slice().to_vec());

    println!("All tests passed!");
}

fn main() {
    // test_get_func();
    let graph1_file = "/home/george/JRaph/jraph-core/examples/graphs/bench1.json";
    let graph2_file = "/home/george/JRaph/jraph-core/examples/graphs/bench2.json";
    let graph3_file = "/home/george/JRaph/jraph-core/examples/graphs/bench3.json";

    let graphs = vec![graph1_file, graph2_file, graph3_file];

    for i in 0..graphs.len() {
        let graph = from_json(graphs[i]).unwrap();
        let mult_factor = graph
            .stage(0)
            .node("FFT")
            .unwrap()
            .read()
            .unwrap()
            .mult_factor();

        let mut results: Vec<CmTypes> = Vec::new();
        let executor = Executor::new(0, 64);
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

        let time = cycles_to_sec(duration);
        println!(
            "Bench {} Execution Time for {} tasks: {:.2?} seconds",
            i + 1,
            mult_factor,
            time
        );
    }
}
