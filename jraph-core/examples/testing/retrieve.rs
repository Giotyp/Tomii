use crate::functions::generate_set_complex_dmatrix;
use nalgebra::DMatrix;
use num_complex::Complex32;
use shared::CmTypes;
use std::sync::Arc;
use std::time::Instant;

use jraph_core::time_buffer::TimeBuffer;
use jraph_core::utils_rdtsc::*;

pub fn retrieve() {
    let factor = 100;

    let mut buf_cm: Vec<CmTypes> = Vec::new();
    let mut buf_mat: Vec<DMatrix<Complex32>> = Vec::new();

    for _ in 0..factor {
        let matrix = generate_set_complex_dmatrix(100);
        buf_mat.push(matrix.clone());

        let mat_arc = Arc::new(matrix);
        let arg = CmTypes::DMatrixC32(mat_arc);
        buf_cm.push(arg);
    }

    let repeat = 50;

    let mut timebuf = TimeBuffer::new(1, repeat);
    timebuf.init_task("Mat-Retrieve");
    timebuf.init_task("Mat-Clone");
    timebuf.init_task("CmT-Retrieve");
    timebuf.init_task("CmT-Clone");

    for run_idx in 0..repeat {
        let mut res_cm: Vec<DMatrix<Complex32>> = Vec::new();
        let mut res_mat: Vec<DMatrix<Complex32>> = Vec::new();

        // retrieve matrices from buf_mat
        let tmat_start = Instant::now();
        for i in 0..factor {
            let mat = &buf_mat[i];
            let t1_clone = Instant::now();
            res_mat.push(mat.clone());
            let t2_clone = Instant::now();
            timebuf.add_time("Mat-Clone", run_idx, 0, t2_clone - t1_clone);
        }
        let tmat_end = Instant::now();
        timebuf.add_time("Mat-Retrieve", run_idx, 0, tmat_end - tmat_start);

        // retrieve matrices from buf_cm
        let tcm_start = Instant::now();
        for i in 0..factor {
            let cm = &buf_cm[i];
            let res = match &cm {
                CmTypes::DMatrixC32(mat) => mat,
                _ => panic!("Invalid type"),
            };
            let res_ref: &DMatrix<Complex32> = &res;
            let t1_clone = Instant::now();
            res_cm.push(res_ref.clone());
            let t2_clone = Instant::now();
            timebuf.add_time("CmT-Clone", run_idx, 0, t2_clone - t1_clone);
        }
        let tcm_end = Instant::now();
        timebuf.add_time("CmT-Retrieve", run_idx, 0, tcm_end - tcm_start);
    }

    let bench = "CmTypes Retrieve Comparison";
    timebuf.print_stats(bench, None);
}
