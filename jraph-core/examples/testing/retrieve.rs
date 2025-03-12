use crate::functions::generate_set_complex_dmatrix;
use nalgebra::DMatrix;
use num_complex::Complex32;
use shared::CmTypes;
use std::sync::Arc;

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

    let mut timebuf = TimeBuffer::new();
    timebuf.init_task("Mat-Retrieve", repeat);
    timebuf.init_task("Mat-Clone", repeat);
    timebuf.init_task("CmT-Retrieve", repeat);
    timebuf.init_task("CmT-Clone", repeat);


    for run_idx in 0..repeat {
        let mut res_cm: Vec<DMatrix<Complex32>> = Vec::new();
        let mut res_mat: Vec<DMatrix<Complex32>> = Vec::new();

        // retrieve matrices from buf_mat
        let tmat_start = rdtsc();
        for i in 0..factor {
            let mat = &buf_mat[i];
            let t1_clone = rdtsc();
            res_mat.push(mat.clone());
            let t2_clone = rdtsc();
            timebuf.add_time("Mat-Clone", run_idx, t2_clone - t1_clone);
        }
        let tmat_end = rdtsc();
        timebuf.add_time("Mat-Retrieve", run_idx, tmat_end - tmat_start);

        // retrieve matrices from buf_cm
        let tcm_start = rdtsc();
        for i in 0..factor {
            let cm = &buf_cm[i];
            let res = match &cm {
                CmTypes::DMatrixC32(mat) => mat,
                _ => panic!("Invalid type"),
            };
            let res_ref: &DMatrix<Complex32> = &res;
            let t1_clone = rdtsc();
            res_cm.push(res_ref.clone());
            let t2_clone = rdtsc();
            timebuf.add_time("CmT-Clone", run_idx, t2_clone - t1_clone);
        }
        let tcm_end = rdtsc();
        timebuf.add_time("CmT-Retrieve", run_idx, tcm_end - tcm_start);
    }

    let avg_mat = timebuf.task_average("Mat-Retrieve", "ms");
    let avg_mat_clone = timebuf.task_average("Mat-Clone", "ms");
    let avg_cmt = timebuf.task_average("CmT-Retrieve", "ms");
    let avg_cmt_clone = timebuf.task_average("CmT-Clone", "ms");

    println!("Avg ({}) for matrix retrieval: {:.4?} ms", repeat, avg_mat);
    println!("Mat Retrieval Clone Time: {:.4?} ms", avg_mat_clone);
    println!();
    println!("Avg ({}) for CmTypes retrieval: {:.4?} ms", repeat, avg_cmt);
    println!("CmTypes Retrieval Clone Time: {:.4?} ms", avg_cmt_clone);
}
