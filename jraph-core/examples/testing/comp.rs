use crate::functions::*;
use jraph_core::func_reg::get_func;
use jraph_core::time_buffer::TimeBuffer;
use jraph_core::utils_rdtsc::*;
use num_complex::Complex32;
use shared::CmTypes;

const BUF_SIZE: usize = 100;
const BUF_NUM: usize = 500;
const REPEAT: usize = 100;

pub fn vec_mat() {
    let bufs = {
        let mut vec = Vec::new();
        for _ in 0..BUF_NUM {
            let data = generate_set_complex_vec(BUF_SIZE);
            vec.push(data);
        }
        vec
    };

    let mut timebuffer = TimeBuffer::new();
    timebuffer.init_task("Man-Vec-Mat", REPEAT);

    for run_idx in 0..REPEAT {
        for factor in 0..BUF_NUM {
            let data = &bufs[factor];
            let t1 = rdtsc();
            let _ = vec_to_mat(data);
            let t2 = rdtsc();
            timebuffer.add_time("Man-Vec-Mat", run_idx, t2 - t1);
        }
    }

    let avg = timebuffer.task_average("Man-Vec-Mat", "ms");
    println!("Manual Vec-Mat Avg({}): {:.4?} ms", REPEAT, avg);

    let cmt_vec = {
        let mut vec = Vec::new();
        for i in 0..BUF_NUM {
            let data = generate_set_complex_vec(BUF_SIZE);
            let cmt = CmTypes::VecC32(data);
            vec.push(cmt);
        }
        vec
    };

    timebuffer.init_task("Cmt-Vec-Mat", REPEAT);
    let func = get_func("vec_to_mat").unwrap();

    for run_idx in 0..REPEAT {
        for factor in 0..BUF_NUM {
            let data = &cmt_vec[factor];
            let args = vec![data.clone()];
            let t1 = rdtsc();
            let _ = func(args);
            let t2 = rdtsc();
            timebuffer.add_time("Cmt-Vec-Mat", run_idx, t2 - t1);
        }
    }

    let avg = timebuffer.task_average("Cmt-Vec-Mat", "ms");
    println!("CmTypes Vec-Mat Avg({}): {:.4?} ms", REPEAT, avg);
}

pub fn mt_cgemm() {
    let bufs = {
        let mut vec = Vec::new();
        for _ in 0..BUF_NUM {
            let data = generate_set_complex_dmatrix(BUF_SIZE);
            vec.push(data);
        }
        vec
    };

    let mut timebuffer = TimeBuffer::new();
    timebuffer.init_task("Man-MultCgemm", REPEAT);

    for run_idx in 0..REPEAT {
        for factor in 0..BUF_NUM {
            let buf_vec = vec![bufs[factor].clone(), bufs[factor].clone()];
            let args = buf_vec.iter().collect();
            let t1 = rdtsc();
            let _ = multiple_cgemm(args);
            let t2 = rdtsc();
            timebuffer.add_time("Man-MultCgemm", run_idx, t2 - t1);
        }
    }

    let avg = timebuffer.task_average("Man-MultCgemm", "ms");
    println!("Manual MultCgemm Avg({}): {:.4?} ms", REPEAT, avg);

    let cmt_vec = {
        let mut vec = Vec::new();
        for i in 0..BUF_NUM {
            let data = generate_set_complex_dmatrix(BUF_SIZE);
            let cmt = CmTypes::DMatrixC32(data.into());
            vec.push(cmt);
        }
        vec
    };

    timebuffer.init_task("Cmt-MultCgemm", REPEAT);
    let func = get_func("multiple_cgemm").unwrap();

    for run_idx in 0..REPEAT {
        for factor in 0..BUF_NUM {
            let data = &cmt_vec[factor];
            let args = vec![data.clone(), data.clone()];
            let t1 = rdtsc();
            let _ = func(args);
            let t2 = rdtsc();
            timebuffer.add_time("Cmt-MultCgemm", run_idx, t2 - t1);
        }
    }

    let avg = timebuffer.task_average("Cmt-MultCgemm", "ms");
    println!("CmTypes MultCgemm Avg({}): {:.4?} ms", REPEAT, avg);
}
