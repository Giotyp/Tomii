use crate::functions::*;
use jraph_core::func_reg::get_func;
use jraph_core::time_buffer::TimeBuffer;
use jraph_core::utils_rdtsc::*;
use num_complex::Complex32;
use shared::CmTypes;
use std::time::Instant;

use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::sync::{Arc, Mutex};

const BUF_SIZE: usize = 225;
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

    let mut timebuffer = TimeBuffer::new(1, REPEAT);
    timebuffer.init_task("Man-Vec-Mat");

    for run_idx in 0..REPEAT {
        for factor in 0..BUF_NUM {
            let data = &bufs[factor];
            let t1 = Instant::now();
            let _ = vec_to_mat(data);
            let t2 = Instant::now();
            timebuffer.add_time("Man-Vec-Mat", run_idx, 0, t2 - t1);
        }
    }

    let cmt_vec = {
        let mut vec = Vec::new();
        for i in 0..BUF_NUM {
            let data = generate_set_complex_vec(BUF_SIZE);
            let cmt = CmTypes::VecC32(data);
            vec.push(cmt);
        }
        vec
    };

    timebuffer.init_task("Cmt-Vec-Mat");
    let func = get_func("vec_to_mat").unwrap();

    for run_idx in 0..REPEAT {
        for factor in 0..BUF_NUM {
            let data = &cmt_vec[factor];
            let args = vec![data.clone()];
            let t1 = Instant::now();
            let _ = func(args);
            let t2 = Instant::now();
            timebuffer.add_time("Cmt-Vec-Mat", run_idx, 0, t2 - t1);
        }
    }

    timebuffer.print_stats("Vec-mat direct vs wrapper call", None);
}

pub fn mt_cgemm() {
    let buf_nums = vec![100, 200, 300, 400, 500];

    for numbuf in buf_nums {
        let bufs = {
            let mut vec = Vec::new();
            for _ in 0..numbuf {
                let data = generate_set_complex_dmatrix(BUF_SIZE);
                vec.push(data);
            }
            vec
        };

        // warmup
        for factor in 0..10 {
            let buf_vec = vec![bufs[factor].clone(), bufs[factor].clone()];
            let _ = multiple_cgemm(buf_vec.iter().collect());
        }

        let mut timebuffer = TimeBuffer::new(1, REPEAT);
        timebuffer.init_task("Man-MultCgemm");

        for run_idx in 0..REPEAT {
            for factor in 0..numbuf {
                let buf_vec = vec![bufs[factor].clone(), bufs[factor].clone()];
                let args = buf_vec.iter().collect();
                let t1 = Instant::now();
                let _ = multiple_cgemm(args);
                let t2 = Instant::now();
                timebuffer.add_time("Man-MultCgemm", run_idx, 0, t2 - t1);
            }
        }

        let cmt_vec = {
            let mut vec = Vec::new();
            for i in 0..numbuf {
                let data = generate_set_complex_dmatrix(BUF_SIZE);
                let cmt = CmTypes::DMatrixC32(data.into());
                vec.push(cmt);
            }
            vec
        };

        timebuffer.init_task("Cmt-MultCgemm");
        let func = get_func("multiple_cgemm").unwrap();

        for run_idx in 0..REPEAT {
            for factor in 0..numbuf {
                let data = &cmt_vec[factor];
                let args = vec![data.clone(), data.clone()];
                let t1 = Instant::now();
                let _ = func(args);
                let t2 = Instant::now();
                timebuffer.add_time("Cmt-MultCgemm", run_idx, 0, t2 - t1);
            }
        }

        let bench = &format!(
            "MultCgemm direct vs wrapper call for BUF_SIZE({})-BUF_NUM({})",
            BUF_SIZE, numbuf
        );
        timebuffer.print_stats(bench, None);
    }
}

pub fn rayon_gemm() {
    let core_offset = 12;
    let workers = 10;
    let mut core_ids = core_affinity::get_core_ids().unwrap();
    core_ids.sort();
    let cores_to_use: Vec<core_affinity::CoreId> =
        core_ids[core_offset..core_offset + workers].to_vec();
    let threadpool = ThreadPoolBuilder::new()
        .num_threads(workers)
        .start_handler(move |thread_index| {
            // Pin each thread to a specific core
            let core_id = cores_to_use[thread_index];
            core_affinity::set_for_current(core_id);
        })
        .build()
        .unwrap();

    let buf_nums = vec![100];

    for numbuf in buf_nums {
        let bufs = {
            let mut vec = Vec::new();
            for _ in 0..numbuf {
                let data = generate_set_complex_dmatrix(BUF_SIZE);
                vec.push(data);
            }
            vec
        };

        let mut timebuffer = TimeBuffer::new(workers, REPEAT);
        timebuffer.init_task("Man-MultCgemm");
        timebuffer.init_task("Cmt-MultCgemm");
        let arc_timebuffer = Arc::new(Mutex::new(timebuffer));

        for run_idx in 0..REPEAT {
            let mut arg_vecs = Vec::new();
            for factor in 0..numbuf {
                arg_vecs.push(bufs.clone());
            }

            threadpool.install(|| {
                arg_vecs.par_iter().for_each(|args| {
                    let buf = args.iter().collect();
                    let t1 = Instant::now();
                    let _ = multiple_cgemm(buf);
                    let t2 = Instant::now();
                    let worker_index = rayon::current_thread_index().unwrap();
                    let mut timebuf = arc_timebuffer.lock().unwrap();
                    timebuf.add_time("Man-MultCgemm", run_idx, worker_index, t2 - t1);
                    drop(timebuf);
                });
            });
        }

        let cmt_vec = {
            let mut vec = Vec::new();
            for i in 0..numbuf {
                let data = generate_set_complex_dmatrix(BUF_SIZE);
                let cmt = CmTypes::DMatrixC32(data.into());
                vec.push(cmt);
            }
            vec
        };

        let func = get_func("multiple_cgemm").unwrap();

        for run_idx in 0..REPEAT {
            let mut arg_vecs = Vec::new();
            for factor in 0..numbuf {
                arg_vecs.push(cmt_vec.clone());
            }

            threadpool.install(|| {
                arg_vecs.par_iter().for_each(|args| {
                    let arg = args.clone();
                    let t1 = Instant::now();
                    let _ = func(arg);
                    let t2 = Instant::now();
                    let worker_index = rayon::current_thread_index().unwrap();
                    let mut timebuf = arc_timebuffer.lock().unwrap();
                    timebuf.add_time("Cmt-MultCgemm", run_idx, worker_index, t2 - t1);
                    drop(timebuf);
                });
            });
        }

        let bench = &format!(
            "Rayon MultCgemm direct vs wrapper call for BUF_SIZE({})-BUF_NUM({})",
            BUF_SIZE, numbuf
        );
        let mut timebuf = arc_timebuffer.lock().unwrap();
        timebuf.print_stats(bench, None);
    }
}
