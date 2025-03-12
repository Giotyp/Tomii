#![allow(dead_code)]
mod funcs;
mod validation;

use core_affinity;
use funcs::*;
use nalgebra::*;
use num_complex::Complex32;
use rayon::prelude::*;
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::sync::{Arc, Mutex};
use validation::*;

use jraph_core::time_buffer::TimeBuffer;
use jraph_core::utils_rdtsc::*;

fn find_index(idx: isize, mult_factor: usize) -> usize {
    if idx >= 0 {
        idx as usize
    } else {
        mult_factor - idx.abs() as usize
    }
}

fn bench1(
    threadpool: &ThreadPool,
    mult_factor: usize,
    cgemm_results: Arc<Mutex<Vec<DMatrix<Complex32>>>>,
    arc_timebuf: Arc<Mutex<TimeBuffer>>,
    run_idx: usize,
) -> u64 {
    let fft_size = 10000;
    let num_stages = 3;

    let mut fft_buffers: Vec<Arc<Mutex<Fft>>> = Vec::new();
    for _ in 0..mult_factor {
        fft_buffers.push(Arc::new(Mutex::new(Fft::new(fft_size))));
    }

    let vecmat_results: Arc<Mutex<Vec<DMatrix<Complex32>>>> =
        Arc::new(Mutex::new(vec![
            DMatrix::<Complex32>::zeros(1, 1);
            mult_factor
        ]));

    let stage_scheduled: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(vec![0; num_stages]));
    let stage_completed: Arc<Mutex<Vec<Vec<usize>>>> =
        Arc::new(Mutex::new(vec![Vec::new(); num_stages]));

    let start_time = rdtsc();
    threadpool.install(|| {
        while stage_completed.lock().unwrap()[num_stages - 1].len() < mult_factor {
            for stage in 0..num_stages {
                let scheduled = stage_scheduled.lock().unwrap();
                if scheduled[stage] < mult_factor {
                    drop(scheduled);

                    if stage == 0 {
                        // fft

                        fft_buffers
                            .par_iter()
                            .enumerate()
                            .for_each(|(index, fft_struct)| {
                                let mut fft_struct = fft_struct.lock().unwrap();
                                let t1 = rdtsc();
                                fft_struct.computefft();
                                let t2 = rdtsc();
                                let mut tb = arc_timebuf.lock().unwrap();
                                tb.add_time("FFT-Comp", run_idx, t2 - t1);
                                drop(tb);
                                // task index at stage 0 is completed
                                stage_completed.lock().unwrap()[stage].push(index);
                                stage_scheduled.lock().unwrap()[stage] += 1;
                            });
                    }

                    if stage == 1 {
                        // vec to mat
                        let completed_indices: Vec<usize> =
                            stage_completed.lock().unwrap()[stage - 1].clone();

                        let mut arg_vecs = Vec::new();
                        for task_idx in 0..mult_factor {
                            let req_idx = (task_idx - 0) as isize;
                            let index_needed = find_index(req_idx, mult_factor);

                            if completed_indices.contains(&index_needed) {
                                let arg = fft_buffers[index_needed].lock().unwrap().get_buf();
                                arg_vecs.push((arg, task_idx));
                            }
                        }
                        arg_vecs.par_iter().for_each(|(arg_vec, index)| {
                            let index = *index;
                            let t1 = rdtsc();
                            let vecmat = vec_to_mat(arg_vec);
                            let t2 = rdtsc();
                            let mut tb = arc_timebuf.lock().unwrap();
                            tb.add_time("VecMat-Comp", run_idx, t2 - t1);
                            drop(tb);
                            // task index at stage 1 is completed
                            stage_completed.lock().unwrap()[stage].push(index);
                            stage_scheduled.lock().unwrap()[stage] += 1;
                            vecmat_results.lock().unwrap()[index] = vecmat;
                        });
                    } else if stage == 2 {
                        // blas cgemm
                        let mut arg_vecs = Vec::new();
                        let completed_indices: Vec<usize> =
                            stage_completed.lock().unwrap()[stage - 1].clone();

                        for task_idx in 0..mult_factor {
                            let req_idx = (task_idx - 0) as isize;
                            let index_needed = find_index(req_idx, mult_factor);

                            // Check if all dependencies are present in completed_indices
                            if completed_indices.contains(&index_needed) {
                                let vecmat = &vecmat_results.lock().unwrap()[index_needed];
                                arg_vecs.push((vecmat.clone(), task_idx));
                            }
                        }
                        arg_vecs.par_iter().for_each(|(arg_vec, index)| {
                            let index = *index;
                            let t1 = rdtsc();
                            let cmat = blas_cgemm(arg_vec, arg_vec);
                            let t2 = rdtsc();
                            let mut tb = arc_timebuf.lock().unwrap();
                            tb.add_time("CGEMM-Comp", run_idx, t2 - t1);
                            drop(tb);
                            // task index at stage 2 is completed
                            stage_completed.lock().unwrap()[stage].push(index);
                            stage_scheduled.lock().unwrap()[stage] += 1;
                            cgemm_results.lock().unwrap().push(cmat);
                        });
                    }
                }
            }
        }
    });
    let end_time = rdtsc();
    end_time - start_time
}

fn bench2(
    threadpool: &ThreadPool,
    mult_factor: usize,
    cgemm_results: Arc<Mutex<Vec<DMatrix<Complex32>>>>,
    arc_timebuf: Arc<Mutex<TimeBuffer>>,
    run_idx: usize,
) -> u64 {
    let fft_size = 10000;
    let num_stages = 3;

    let mut fft_buffers: Vec<Arc<Mutex<Fft>>> = Vec::new();
    for _ in 0..mult_factor {
        fft_buffers.push(Arc::new(Mutex::new(Fft::new(fft_size))));
    }

    let vecmat_results: Arc<Mutex<Vec<DMatrix<Complex32>>>> =
        Arc::new(Mutex::new(vec![
            DMatrix::<Complex32>::zeros(1, 1);
            mult_factor
        ]));

    let stage_scheduled: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(vec![0; num_stages]));
    let stage_completed: Arc<Mutex<Vec<Vec<usize>>>> =
        Arc::new(Mutex::new(vec![Vec::new(); num_stages]));

    let start_time = rdtsc();
    threadpool.install(|| {
        while stage_completed.lock().unwrap()[num_stages - 1].len() < mult_factor {
            for stage in 0..num_stages {
                let scheduled = stage_scheduled.lock().unwrap();
                if scheduled[stage] < mult_factor {
                    drop(scheduled);

                    if stage == 0 {
                        // fft
                        fft_buffers
                            .par_iter()
                            .enumerate()
                            .for_each(|(index, fft_struct)| {
                                let mut fft_struct = fft_struct.lock().unwrap();
                                let t1 = rdtsc();
                                fft_struct.computefft();
                                let t2 = rdtsc();
                                let mut tb = arc_timebuf.lock().unwrap();
                                tb.add_time("FFT-Comp", run_idx, t2 - t1);
                                drop(tb);
                                // task index at stage 0 is completed
                                stage_completed.lock().unwrap()[stage].push(index);
                                stage_scheduled.lock().unwrap()[stage] += 1;
                            });
                    }

                    if stage == 1 {
                        // vec to mat
                        let completed_indices: Vec<usize> =
                            stage_completed.lock().unwrap()[stage - 1].clone();

                        let mut arg_vecs = Vec::new();
                        for task_idx in 0..mult_factor {
                            let req_idx = (task_idx - 0) as isize;
                            let index_needed = find_index(req_idx, mult_factor);

                            if completed_indices.contains(&index_needed) {
                                let arg = fft_buffers[index_needed].lock().unwrap().get_buf();
                                arg_vecs.push((arg, task_idx));
                            }
                        }
                        arg_vecs.par_iter().for_each(|(arg_vec, index)| {
                            let index = *index;
                            let t1 = rdtsc();
                            let vecmat = vec_to_mat(arg_vec);
                            let t2 = rdtsc();
                            let mut tb = arc_timebuf.lock().unwrap();
                            tb.add_time("VecMat-Comp", run_idx, t2 - t1);
                            drop(tb);
                            // task index at stage 1 is completed
                            stage_completed.lock().unwrap()[stage].push(index);
                            stage_scheduled.lock().unwrap()[stage] += 1;
                            vecmat_results.lock().unwrap()[index] = vecmat;
                        });
                    } else if stage == 2 {
                        // blas cgemm
                        let mut arg_vecs = Vec::new();
                        let completed_indices: Vec<usize> =
                            stage_completed.lock().unwrap()[stage - 1].clone();

                        for task_idx in 0..mult_factor {
                            let indices = {
                                let mut vec_idx = Vec::new();
                                for i in 0..2 {
                                    let req_idx = (task_idx as isize) - (i as isize);
                                    vec_idx.push(find_index(req_idx, mult_factor));
                                }
                                vec_idx
                            };

                            // Check if all dependencies are present in completed_indices
                            if indices
                                .iter()
                                .all(|&index_needed| completed_indices.contains(&index_needed))
                            {
                                for index_needed in indices {
                                    let vecmat = &vecmat_results.lock().unwrap()[index_needed];
                                    arg_vecs.push((vecmat.clone(), task_idx));
                                }
                            }
                        }
                        arg_vecs.par_iter().for_each(|(arg_vec, index)| {
                            let index = *index;
                            let t1 = rdtsc();
                            let cmat = blas_cgemm(arg_vec, arg_vec);
                            let t2 = rdtsc();
                            let mut tb = arc_timebuf.lock().unwrap();
                            tb.add_time("CGEMM-Comp", run_idx, t2 - t1);
                            drop(tb);
                            // task index at stage 2 is completed
                            stage_completed.lock().unwrap()[stage].push(index);
                            stage_scheduled.lock().unwrap()[stage] += 1;
                            cgemm_results.lock().unwrap().push(cmat);
                        });
                    }
                }
            }
        }
    });
    let end_time = rdtsc();
    end_time - start_time
}

fn bench3(
    threadpool: &ThreadPool,
    mult_factor: usize,
    cgemm_results: Arc<Mutex<Vec<DMatrix<Complex32>>>>,
    arc_timebuf: Arc<Mutex<TimeBuffer>>,
    run_idx: usize,
) -> u64 {
    let fft_size = 10000;
    let num_stages = 3;

    let mut fft_buffers: Vec<Arc<Mutex<Fft>>> = Vec::new();
    for _ in 0..mult_factor {
        fft_buffers.push(Arc::new(Mutex::new(Fft::new(fft_size))));
    }

    let vecmat_results: Arc<Mutex<Vec<DMatrix<Complex32>>>> =
        Arc::new(Mutex::new(vec![
            DMatrix::<Complex32>::zeros(1, 1);
            mult_factor
        ]));

    let stage_scheduled: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(vec![0; num_stages]));
    let stage_completed: Arc<Mutex<Vec<Vec<usize>>>> =
        Arc::new(Mutex::new(vec![Vec::new(); num_stages]));

    let start_time = rdtsc();
    threadpool.install(|| {
        while stage_completed.lock().unwrap()[num_stages - 1].len() < mult_factor {
            for stage in 0..num_stages {
                let scheduled = stage_scheduled.lock().unwrap();
                if scheduled[stage] < mult_factor {
                    drop(scheduled);

                    if stage == 0 {
                        // fft
                        fft_buffers
                            .par_iter()
                            .enumerate()
                            .for_each(|(index, fft_struct)| {
                                let mut fft_struct = fft_struct.lock().unwrap();
                                let t1 = rdtsc();
                                fft_struct.computefft();
                                let t2 = rdtsc();
                                let mut tb = arc_timebuf.lock().unwrap();
                                tb.add_time("FFT-Comp", run_idx, t2 - t1);
                                drop(tb);
                                // task index at stage 0 is completed
                                stage_completed.lock().unwrap()[stage].push(index);
                                stage_scheduled.lock().unwrap()[stage] += 1;
                            });
                    }

                    if stage == 1 {
                        // vec to mat
                        let completed_indices: Vec<usize> =
                            stage_completed.lock().unwrap()[stage - 1].clone();

                        let mut arg_vecs = Vec::new();
                        for task_idx in 0..mult_factor {
                            let req_idx = (task_idx - 0) as isize;
                            let index_needed = find_index(req_idx, mult_factor);

                            if completed_indices.contains(&index_needed) {
                                let arg = fft_buffers[index_needed].lock().unwrap().get_buf();
                                arg_vecs.push((arg, task_idx));
                            }
                        }
                        arg_vecs.par_iter().for_each(|(arg_vec, index)| {
                            let index = *index;
                            let t1 = rdtsc();
                            let vecmat = vec_to_mat(arg_vec);
                            let t2 = rdtsc();
                            let mut tb = arc_timebuf.lock().unwrap();
                            tb.add_time("VecMat-Comp", run_idx, t2 - t1);
                            drop(tb);
                            // task index at stage 1 is completed
                            stage_completed.lock().unwrap()[stage].push(index);
                            stage_scheduled.lock().unwrap()[stage] += 1;
                            vecmat_results.lock().unwrap()[index] = vecmat;
                        });
                    } else if stage == 2 {
                        // blas cgemm
                        let mut arg_vecs = Vec::new();
                        let completed_indices: Vec<usize> =
                            stage_completed.lock().unwrap()[stage - 1].clone();

                        for task_idx in 0..mult_factor {
                            let indices = {
                                let mut vec_idx = Vec::new();
                                for i in 0..mult_factor {
                                    let req_idx = (task_idx as isize) - (i as isize);
                                    vec_idx.push(find_index(req_idx, mult_factor));
                                }
                                vec_idx
                            };

                            // Check if all dependencies are present in completed_indices
                            if indices
                                .iter()
                                .all(|&index_needed| completed_indices.contains(&index_needed))
                            {
                                let mut res_vector = Vec::new();
                                for index_needed in indices {
                                    let vecmat = &vecmat_results.lock().unwrap()[index_needed];
                                    res_vector.push(vecmat.clone());
                                }
                                arg_vecs.push((res_vector, task_idx));
                            }
                        }
                        arg_vecs.par_iter().for_each(|(arg_vec, index)| {
                            let index = *index;
                            let refmats: Vec<&DMatrix<Complex32>> = arg_vec.iter().collect();
                            let t1_comp = rdtsc();
                            let cmat = multiple_cgemm(refmats);
                            let t2 = rdtsc();
                            let mut tb = arc_timebuf.lock().unwrap();
                            tb.add_time("CGEMM-Comp", run_idx, t2 - t1_comp);
                            drop(tb);
                            // task index at stage 2 is completed
                            stage_completed.lock().unwrap()[stage].push(index);
                            stage_scheduled.lock().unwrap()[stage] += 1;
                            cgemm_results.lock().unwrap().push(cmat);
                        });
                    }
                }
            }
        }
    });
    let end_time = rdtsc();
    end_time - start_time
}

fn main() {
    let core_offset = 1;
    let workers = 1;
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

    let factors = vec![100];
    let repeat = 50;

    for bench in 0..3 {
        for factor in &factors {
            let factor = *factor;
            let mut timebuf = TimeBuffer::new();
            timebuf.init_task("Total", repeat);
            timebuf.init_task("FFT-Comp", repeat);
            timebuf.init_task("VecMat", repeat);
            timebuf.init_task("CGEMM", repeat);
            timebuf.init_task("VecMat-Comp", repeat);
            timebuf.init_task("CGEMM-Comp", repeat);
            let arc_timebuf = Arc::new(Mutex::new(timebuf));

            let mut results: Vec<DMatrix<Complex32>> = Vec::with_capacity(factor);
            let cgemm_results = Arc::new(Mutex::new(results));

            for run_idx in 0..repeat {
                let mut res_lock = cgemm_results.lock().unwrap();
                res_lock.clear();
                drop(res_lock);

                let duration = {
                    match bench {
                        0 => bench1(
                            &threadpool,
                            factor,
                            cgemm_results.clone(),
                            arc_timebuf.clone(),
                            run_idx,
                        ),
                        1 => bench2(
                            &threadpool,
                            factor,
                            cgemm_results.clone(),
                            arc_timebuf.clone(),
                            run_idx,
                        ),
                        2 => bench3(
                            &threadpool,
                            factor,
                            cgemm_results.clone(),
                            arc_timebuf.clone(),
                            run_idx,
                        ),
                        _ => 0,
                    }
                };

                // let val = {
                //     match bench {
                //         0 => validate1(factor),
                //         1 => validate2(factor),
                //         2 => validate3(factor),
                //         _ => Vec::new(),
                //     }
                // };

                // for i in 0..factor {
                //     assert_eq!(results[i], val[i]);
                // }
                let mut timebuf = arc_timebuf.lock().unwrap();
                timebuf.add_time("Total", run_idx, duration);
                drop(timebuf);
            }
            // Average times
            let timebuf = arc_timebuf.lock().unwrap();
            let avg_total = timebuf.task_average("Total", "ms");
            let avg_fft = timebuf.task_average("FFT-Comp", "ms");
            let avg_vecmat_comp = timebuf.task_average("VecMat-Comp", "ms");
            let avg_cgemm_comp = timebuf.task_average("CGEMM-Comp", "ms");
            println!(
                "Bench {} Average Total Time({}) for {} tasks: {:.4?} ms",
                bench + 1,
                repeat,
                factor,
                avg_total
            );
            println!(
                "FFT-Comp: {:.4?} ms, VecMat-Comp: {:.4?} ms, CGEMM-Comp: {:.4?} ms",
                avg_fft, avg_vecmat_comp, avg_cgemm_comp
            );
            println!();
        }
    }
}
