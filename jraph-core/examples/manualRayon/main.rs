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
    results: &mut Vec<DMatrix<Complex32>>,
) -> u64 {
    let fft_size = 10000;
    let num_stages = 3;

    let mut fft_buffers: Vec<Arc<Mutex<Fft>>> = Vec::new();
    for _ in 0..mult_factor {
        fft_buffers.push(Arc::new(Mutex::new(Fft::new(fft_size))));
    }

    let vecmat_results: Arc<Mutex<Vec<Option<DMatrix<Complex32>>>>> =
        Arc::new(Mutex::new(vec![None; mult_factor]));

    let cgemm_results: Arc<Mutex<Vec<DMatrix<Complex32>>>> = Arc::new(Mutex::new(Vec::new()));

    let stage_scheduled: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(vec![0; num_stages]));
    let stage_completed: Arc<Mutex<Vec<Vec<usize>>>> =
        Arc::new(Mutex::new(vec![Vec::new(); num_stages]));

    let start_time = rdtsc();
    threadpool.install(|| {
        // schedule fft
        fft_buffers
            .par_iter()
            .enumerate()
            .for_each(|(index, fft_struct)| {
                let mut fft_struct = fft_struct.lock().unwrap();
                fft_struct.computefft();
                // task index at stage 0 is completed
                stage_completed.lock().unwrap()[0].push(index);
                stage_scheduled.lock().unwrap()[0] += 1;
            });

        while stage_completed.lock().unwrap()[num_stages - 1].len() < mult_factor {
            for stage in 1..num_stages {
                let scheduled = stage_scheduled.lock().unwrap();
                if scheduled[stage] < mult_factor {
                    drop(scheduled);

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
                            let vecmat = vec_to_mat(arg_vec.to_vec());
                            // task index at stage 1 is completed
                            stage_completed.lock().unwrap()[stage].push(index);
                            stage_scheduled.lock().unwrap()[stage] += 1;
                            vecmat_results.lock().unwrap()[index] = Some(vecmat);
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
                                if let Some(vecmat) = &vecmat_results.lock().unwrap()[index_needed]
                                {
                                    arg_vecs.push((vecmat.clone(), task_idx));
                                }
                            }
                        }
                        arg_vecs.par_iter().for_each(|(arg_vec, index)| {
                            let index = *index;
                            let cmat = blas_cgemm(arg_vec, arg_vec);
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
    for i in 0..mult_factor {
        results.push(cgemm_results.lock().unwrap()[i].clone());
    }
    let end_time = rdtsc();
    end_time - start_time
}

fn bench2(
    threadpool: &ThreadPool,
    mult_factor: usize,
    results: &mut Vec<DMatrix<Complex32>>,
) -> u64 {
    let fft_size = 10000;
    let num_stages = 3;

    let mut fft_buffers: Vec<Arc<Mutex<Fft>>> = Vec::new();
    for _ in 0..mult_factor {
        fft_buffers.push(Arc::new(Mutex::new(Fft::new(fft_size))));
    }

    let vecmat_results: Arc<Mutex<Vec<Option<DMatrix<Complex32>>>>> =
        Arc::new(Mutex::new(vec![None; mult_factor]));

    let cgemm_results: Arc<Mutex<Vec<DMatrix<Complex32>>>> = Arc::new(Mutex::new(Vec::new()));

    let stage_scheduled: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(vec![0; num_stages]));
    let stage_completed: Arc<Mutex<Vec<Vec<usize>>>> =
        Arc::new(Mutex::new(vec![Vec::new(); num_stages]));

    let start_time = rdtsc();
    threadpool.install(|| {
        // schedule fft
        fft_buffers
            .par_iter()
            .enumerate()
            .for_each(|(index, fft_struct)| {
                let mut fft_struct = fft_struct.lock().unwrap();
                fft_struct.computefft();
                // task index at stage 0 is completed
                stage_completed.lock().unwrap()[0].push(index);
                stage_scheduled.lock().unwrap()[0] += 1;
            });

        while stage_completed.lock().unwrap()[num_stages - 1].len() < mult_factor {
            for stage in 1..num_stages {
                let scheduled = stage_scheduled.lock().unwrap();
                if scheduled[stage] < mult_factor {
                    drop(scheduled);

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
                            let vecmat = vec_to_mat(arg_vec.to_vec());
                            // task index at stage 1 is completed
                            stage_completed.lock().unwrap()[stage].push(index);
                            stage_scheduled.lock().unwrap()[stage] += 1;
                            vecmat_results.lock().unwrap()[index] = Some(vecmat);
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
                                    if let Some(vecmat) =
                                        &vecmat_results.lock().unwrap()[index_needed]
                                    {
                                        arg_vecs.push((vecmat.clone(), task_idx));
                                    }
                                }
                            }
                        }
                        arg_vecs.par_iter().for_each(|(arg_vec, index)| {
                            let index = *index;
                            let cmat = blas_cgemm(arg_vec, arg_vec);
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
    for i in 0..mult_factor {
        results.push(cgemm_results.lock().unwrap()[i].clone());
    }
    let end_time = rdtsc();
    end_time - start_time
}

fn bench3(
    threadpool: &ThreadPool,
    mult_factor: usize,
    results: &mut Vec<DMatrix<Complex32>>,
) -> u64 {
    let fft_size = 10000;
    let num_stages = 3;

    let mut fft_buffers: Vec<Arc<Mutex<Fft>>> = Vec::new();
    for _ in 0..mult_factor {
        fft_buffers.push(Arc::new(Mutex::new(Fft::new(fft_size))));
    }

    let vecmat_results: Arc<Mutex<Vec<Option<DMatrix<Complex32>>>>> =
        Arc::new(Mutex::new(vec![None; mult_factor]));

    let cgemm_results: Arc<Mutex<Vec<DMatrix<Complex32>>>> = Arc::new(Mutex::new(Vec::new()));

    let stage_scheduled: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(vec![0; num_stages]));
    let stage_completed: Arc<Mutex<Vec<Vec<usize>>>> =
        Arc::new(Mutex::new(vec![Vec::new(); num_stages]));

    let start_time = rdtsc();
    threadpool.install(|| {
        // schedule fft
        fft_buffers
            .par_iter()
            .enumerate()
            .for_each(|(index, fft_struct)| {
                let mut fft_struct = fft_struct.lock().unwrap();
                fft_struct.computefft();
                // task index at stage 0 is completed
                stage_completed.lock().unwrap()[0].push(index);
                stage_scheduled.lock().unwrap()[0] += 1;
            });

        while stage_completed.lock().unwrap()[num_stages - 1].len() < mult_factor {
            for stage in 1..num_stages {
                let scheduled = stage_scheduled.lock().unwrap();
                if scheduled[stage] < mult_factor {
                    drop(scheduled);

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
                            let vecmat = vec_to_mat(arg_vec.to_vec());
                            // task index at stage 1 is completed
                            stage_completed.lock().unwrap()[stage].push(index);
                            stage_scheduled.lock().unwrap()[stage] += 1;
                            vecmat_results.lock().unwrap()[index] = Some(vecmat);
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
                                    if let Some(vecmat) =
                                        &vecmat_results.lock().unwrap()[index_needed]
                                    {
                                        res_vector.push(vecmat.clone());
                                    }
                                }
                                arg_vecs.push((res_vector, task_idx));
                            }
                        }
                        arg_vecs.par_iter().for_each(|(arg_vec, index)| {
                            let index = *index;
                            let cmat = multiple_cgemm(arg_vec.to_vec());
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
    for i in 0..mult_factor {
        results.push(cgemm_results.lock().unwrap()[i].clone());
    }
    let end_time = rdtsc();
    end_time - start_time
}

fn main() {
    let core_offset = 0;
    let workers = 4;
    let mut core_ids = core_affinity::get_core_ids().unwrap();
    core_ids.sort();
    println!("Core IDs: {:?}", core_ids);
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

    let factors = vec![25, 50, 75, 100];

    for factor in factors {
        let mut results: Vec<DMatrix<Complex32>> = Vec::new();

        // Bench 1
        let duration = bench1(&threadpool, factor, &mut results);

        let val1 = validate1(factor);

        // assert results == val1
        for i in 0..factor {
            assert_eq!(results[i], val1[i]);
        }

        let time = cycles_to_ms(duration);
        println!(
            "Bench 1 Execution Time for {} tasks: {:.4?} ms",
            factor, time
        );

        // Bench 2
        results.clear();
        let duration = bench2(&threadpool, factor, &mut results);

        let val2 = validate2(factor);

        // assert results == val2
        for i in 0..factor {
            assert_eq!(results[i], val2[i]);
        }

        let time = cycles_to_ms(duration);
        println!(
            "Bench 2 Execution Time for {} tasks: {:.4?} ms",
            factor, time
        );

        // Bench 3
        results.clear();
        let duration = bench3(&threadpool, factor, &mut results);

        let val3 = validate3(factor);

        // assert results == val3
        for i in 0..factor {
            assert_eq!(results[i], val3[i]);
        }

        let time = cycles_to_ms(duration);
        println!(
            "Bench 3 Execution Time for {} tasks: {:.4?} ms",
            factor, time
        );
        println!("");
    }
}
