#![allow(unused_imports)]
#![allow(dead_code)]
use core_affinity;
use rayon::prelude::*;
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::graph_struct::*;
use crate::time_buffer::TimeBuffer;
use shared::CmTypes;
use std::collections::HashMap;

use crate::temp_funcs::*;
use crate::utils_rdtsc::*;
use num_complex::Complex32;

fn find_index(idx: isize, mult_factor: usize) -> usize {
    if idx >= 0 {
        idx as usize
    } else {
        mult_factor - idx.abs() as usize
    }
}

pub struct Executor {
    workers: usize,
    threadpool: ThreadPool,
}

// Public API
impl Executor {
    pub fn new(core_offset: usize, workers: usize) -> Executor {
        // Create threadpool and pin workers to cores
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

        Executor {
            workers,
            threadpool,
        }
    }

    pub fn execute(
        &self,
        graph: &Graph,
        results: &mut Vec<CmTypes>,
        arc_timebuf: Arc<Mutex<TimeBuffer>>,
        run_idx: usize,
    ) -> Duration {
        let num_stages = graph.len();

        // Initializations that need to be done from graph

        let fft_size = 10000;
        let mult_factor = graph.stage(0).node("FFT").mult_factor();

        let mut fft_buffers: Vec<Arc<Mutex<Fft>>> = Vec::new();
        for _ in 0..mult_factor {
            fft_buffers.push(Arc::new(Mutex::new(Fft::new(fft_size))));
        }

        let stage_results: Arc<Mutex<Vec<Vec<CmTypes>>>> = Arc::new(Mutex::new(vec![
                vec![CmTypes::None(); mult_factor];
                num_stages
            ]));
        let stage_scheduled: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(vec![0; num_stages]));
        let stage_completed: Arc<Mutex<Vec<Vec<usize>>>> =
            Arc::new(Mutex::new(vec![Vec::new(); num_stages]));

        let start_time = Instant::now();
        self.threadpool.install(|| {
            while stage_scheduled.lock().unwrap()[num_stages - 1] < mult_factor {
                for stage in 0..num_stages {
                    let scheduled = stage_scheduled.lock().unwrap();
                    if scheduled[stage] < mult_factor {
                        drop(scheduled);
                        let nodes_map = graph.stage(stage).nodes_map();
                        for (node_name, node) in nodes_map {
                            let node_args = node.task().args();

                            let func_opt = node.task().func_ptr();

                            if stage == 0 {
                                fft_buffers.par_iter().enumerate().for_each(
                                    |(index, fft_struct)| {
                                        let mut fft_struct = fft_struct.lock().unwrap();
                                        let t1 = Instant::now();
                                        fft_struct.computefft();
                                        let t2 = Instant::now();
                                        let mut tb = arc_timebuf.lock().unwrap();
                                        let worker_index = rayon::current_thread_index().unwrap();
                                        tb.add_time("FFT-Comp", run_idx, worker_index, t2 - t1);
                                        drop(tb);
                                        // task index at stage 0 is completed
                                        stage_completed.lock().unwrap()[stage].push(index);
                                        stage_scheduled.lock().unwrap()[stage] += 1;
                                    },
                                );
                            } else if node_args[0].arg_name() == "$ref" {
                                let dependencies: HashMap<String, Vec<usize>> = {
                                    let mut map = HashMap::new();
                                    let dependents = node.dependents();
                                    for dependent in dependents {
                                        let dep_node = graph.stage(dependent.1).node(&dependent.0);
                                        let successors_index =
                                            dep_node.successors_index()[&node_name.clone()].clone();
                                        map.insert(dep_node.name().clone(), successors_index);
                                    }
                                    map
                                };

                                let dependents_index: Vec<usize> =
                                    dependencies[&node.dependents()[0].0].clone();
                                let completed_indices: Vec<usize> =
                                    stage_completed.lock().unwrap()[stage - 1].clone();

                                let remaining = {
                                    let stage_finished = &stage_completed.lock().unwrap()[stage];
                                    let mut vec = Vec::new();
                                    for i in 0..mult_factor {
                                        if !stage_finished.contains(&i) {
                                            vec.push(i);
                                        }
                                    }
                                    vec
                                };

                                let mut arg_vecs = Vec::new();
                                for task_idx in remaining.iter() {
                                    let deps = {
                                        let mut vec = Vec::new();
                                        for i in dependents_index.iter() {
                                            let req_idx = (*task_idx as isize) - (*i as isize);
                                            vec.push(find_index(req_idx, mult_factor));
                                        }
                                        vec
                                    };

                                    // Check if all dependencies are present in completed_indices
                                    if deps.iter().all(|&dep| completed_indices.contains(&dep)) {
                                        let mut arg_vect = Vec::new();
                                        for dep in deps.iter() {
                                            let arg = CmTypes::VecC32(
                                                fft_buffers[*dep].lock().unwrap().get_buf(),
                                            );
                                            arg_vect.push(arg);
                                        }
                                        arg_vecs.push((arg_vect, *task_idx));
                                    }
                                }

                                arg_vecs.par_iter().for_each(|(arg_vec, index)| {
                                    let index = *index;
                                    let func = func_opt.unwrap();
                                    let t1_comp = Instant::now();
                                    let vecmat = func(arg_vec.to_vec());
                                    let t2_comp = Instant::now();
                                    let mut tb = arc_timebuf.lock().unwrap();
                                    let worker_index = rayon::current_thread_index().unwrap();
                                    tb.add_time(
                                        "VecMat-Comp",
                                        run_idx,
                                        worker_index,
                                        t2_comp - t1_comp,
                                    );
                                    drop(tb);
                                    // task index at stage 1 is completed
                                    stage_completed.lock().unwrap()[stage].push(index);
                                    stage_scheduled.lock().unwrap()[stage] += 1;
                                    stage_results.lock().unwrap()[stage][index] = vecmat;
                                });
                            } else if node_args[0].arg_name() == "$res" {
                                let dependencies: HashMap<String, Vec<usize>> = {
                                    let mut map = HashMap::new();
                                    let dependents = node.dependents();
                                    for dependent in dependents {
                                        let dep_node = graph.stage(dependent.1).node(&dependent.0);
                                        let successors_index =
                                            dep_node.successors_index()[&node_name.clone()].clone();
                                        map.insert(dep_node.name().clone(), successors_index);
                                    }
                                    map
                                };

                                let dependents_index: Vec<usize> = dependencies["Vec2Mat"].clone();

                                let remaining = {
                                    let stage_finished = &stage_completed.lock().unwrap()[stage];
                                    let mut vec = Vec::new();
                                    for i in 0..mult_factor {
                                        if !stage_finished.contains(&i) {
                                            vec.push(i);
                                        }
                                    }
                                    vec
                                };

                                let mut arg_vecs = Vec::new();
                                let completed_indices: Vec<usize> =
                                    stage_completed.lock().unwrap()[stage - 1].clone();

                                for task_idx in remaining.iter() {
                                    let deps = {
                                        let mut vec = Vec::new();
                                        for i in dependents_index.iter() {
                                            let req_idx = (*task_idx as isize) - (*i as isize);
                                            vec.push(find_index(req_idx, mult_factor));
                                        }
                                        vec
                                    };

                                    // Check if all dependencies are present in completed_indices
                                    if deps.iter().all(|&dep| completed_indices.contains(&dep)) {
                                        let mut arg_vect = Vec::new();
                                        let t1_clone = Instant::now();
                                        for dep in deps.iter() {
                                            let vecmat = stage_results.lock().unwrap()[stage - 1]
                                                [*dep]
                                                .clone();
                                            let res = match vecmat {
                                                CmTypes::DMatrixC32(x) => x,
                                                _ => panic!("Invalid return type"),
                                            };

                                            let arg = CmTypes::DMatrixC32(res.clone());
                                            arg_vect.push(arg);
                                        }
                                        let t2_clone = Instant::now();
                                        let mut tb = arc_timebuf.lock().unwrap();
                                        tb.add_time(
                                            "Stage2-Clone",
                                            run_idx,
                                            0,
                                            t2_clone - t1_clone,
                                        );
                                        drop(tb);
                                        arg_vecs.push((arg_vect, *task_idx));
                                    }
                                }

                                arg_vecs.par_iter().for_each(|(arg_vec, index)| {
                                    let index = *index;
                                    let worker_index = rayon::current_thread_index().unwrap();
                                    let func = func_opt.unwrap();
                                    let t1_comp = Instant::now();
                                    let cmat = func(arg_vec.to_vec());
                                    let t2_comp = Instant::now();
                                    let mut tb = arc_timebuf.lock().unwrap();
                                    tb.add_time(
                                        "CGEMM-Comp",
                                        run_idx,
                                        worker_index,
                                        t2_comp - t1_comp,
                                    );
                                    drop(tb);
                                    // task index at stage 2 is completed
                                    stage_completed.lock().unwrap()[stage].push(index);
                                    stage_scheduled.lock().unwrap()[stage] += 1;
                                    stage_results.lock().unwrap()[stage][index] = cmat;
                                });
                            }
                        }
                    }
                }
            }
        });
        // Collect results from last stage
        let t1 = Instant::now();
        let last_stage_results = &stage_results.lock().unwrap()[num_stages - 1];
        for i in 0..mult_factor {
            // pushing Arc references
            results.push(last_stage_results[i].clone());
        }
        let t2 = Instant::now();
        let mut tb = arc_timebuf.lock().unwrap();
        tb.add_time("CmRetrieve", run_idx, 0, t2 - t1);
        drop(tb);
        let end_time = Instant::now();
        end_time - start_time
    }
}
