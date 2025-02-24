#![allow(unused_imports)]
#![allow(dead_code)]
use core_affinity;
use rayon::prelude::*;
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::sync::{Arc, Mutex};

use crate::graph_struct::*;
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

    pub fn execute(&self, graph: &Graph, results: &mut Vec<CmTypes>) -> u64 {
        let num_stages = graph.len();

        // Initializations that need to be done from graph

        let mut stage_dict: Vec<Vec<HashMap<String, String>>> = vec![Vec::new(); num_stages];

        for i in 0..num_stages {
            let mut stage_vec = vec![];
            let node_names = graph.stage(i).node_names().clone();
            for node in node_names {
                let info = graph.node_info(i, &node);
                let task_dict = HashMap::from([
                    ("name".to_string(), node.clone()),
                    ("mult_factor".to_string(), info["mult_factor"].clone()),
                    ("function_name".to_string(), info["function_name"].clone()),
                    (
                        "successors_index".to_string(),
                        info["successors_index"].clone(),
                    ),
                    (
                        "successors_names".to_string(),
                        info["successors_names"].clone(),
                    ),
                    (
                        "dependents_names".to_string(),
                        info["dependents_names"].clone(),
                    ),
                    ("args".to_string(), info["args"].clone()),
                ]);
                stage_vec.push(task_dict);
            }
            stage_dict[i] = stage_vec;
        }

        let fft_size = 10000;
        let mult_factor = stage_dict[0][0]["mult_factor"].parse::<usize>().unwrap();

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

        let start_time = rdtsc();
        self.threadpool.install(|| {
            while stage_completed.lock().unwrap()[num_stages - 1].len() < mult_factor {
                for stage in 0..num_stages {
                    let scheduled = stage_scheduled.lock().unwrap();
                    if scheduled[stage] < mult_factor {
                        drop(scheduled);
                        for nodes in stage_dict[stage].iter() {
                            let node_args = graph
                                .stage(stage)
                                .node(&nodes["name"])
                                .unwrap()
                                .read()
                                .unwrap()
                                .task()
                                .args()
                                .clone();

                            let func_opt = graph
                                .stage(stage)
                                .node(&nodes["name"])
                                .unwrap()
                                .read()
                                .unwrap()
                                .task()
                                .func_ptr();

                            if stage == 0 {
                                fft_buffers.par_iter().enumerate().for_each(
                                    |(index, fft_struct)| {
                                        let mut fft_struct = fft_struct.lock().unwrap();
                                        fft_struct.computefft();
                                        // task index at stage 0 is completed
                                        stage_completed.lock().unwrap()[stage].push(index);
                                        stage_scheduled.lock().unwrap()[stage] += 1;
                                    },
                                );
                            } else if node_args[0].arg_name() == "$ref" {
                                let dependents_index: Vec<usize> = stage_dict[stage - 1][0]
                                    ["successors_index"]
                                    .split(',')
                                    .map(|x| x.trim().parse::<usize>().unwrap())
                                    .collect();

                                let completed_indices: Vec<usize> =
                                    stage_completed.lock().unwrap()[stage - 1].clone();

                                let mut arg_vecs = Vec::new();
                                for task_idx in 0..mult_factor {
                                    let deps = {
                                        let mut vec = Vec::new();
                                        for i in dependents_index.iter() {
                                            let req_idx = (task_idx as isize) - (*i as isize);
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
                                        arg_vecs.push((arg_vect, task_idx));
                                    }
                                }

                                arg_vecs.par_iter().for_each(|(arg_vec, index)| {
                                    let index = *index;
                                    let func = func_opt.unwrap();
                                    let vecmat = func(arg_vec.to_vec());
                                    // task index at stage 1 is completed
                                    stage_completed.lock().unwrap()[stage].push(index);
                                    stage_scheduled.lock().unwrap()[stage] += 1;
                                    stage_results.lock().unwrap()[stage][index] = vecmat;
                                });
                            } else if node_args[0].arg_name() == "$res" {
                                let dependents_index: Vec<usize> = {
                                    let indexes =
                                        stage_dict[stage - 1][0]["successors_index"].clone();

                                    // check if ',' is present in indexes
                                    if indexes.contains(',') {
                                        indexes
                                            .split(',')
                                            .map(|x| x.trim().parse::<usize>().unwrap())
                                            .collect()
                                    } else if indexes.contains('-') {
                                        // - is present so range of nodes is required
                                        let mut vec = Vec::new();
                                        let range: Vec<usize> = indexes
                                            .split('-')
                                            .map(|x| x.trim().parse::<usize>().unwrap())
                                            .collect();
                                        for i in range[0]..(range[1]+1) {
                                            vec.push(i);
                                        }
                                        vec
                                    } else {
                                        vec![indexes.trim().parse::<usize>().unwrap()]
                                    }
                                };

                                let mut arg_vecs = Vec::new();
                                let completed_indices: Vec<usize> =
                                    stage_completed.lock().unwrap()[stage - 1].clone();

                                for task_idx in 0..mult_factor {
                                    let deps = {
                                        let mut vec = Vec::new();
                                        for i in dependents_index.iter() {
                                            let req_idx = (task_idx as isize) - (*i as isize);
                                            vec.push(find_index(req_idx, mult_factor));
                                        }
                                        vec
                                    };

                                    // Check if all dependencies are present in completed_indices
                                    if deps.iter().all(|&dep| completed_indices.contains(&dep)) {
                                        let mut arg_vect = Vec::new();
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
                                        arg_vecs.push((arg_vect, task_idx));
                                    }
                                }
                                arg_vecs.par_iter().for_each(|(arg_vec, index)| {
                                    let index = *index;
                                    let func = func_opt.unwrap();
                                    let cmat = func(arg_vec.to_vec());
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
        for i in 0..mult_factor {
            results.push(stage_results.lock().unwrap()[num_stages - 1][i].clone());
        }
        let end_time = rdtsc();
        end_time - start_time
    }
}
