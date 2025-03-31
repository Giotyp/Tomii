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
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_channel::{bounded, Receiver, Sender};

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

        let mut fft_buffers: Vec<Arc<Mutex<Fft>>> = Vec::with_capacity(mult_factor);
        for _ in 0..mult_factor {
            fft_buffers.push(Arc::new(Mutex::new(Fft::new(fft_size))));
        }

        let stage_results: Arc<Mutex<Vec<Vec<CmTypes>>>> = Arc::new(Mutex::new(vec![
                vec![CmTypes::None(); mult_factor];
                num_stages
            ]));
        let stage_scheduled: Arc<Mutex<Vec<Vec<usize>>>> =
            Arc::new(Mutex::new(vec![Vec::new(); num_stages]));
        let stage_completed: Arc<Mutex<Vec<Vec<usize>>>> =
            Arc::new(Mutex::new(vec![Vec::new(); num_stages]));

        // Create a channel for task submission
        let (task_sender, task_receiver) =
            bounded::<Box<dyn FnOnce() + Send>>(num_stages * mult_factor);

        // Atomic counter to track task completion
        let task_counter = Arc::new(AtomicUsize::new(0));
        self.task_thread(task_receiver, task_counter.clone());

        let start_time = Instant::now();
        while stage_scheduled.lock().unwrap()[num_stages - 1].len() < mult_factor {
            for stage in 0..num_stages {
                let scheduled = stage_scheduled.lock().unwrap();
                if scheduled[stage].len() < mult_factor {
                    drop(scheduled);

                    // Check if any task from the previous stage is completed
                    let completed = stage_completed.lock().unwrap();
                    if stage > 0 && completed[stage - 1].len() == 0 {
                        continue;
                    }
                    drop(completed);

                    let nodes_map = graph.stage(stage).nodes_map();
                    for (node_name, node) in nodes_map {
                        let node_args = node.task().args();

                        let func_opt = node.task().func_ptr();

                        if stage == 0 {
                            self.execute_fft_tasks(
                                &fft_buffers,
                                stage_completed.clone(),
                                stage_scheduled.clone(),
                                arc_timebuf.clone(),
                                run_idx,
                                stage,
                                &task_sender,
                                task_counter.clone(),
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
                            let scheduled_indices = stage_scheduled.lock().unwrap()[stage].clone();

                            let remaining = {
                                let stage_finished = &stage_completed.lock().unwrap()[stage];
                                let mut vec = Vec::new();
                                for i in 0..mult_factor {
                                    if !stage_finished.contains(&i)
                                        && !scheduled_indices.contains(&i)
                                    {
                                        vec.push(i);
                                    }
                                }
                                vec
                            };

                            if remaining.is_empty() {
                                continue;
                            }

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

                            let tasks = self.execute_vecmat_tasks(
                                arg_vecs,
                                func_opt.unwrap(),
                                stage_results.clone(),
                                stage_completed.clone(),
                                stage_scheduled.clone(),
                                arc_timebuf.clone(),
                                run_idx,
                                stage,
                                &task_sender,
                                task_counter.clone(),
                            );
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
                            let completed_indices: Vec<usize> =
                                stage_completed.lock().unwrap()[stage - 1].clone();
                            let scheduled_indices = stage_scheduled.lock().unwrap()[stage].clone();

                            let remaining = {
                                let stage_finished = &stage_completed.lock().unwrap()[stage];
                                let mut vec = Vec::new();
                                for i in 0..mult_factor {
                                    if !stage_finished.contains(&i)
                                        && !scheduled_indices.contains(&i)
                                    {
                                        vec.push(i);
                                    }
                                }
                                vec
                            };

                            if remaining.is_empty() {
                                continue;
                            }

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

                                let mut count = 0;
                                let t1_clone = Instant::now();
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
                                    count += 1;
                                    arg_vecs.push((arg_vect, *task_idx));
                                }
                                let t2_clone = Instant::now();
                                if count > 0 {
                                    let mut tb = arc_timebuf.lock().unwrap();
                                    tb.add_time("Stage2-Clone", run_idx, 0, t2_clone - t1_clone);
                                }
                                drop(tb);
                            }

                            let tasks = self.execute_cgemm_tasks(
                                arg_vecs,
                                func_opt.unwrap(),
                                stage_results.clone(),
                                stage_completed.clone(),
                                stage_scheduled.clone(),
                                arc_timebuf.clone(),
                                run_idx,
                                stage,
                                &task_sender,
                                task_counter.clone(),
                            );
                        }
                    }
                }
            }
        }
        // Wait for all tasks to complete
        while task_counter.load(Ordering::SeqCst) > 0 {
            std::thread::sleep(Duration::from_micros(1));
        }

        // Collect results from last stage
        let t1 = Instant::now();
        let last_stage_results = &stage_results.lock().unwrap()[num_stages - 1];
        for i in 0..mult_factor {
            // pushing Arc references
            let res = last_stage_results[i].clone();
            results.push(res);
        }
        let t2 = Instant::now();
        let mut tb = arc_timebuf.lock().unwrap();
        tb.add_time("CmRetrieve", run_idx, 0, t2 - t1);
        drop(tb);
        let end_time = Instant::now();
        end_time - start_time
    }

    fn task_thread(
        &self,
        task_receiver: Receiver<Box<dyn FnOnce() + Send>>,
        task_counter: Arc<AtomicUsize>,
    ) {
        // Spawn Rayon tasks to process incoming work
        self.threadpool.spawn(move || {
            task_receiver.into_iter().par_bridge().for_each(|task| {
                task();
                task_counter.fetch_sub(1, Ordering::SeqCst);
            });
        });
    }

    fn execute_fft_tasks(
        &self,
        fft_buffers: &[Arc<Mutex<Fft>>],
        stage_completed: Arc<Mutex<Vec<Vec<usize>>>>,
        stage_scheduled: Arc<Mutex<Vec<Vec<usize>>>>,
        arc_timebuf: Arc<Mutex<TimeBuffer>>,
        run_idx: usize,
        stage: usize,
        task_sender: &Sender<Box<dyn FnOnce() + Send>>,
        task_counter: Arc<AtomicUsize>,
    ) {
        let mut tasks = Vec::new();

        for (index, fft_struct) in fft_buffers.iter().enumerate() {
            let fft_struct = Arc::clone(fft_struct);
            let arc_timebuf = arc_timebuf.clone();
            let stage_completed = stage_completed.clone();
            stage_scheduled.lock().unwrap()[stage].push(index);

            let task = move || {
                let mut fft_struct = fft_struct.lock().unwrap();

                let worker_index = rayon::current_thread_index().unwrap_or(0);
                let t1 = Instant::now();
                fft_struct.computefft();
                let t2 = Instant::now();

                let mut tb = arc_timebuf.lock().unwrap();
                tb.add_time("FFT-Comp", run_idx, worker_index, t2 - t1);
                drop(tb);
                stage_completed.lock().unwrap()[stage].push(index);
            };

            tasks.push(task);
        }

        for task in tasks {
            task_counter.fetch_add(1, Ordering::SeqCst);
            task_sender.send(Box::new(task)).unwrap();
        }
    }

    fn execute_vecmat_tasks(
        &self,
        arg_vecs: Vec<(Vec<CmTypes>, usize)>,
        func: fn(Vec<CmTypes>) -> CmTypes,
        stage_results: Arc<Mutex<Vec<Vec<CmTypes>>>>,
        stage_completed: Arc<Mutex<Vec<Vec<usize>>>>,
        stage_scheduled: Arc<Mutex<Vec<Vec<usize>>>>,
        arc_timebuf: Arc<Mutex<TimeBuffer>>,
        run_idx: usize,
        stage: usize,
        task_sender: &Sender<Box<dyn FnOnce() + Send>>,
        task_counter: Arc<AtomicUsize>,
    ) {
        let mut tasks = Vec::new();

        for (arg_vec, index) in arg_vecs {
            let arc_timebuf = arc_timebuf.clone();
            let stage_results = stage_results.clone();
            let stage_completed = stage_completed.clone();
            stage_scheduled.lock().unwrap()[stage].push(index);

            let task = move || {
                let worker_index = rayon::current_thread_index().unwrap_or(0);
                let t1_comp = Instant::now();
                let vecmat = func(arg_vec);
                let t2_comp = Instant::now();

                let mut tb = arc_timebuf.lock().unwrap();
                tb.add_time("VecMat-Comp", run_idx, worker_index, t2_comp - t1_comp);
                drop(tb);

                stage_completed.lock().unwrap()[stage].push(index);
                stage_results.lock().unwrap()[stage][index] = vecmat;
            };

            tasks.push(task);
        }

        for task in tasks {
            task_counter.fetch_add(1, Ordering::SeqCst);
            task_sender.send(Box::new(task)).unwrap();
        }
    }

    fn execute_cgemm_tasks(
        &self,
        arg_vecs: Vec<(Vec<CmTypes>, usize)>,
        func: fn(Vec<CmTypes>) -> CmTypes,
        stage_results: Arc<Mutex<Vec<Vec<CmTypes>>>>,
        stage_completed: Arc<Mutex<Vec<Vec<usize>>>>,
        stage_scheduled: Arc<Mutex<Vec<Vec<usize>>>>,
        arc_timebuf: Arc<Mutex<TimeBuffer>>,
        run_idx: usize,
        stage: usize,
        task_sender: &Sender<Box<dyn FnOnce() + Send>>,
        task_counter: Arc<AtomicUsize>,
    ) {
        let mut tasks = Vec::new();

        for (arg_vec, index) in arg_vecs {
            let arc_timebuf = arc_timebuf.clone();
            let stage_results = stage_results.clone();
            let stage_completed = stage_completed.clone();
            stage_scheduled.lock().unwrap()[stage].push(index);

            let task = move || {
                let worker_index = rayon::current_thread_index().unwrap_or(0);
                let t1_comp = Instant::now();
                let cmat = func(arg_vec);
                let t2_comp = Instant::now();

                let mut tb = arc_timebuf.lock().unwrap();
                tb.add_time("CGEMM-Comp", run_idx, worker_index, t2_comp - t1_comp);
                drop(tb);

                stage_completed.lock().unwrap()[stage].push(index);
                stage_results.lock().unwrap()[stage][index] = cmat;
            };

            tasks.push(task);
        }

        for task in tasks {
            task_counter.fetch_add(1, Ordering::SeqCst);
            task_sender.send(Box::new(task)).unwrap();
        }
    }
}
