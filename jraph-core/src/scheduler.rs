#![allow(unused_imports)]
#![allow(dead_code)]
use core_affinity;
use rayon::{prelude::*, vec};
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::future::ready;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::cmtypes::CmTypes;
use crate::graph_struct::*;
use crate::time_buffer::TimeBuffer;
use std::collections::HashMap;
use std::hint::spin_loop;

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

fn get_thread_idx() -> usize {
    rayon::current_thread_index().unwrap_or(0)
}

fn spawn_task<F>(threadpool: &ThreadPool, task_clos: F, task_counter: Arc<AtomicUsize>)
where
    F: FnOnce() + Send + 'static,
{
    // Spawn Rayon tasks to process the given closure
    threadpool.spawn(move || {
        task_clos();
        task_counter.fetch_sub(1, Ordering::SeqCst);
    });
}

struct StageBuffer<T> {
    buffer: Vec<HashMap<String, Vec<T>>>,
}
impl<T> StageBuffer<T> {
    fn new(graph: &Graph) -> StageBuffer<T> {
        let buffer = (0..graph.len())
            .map(|stage_i| {
                graph
                    .stage(stage_i)
                    // Get the nodes map for each stage
                    .nodes_map()
                    .keys()
                    .cloned()
                    // Create an empty Vec for each node
                    .map(|name| (name, Vec::new()))
                    .collect()
            })
            .collect();
        StageBuffer { buffer }
    }

    fn new_empty() -> StageBuffer<T> {
        StageBuffer { buffer: Vec::new() }
    }

    fn new_mfval(graph: &Graph, init_val: T) -> StageBuffer<T>
    where
        T: Clone,
    {
        let mut buffer = Vec::new();
        for i in 0..graph.len() {
            let mut map = HashMap::new();
            // Get the nodes map for each stage
            let nodes_map = graph.stage(i).nodes_map();
            // iterate over the nodes map to create a vector for each node
            for (node_name, node) in nodes_map {
                let mult_factor = node.mult_factor();
                let new_vec = vec![init_val.clone(); mult_factor];
                map.insert(node_name.clone(), new_vec);
            }
            buffer.push(map);
        }
        StageBuffer { buffer }
    }

    fn search_node(&self, node_name: &str) -> Option<Vec<T>>
    where
        T: Clone,
    {
        for stage in &self.buffer {
            if let Some(indices) = stage.get(node_name) {
                return Some(indices.to_vec());
            }
        }
        None
    }

    fn search_node_idx(&self, node_name: &str, index: usize) -> Option<T>
    where
        T: Clone,
    {
        for stage in &self.buffer {
            if let Some(indices) = stage.get(node_name) {
                if index < indices.len() {
                    return Some(indices[index].clone());
                }
            }
        }
        None
    }

    fn add_elem(&mut self, stage: usize, node_name: String, elem: T) {
        let stage_buffer = &mut self.buffer[stage];
        if let Some(idx_vec) = stage_buffer.get_mut(&node_name) {
            idx_vec.push(elem);
        } else {
            stage_buffer.insert(node_name, vec![elem]);
        }
    }

    fn stage_buffer(&self, stage: usize) -> &HashMap<String, Vec<T>> {
        &self.buffer[stage]
    }
    fn stage_buffer_mut(&mut self, stage: usize) -> &mut HashMap<String, Vec<T>> {
        &mut self.buffer[stage]
    }

    fn clear_buffer(&mut self) {
        for stage in &mut self.buffer {
            for (_, idx_vec) in stage {
                idx_vec.clear();
            }
        }
    }

    fn stage_total(&self, stage: usize) -> usize {
        let stage_buffer = &self.buffer[stage];
        let mut total = 0;
        for (_, idx_vec) in stage_buffer {
            total += idx_vec.len();
        }
        total
    }

    fn total(&self) -> usize {
        let mut total = 0;
        for i in 0..self.buffer.len() {
            total += self.stage_total(i);
        }
        total
    }
}

pub struct Scheduler {
    workers: usize,
    threadpool: ThreadPool,
    stage_results: Arc<Mutex<StageBuffer<CmTypes>>>,
}

// Public API
impl Scheduler {
    pub fn new(core_offset: usize, workers: usize) -> Scheduler {
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

        let stage_results = Arc::new(Mutex::new(StageBuffer::new_empty()));

        Scheduler {
            workers,
            threadpool,
            stage_results,
        }
    }

    pub fn get_stage_results(&self, stage_no: usize) -> HashMap<String, Vec<CmTypes>> {
        let stage_results = self.stage_results.lock().unwrap();
        stage_results.stage_buffer(stage_no).clone()
    }

    pub fn clear_stage_results(&mut self) {
        let mut stage_results = self.stage_results.lock().unwrap();
        stage_results.clear_buffer();
    }

    pub fn schedule(
        &mut self,
        graph: &Graph,
        arc_timebuf: Arc<Mutex<TimeBuffer>>,
        run_idx: usize,
    ) -> Duration {
        let num_stages = graph.len();
        let init_objects_opt = graph.init_objects();
        let last_stage_nodes = graph.stage(num_stages - 1).total_nodes();

        let mut stage_scheduled = StageBuffer::<usize>::new(graph);
        let stage_completed = StageBuffer::<usize>::new(graph);
        let stage_completed = Arc::new(Mutex::new(stage_completed));

        let stage_results = StageBuffer::<CmTypes>::new_mfval(graph, CmTypes::None());
        self.stage_results = Arc::new(Mutex::new(stage_results));

        // Get dependencies for each node in each stage
        let node_dependencies_vecmap: Vec<HashMap<String, HashMap<String, Vec<usize>>>> =
            graph.node_dependencies_vecmap();

        // Atomic counter to track task completion
        let task_counter = Arc::new(AtomicUsize::new(0));

        let start_time = Instant::now();
        while stage_scheduled.stage_total(num_stages - 1) < last_stage_nodes {
            for stage in 0..num_stages {
                let stage_nodes = graph.stage(stage).total_nodes();

                if stage_scheduled.stage_total(stage) < stage_nodes {
                    // Check if any task from the previous stage is completed
                    let completed = stage_completed.lock().unwrap();
                    if stage > 0 && completed.stage_total(stage - 1) == 0 {
                        continue;
                    }
                    drop(completed);

                    let nodes_map = graph.stage(stage).nodes_map();
                    for (node_name, node) in nodes_map {
                        let node_args = node.task().args();
                        let mult_factor = node.mult_factor();
                        let ref_task_opt = node.task().ref_tasks();

                        let time_key = format!("{}-{}", stage, node_name);

                        let func_opt = node.task().func_ptr();
                        let func = func_opt.unwrap();

                        if stage == 0 {
                            for index in 0..mult_factor {
                                let mut arg_vec = Vec::new();

                                for arg in node_args.iter() {
                                    match arg {
                                        CmTypes::Ref(obj_name) => {
                                            let init_objects = init_objects_opt.as_ref().unwrap();
                                            let obj = &init_objects[obj_name][index];
                                            arg_vec.push(obj.clone());
                                        }
                                        _ => {
                                            arg_vec.push(arg.clone());
                                        }
                                    }
                                }

                                stage_scheduled.add_elem(stage, node_name.clone(), index);

                                self.enqueue_task(
                                    stage,
                                    node_name.clone(),
                                    index,
                                    arg_vec,
                                    func,
                                    time_key.clone(),
                                    run_idx,
                                    arc_timebuf.clone(),
                                    stage_completed.clone(),
                                    self.stage_results.clone(),
                                    task_counter.clone(),
                                );
                            }
                        } else {
                            // Check if there are remaining tasks for the current node
                            let scheduled_indices: &Vec<usize> =
                                &stage_scheduled.stage_buffer(stage)[node_name];

                            let node_remaining: Vec<usize> = {
                                let mut rem = Vec::new();
                                let completed_lock = stage_completed.lock().unwrap();
                                let node_finished: &Vec<usize> =
                                    &completed_lock.stage_buffer(stage)[node_name];
                                for i in 0..mult_factor {
                                    if !node_finished.contains(&i)
                                        && !scheduled_indices.contains(&i)
                                    {
                                        rem.push(i);
                                    }
                                }
                                rem
                            };

                            if node_remaining.is_empty() {
                                continue;
                            }

                            // Access dependencies for the current node
                            let dependencies: &HashMap<String, Vec<usize>> =
                                &node_dependencies_vecmap[stage][node_name];

                            let mut arg_vecs: Vec<(Vec<CmTypes>, usize)> = Vec::new();
                            for task_idx in node_remaining.iter() {
                                let deps: HashMap<String, Vec<usize>> = dependencies
                                    .iter()
                                    .map(|(dep_name, idxs)| {
                                        // use find_index to correctly correspond the
                                        // dependent indexes to the current task
                                        let transformed = idxs
                                            .iter()
                                            .map(|&i| {
                                                let req_idx = (*task_idx as isize) - (i as isize);
                                                find_index(req_idx, mult_factor)
                                            })
                                            .collect::<Vec<_>>();
                                        (dep_name.clone(), transformed)
                                    })
                                    .collect();

                                // All indexes for each key in deps should also be present in
                                // completed_indices
                                let mut ready_to_schedule: bool = true;

                                for (dep_node, dep_idx) in deps.iter() {
                                    let complete_lock = stage_completed.lock().unwrap();
                                    let completed_indices_opt: Option<Vec<usize>> =
                                        complete_lock.search_node(dep_node);
                                    drop(complete_lock);

                                    if let Some(completed_indices) = completed_indices_opt {
                                        // all dep_idxes should be present in completed_indices
                                        for dep in dep_idx.iter() {
                                            if !completed_indices.contains(dep) {
                                                ready_to_schedule = false;
                                                break;
                                            }
                                        }
                                    } else {
                                        // dep_node is not present in completed_indices
                                        ready_to_schedule = false;
                                    }
                                    if !ready_to_schedule {
                                        break;
                                    }
                                }

                                // Retrieve arguments from dependencies
                                if ready_to_schedule {
                                    let mut arg_vec: Vec<CmTypes> = Vec::new();
                                    let mut ref_task_count = 0;

                                    for arg in node_args.iter() {
                                        match arg {
                                            CmTypes::Ref(obj_name) => {
                                                // Check for reference task
                                                if let Some(ref_task_vec) = ref_task_opt {
                                                    let task_key = &ref_task_vec[ref_task_count];
                                                    ref_task_count += 1;

                                                    let indices = &deps[&task_key.clone()];
                                                    for dep in indices.iter() {
                                                        let init_objects =
                                                            init_objects_opt.as_ref().unwrap();
                                                        let obj =
                                                            &init_objects[&obj_name.clone()][*dep];
                                                        arg_vec.push(obj.clone());
                                                    }
                                                } else {
                                                    let init_objects =
                                                        init_objects_opt.as_ref().unwrap();
                                                    let obj = &init_objects[obj_name][*task_idx];
                                                    arg_vec.push(obj.clone());
                                                }
                                            }
                                            CmTypes::Res(res_node) => {
                                                let indices = &deps[res_node];
                                                for dep in indices.iter() {
                                                    // for each task index, retrieve the
                                                    // corresponding results
                                                    // (must exist since they are completed)
                                                    let res_lock =
                                                        self.stage_results.lock().unwrap();

                                                    let result = res_lock
                                                        .search_node_idx(&res_node, *dep)
                                                        .unwrap();
                                                    drop(res_lock);
                                                    arg_vec.push(result);
                                                }
                                            }
                                            _ => {
                                                arg_vec.push(arg.clone());
                                            }
                                        }
                                    }
                                    arg_vecs.push((arg_vec, *task_idx));
                                }
                            }

                            // Schedule all remaining tasks
                            for (arg_vec, index) in arg_vecs {
                                stage_scheduled.add_elem(stage, node_name.clone(), index);

                                self.enqueue_task(
                                    stage,
                                    node_name.clone(),
                                    index,
                                    arg_vec,
                                    func,
                                    time_key.clone(),
                                    run_idx,
                                    arc_timebuf.clone(),
                                    stage_completed.clone(),
                                    self.stage_results.clone(),
                                    task_counter.clone(),
                                );
                            }
                        }
                    }
                }
            }
        }
        // Wait for all tasks to complete
        while task_counter.load(Ordering::SeqCst) > 0 {
            spin_loop();
        }
        let end_time = Instant::now();
        let total_done = stage_completed.lock().unwrap().stage_total(num_stages - 1);
        assert_eq!(total_done, last_stage_nodes);
        end_time - start_time
    }

    fn enqueue_task(
        &self,
        stage: usize,
        node_name: String,
        index: usize,
        arg_vec: Vec<CmTypes>,
        func: fn(Vec<CmTypes>) -> CmTypes,
        time_key: String,
        run_idx: usize,
        arc_timebuf: Arc<Mutex<TimeBuffer>>,
        stage_completed: Arc<Mutex<StageBuffer<usize>>>,
        stage_results: Arc<Mutex<StageBuffer<CmTypes>>>,
        task_counter: Arc<AtomicUsize>,
    ) {
        let task = move || {
            let worker = get_thread_idx();
            let t1 = Instant::now();
            let res = func(arg_vec);
            let t2 = Instant::now();

            // Time function
            {
                let mut tb = arc_timebuf.lock().unwrap();
                tb.add_time(&time_key, run_idx, worker, t2 - t1);
            }
            // store result
            {
                let mut res_lock = stage_results.lock().unwrap();
                let slot = res_lock
                    .stage_buffer_mut(stage)
                    .get_mut(&node_name)
                    .unwrap();
                slot[index] = res;
                drop(res_lock);
            }
            // mark completed
            {
                let mut comp_lock = stage_completed.lock().unwrap();
                comp_lock.add_elem(stage, node_name.clone(), index);
                drop(comp_lock);
            }
        };

        task_counter.fetch_add(1, Ordering::SeqCst);
        spawn_task(&self.threadpool, task, task_counter.clone());
    }
}
