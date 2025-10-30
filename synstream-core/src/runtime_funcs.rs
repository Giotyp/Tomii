use crate::debug::print_debug;
use crate::time_buffer::TimeBufferManager;
use crate::{buffers::*, graph::*, graph_struct::*, scheduler::*, IdType};
use crossbeam_channel::Sender;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use synstream_types::*;

/// Cache entry for quick node access - stores commonly accessed node fields
#[derive(Clone)]
pub struct NodeCacheEntry {
    pub factor: usize,
    pub name: String,
    pub func_ptr: CmPtr,
    pub arg_cache: ArgCacheEntry,
}

#[derive(Clone)]
pub struct ArgCacheEntry {
    // initially store ref indexes for node id
    pub args: Vec<CmTypes>,
    // indexes of buffer ref in args
    pub buffer_ref_indexes: Vec<usize>,
    // buffer values
    pub buffer_values: Vec<Vec<CmTypes>>,
    // indexes of $ref::index in args
    pub rt_idxs_indexes: Vec<usize>,
    // indexes of $ref::worker in args
    pub rt_workers_indexes: Vec<usize>,
    // indexes of $res in args
    pub res_indexes: Vec<usize>,
    // real indexes of $res
    pub real_res_indexes: Vec<usize>,
}

impl std::fmt::Debug for ArgCacheEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArgCacheEntry")
            .field("args", &self.args)
            .field("buffer_ref_indexes", &self.buffer_ref_indexes)
            .field("buffer_values", &self.buffer_values)
            .field("rt_idxs_indexes", &self.rt_idxs_indexes)
            .field("rt_workers_indexes", &self.rt_workers_indexes)
            .field("res_indexes", &self.res_indexes)
            .field("real_res_indexes", &self.real_res_indexes)
            .finish()
    }
}

pub fn node_cache_entry(node: &Node, init_objects: &Vec<Vec<CmTypes>>) -> NodeCacheEntry {
    let mut rt_idxs_indexes = Vec::new();
    let mut buffer_ref_indexes = Vec::new();
    let mut buffer_values = Vec::new();
    let mut rt_workers_indexes = Vec::new();
    let mut real_res_indexes = Vec::new();
    let mut res_indexes = Vec::new();
    let mut args = vec![CmTypes::None; node.args.len()];

    let mut idx_count = 0;

    for (idx, arg) in node.args.iter().enumerate() {
        if arg.is_condition() {
            continue;
        }
        match &arg.type_ {
            CmTypes::Ref(obj_id) => {
                if *obj_id == 0 {
                    // Reserved for $index
                    rt_idxs_indexes.push(idx_count);
                } else if *obj_id == 1 {
                    // Reserved for $workers
                    rt_workers_indexes.push(idx_count);
                } else {
                    // For init_object values
                    let obj_vec = &init_objects[*obj_id];
                    if obj_vec.len() > 1 {
                        // If the object is a buffer, we need node_index
                        buffer_ref_indexes.push(idx_count);
                        buffer_values.push(obj_vec.clone());
                    } else {
                        // If the object is a variable, get the first element
                        args[idx_count] = obj_vec[0].clone()
                    }
                }
            }
            CmTypes::Res(_) => {
                res_indexes.push(idx_count);
                real_res_indexes.push(idx);
            }
            CmTypes::Barrier(_) => { //ignore
            }
            _ => {
                args[idx_count] = arg.type_.clone();
            }
        }
        idx_count += 1;
    }

    let arg_cache = ArgCacheEntry {
        args,
        buffer_ref_indexes,
        buffer_values,
        rt_idxs_indexes,
        rt_workers_indexes,
        res_indexes,
        real_res_indexes,
    };

    NodeCacheEntry {
        factor: node.factor,
        name: node.name.clone(),
        func_ptr: node.func_ptr.expect("Node function pointer is None"),
        arg_cache,
    }
}

/// Shared data across all SynStream threads - immutable or internally synchronized
pub struct SharedData {
    // Immutable data
    pub graph: Graph,
    pub slots: usize,
    pub max_streams: usize,
    pub max_runtime: Option<u64>,

    // Node cache for fast repeated access
    pub node_cache: Vec<NodeCacheEntry>,

    // Internally synchronized data
    pub node_results: Arc<RwLock<VecMap<CmTypes>>>,
    pub stream_complete_counter: Arc<AtomicUsize>,
    pub available_stream_slots: Arc<RwLock<Vec<usize>>>,
    pub time_buffer: Arc<TimeBufferManager>,

    // Shared between threads
    pub scheduler: Arc<RwLock<Option<Arc<SchedulerImpl>>>>,
    pub completed_tx: Arc<RwLock<Option<Sender<(NodeInfo, CmTypes)>>>>,
    pub workers: Arc<AtomicUsize>,
}

#[inline(always)]
fn execute_task(
    func: CmPtr,
    arg_vec: Vec<CmTypes>,
    node_info: NodeInfo,
    time_buf: &Arc<TimeBufferManager>,
    node_name: &str,
    completed_tx: &Sender<(NodeInfo, CmTypes)>,
) {
    let start_time = if !node_info.post_node {
        Some(time_buf.measure_time())
    } else {
        None
    };

    let result = func(arg_vec);

    if let Some(start) = start_time {
        let end_time = time_buf.measure_time();
        let duration = time_buf.measure_duration(start, end_time);
        time_buf.add_task_time(node_info.slot, node_name, duration);
    }

    // Send result
    let _ = completed_tx.send((node_info, result));
}

pub fn send_to_scheduler(
    shared: &Arc<SharedData>,
    node_info: NodeInfo,
    arg_vec: Vec<CmTypes>,
    custom_func: Option<CmPtr>,
) {
    let cache_entry = &shared.node_cache[node_info.id as usize];
    let func = {
        if custom_func.is_some() {
            custom_func.unwrap()
        } else {
            cache_entry.func_ptr
        }
    };
    let node_name = cache_entry.name.clone();
    let time_buf = Arc::clone(&shared.time_buffer);

    // Get scheduler with proper error handling
    let scheduler_guard = match shared.scheduler.read() {
        Ok(guard) => guard,
        Err(e) => {
            eprintln!("Failed to acquire scheduler lock: {}", e);
            return;
        }
    };

    let scheduler = match scheduler_guard.as_ref() {
        Some(s) => s,
        None => {
            eprintln!("Scheduler is not initialized");
            return;
        }
    };

    let completed_tx = match shared.completed_tx.read() {
        Ok(guard) => guard.as_ref().unwrap().clone(),
        Err(e) => {
            eprintln!("Failed to acquire completed_tx lock: {}", e);
            return;
        }
    };

    // Spawn task
    scheduler.spawn_task(move || {
        execute_task(
            func,
            arg_vec,
            node_info,
            &time_buf,
            &node_name,
            &completed_tx,
        )
    });
}

pub fn conditions_met(
    shared: &Arc<SharedData>,
    node_info: &NodeInfo,
    arg_indexes: &Vec<usize>,
) -> bool {
    let node = &shared.graph.nodes[node_info.id as usize];
    let mut is_ready = true;

    for arg_idx in arg_indexes {
        let arg = &node.args[*arg_idx];
        let init_condition: &InitCondition = &arg.init_condition.as_ref().unwrap();
        // We assume condition has a single predecessor
        let result = &collect_arg_result(
            arg,
            node_info.index,
            node_info.slot,
            node_info.pred_index,
            None,
            shared,
        )
        .unwrap()[0];

        let eval = init_condition.evaluate(&result);
        if !eval {
            is_ready = false;
            break;
        }
    }
    is_ready
}

pub fn process_slot_completion(shared: &Arc<SharedData>, slot: usize) -> bool {
    // Complete timing - use unwrap_or to handle errors gracefully
    if let Err(e) = shared.time_buffer.finish_slot_processing(slot) {
        eprintln!("Warning: Failed to finish slot {} timing: {}", slot, e);
    }

    let mut new_iteration = false;
    // Increment global completion counter
    let new_counter = shared
        .stream_complete_counter
        .fetch_add(1, Ordering::SeqCst)
        + shared.slots;

    // Check if we should start a new iteration
    if new_counter < shared.max_streams {
        print_debug(|| format!("Starting new iteration {}", new_counter));
        new_iteration = true;

        // Release the slot
        release_slot(shared, slot);

        // Clear completed nodes for this stream to allow restart
        let mut result_lock = shared.node_results.write().unwrap();
        result_lock.reinit_slot(slot);
        drop(result_lock);
    }
    new_iteration
}

pub fn assign_stream_to_available_slot(shared: &Arc<SharedData>, stream: usize) -> usize {
    let mut available_slots = shared.available_stream_slots.write().unwrap();

    // Check if this streams is already mapped to a slot
    let mut av_slot_id: usize = usize::MAX;
    for (slot_id, &real_stream) in available_slots.iter().enumerate() {
        if real_stream == stream {
            print_debug(|| format!("Stream: {} is already assigned to slot {}", stream, slot_id));
            return slot_id;
        } else if real_stream == std::usize::MAX && av_slot_id == std::usize::MAX {
            av_slot_id = slot_id;
        }
    }

    // Assign this stream to the available slot

    if av_slot_id == std::usize::MAX {
        // Find first available slot
        for (slot_id, &real_stream) in available_slots.iter().enumerate() {
            if real_stream == std::usize::MAX {
                av_slot_id = slot_id;
                break;
            }
        }
    }

    if av_slot_id == std::usize::MAX {
        panic!("No available stream slots for stream: {}", stream);
    }

    available_slots[av_slot_id] = stream; // Mark as busy
    print_debug(|| {
        format!(
            "Assigned stream: {} to available slot {}",
            stream, av_slot_id
        )
    });
    // Start Slot Timing
    shared.time_buffer.start_slot_processing(av_slot_id);
    return av_slot_id;
}

pub fn release_slot(shared: &Arc<SharedData>, slot: usize) {
    let mut available_slots = shared.available_stream_slots.write().unwrap();

    let old_stream = available_slots[slot].clone();
    available_slots[slot] = std::usize::MAX; // Mark as available
    print_debug(|| format!("Released slot {} (had stream: {})", slot, old_stream));
}

pub fn process_id_function(
    shared: &Arc<SharedData>,
    node_info: &NodeInfo,
    result: &CmTypes,
) -> Option<usize> {
    let id_function_opt = shared.graph.id_function.clone();

    if let Some(id_function) = id_function_opt {
        let msg = "ID function is not set".to_string();
        let func_ptr = id_function.func_ptr.expect(&msg);
        let predecessor = id_function.predecessor;
        // Check if completed node is the predecessor
        if predecessor == node_info.id {
            let arg_vec = parse_args(
                shared,
                &id_function.args,
                node_info.index,
                node_info.slot,
                node_info.pred_index,
                Some(result),
            );

            // Call the id function
            print_debug(|| format!("Calling ID function for {:?}", node_info));
            let id_result = func_ptr(arg_vec);

            // Extract stream from the result
            if let Some(new_stream) = id_result.valid_number_to_usize() {
                // Validate stream range
                let current_counter = shared.stream_complete_counter.load(Ordering::SeqCst);
                let max_allowed_stream = current_counter + shared.slots;

                if new_stream >= max_allowed_stream {
                    eprintln!(
                                "ID function returned stream {} which exceeds maximum allowed {} (current_counter: {}, slots: {})",
                                new_stream, max_allowed_stream, current_counter, shared.slots
                            );
                    return None;
                }
                return Some(new_stream);
            } else {
                panic!("ID function did not return a valid number for stream");
            }
        }
    }
    return Some(node_info.slot);
}

pub fn create_task(
    func: CmPtr,
    arg_vec: Vec<CmTypes>,
    node_info: NodeInfo,
    node_name: String,
    completed_tx: Sender<(NodeInfo, CmTypes)>,
    time_buf: Arc<TimeBufferManager>,
) -> impl FnOnce() {
    let task = move || {
        let start_time = time_buf.measure_time();

        let result = func(arg_vec);

        if !node_info.post_node {
            let end_time = time_buf.measure_time();
            let duration = time_buf.measure_duration(start_time, end_time);
            time_buf.add_task_time(node_info.slot, &node_name, duration);
        }
        // Send result through channel
        completed_tx.send((node_info, result)).unwrap();
    };
    task
}

#[inline]
pub fn create_node_args(
    shared: &Arc<SharedData>,
    node: &NodeCacheEntry,
    node_id: IdType,
    node_index: usize,
    slot: usize,
    pred_index: usize,
) -> Vec<CmTypes> {
    let args_cache = {
        // check if node is in loop_nodes
        // let loop_read = self.loop_nodes.read().unwrap();
        // let mut looping = false;
        // if loop_read.contains(&node.name.clone()) {
        //     // node is in loop_nodes
        //     looping = true;
        // }

        // let loop_opt = node.loop_args.as_ref();

        // if looping && loop_opt.is_some() {
        //     loop_opt.unwrap()
        // } else {
        //     &node.args
        // }
        &node.arg_cache
    };

    parse_cached_args(
        shared, args_cache, node_id, node_index, slot, pred_index, None,
    )
}

#[inline(always)]
fn process_buffer_refs(arg_vec: &mut Vec<CmTypes>, cache: &ArgCacheEntry, node_index: usize) {
    for (i, idx) in cache.buffer_ref_indexes.iter().enumerate() {
        arg_vec[*idx] = get_object_value(&cache.buffer_values[i], node_index);
    }
}

#[inline(always)]
fn process_runtime_refs(
    arg_vec: &mut Vec<CmTypes>,
    cache: &ArgCacheEntry,
    node_index: usize,
    workers: usize,
) {
    // Process both types of runtime refs in a single iteration if possible
    if cache.rt_idxs_indexes.len() == cache.rt_workers_indexes.len() {
        for (idx_idx, worker_idx) in cache
            .rt_idxs_indexes
            .iter()
            .zip(cache.rt_workers_indexes.iter())
        {
            arg_vec[*idx_idx] = CmTypes::Usize(node_index);
            arg_vec[*worker_idx] = CmTypes::Usize(workers);
        }
    } else {
        // Fall back to separate processing
        for idx in cache.rt_idxs_indexes.iter() {
            arg_vec[*idx] = CmTypes::Usize(node_index);
        }
        for idx in cache.rt_workers_indexes.iter() {
            arg_vec[*idx] = CmTypes::Usize(workers);
        }
    }
}

#[inline(always)]
pub fn parse_cached_args(
    shared: &Arc<SharedData>,
    args_cache: &ArgCacheEntry,
    node_id: IdType,
    node_index: usize,
    slot: usize,
    pred_index: usize,
    custom_res: Option<&CmTypes>,
) -> Vec<CmTypes> {
    if args_cache.buffer_ref_indexes.is_empty()
        && args_cache.rt_idxs_indexes.is_empty()
        && args_cache.rt_workers_indexes.is_empty()
        && args_cache.res_indexes.is_empty()
    {
        return args_cache.args.clone();
    }

    let mut arg_vec = args_cache.args.clone();

    // Pre-fetch workers count if needed
    let workers = if !args_cache.rt_workers_indexes.is_empty() {
        shared.workers.load(Ordering::Relaxed)
    } else {
        0
    };

    process_buffer_refs(&mut arg_vec, args_cache, node_index);
    process_runtime_refs(&mut arg_vec, args_cache, node_index, workers);

    for (res_idx, real_idx) in args_cache
        .res_indexes
        .iter()
        .zip(args_cache.real_res_indexes.iter())
    {
        let arg = shared.graph.nodes[node_id as usize]
            .args
            .get(*real_idx)
            .expect("Argument index out of bounds");

        let result_opt = collect_arg_result(arg, node_index, slot, pred_index, custom_res, shared);
        if let Some(mut result) = result_opt {
            if result.len() == 1 {
                arg_vec[*res_idx] = result.remove(0);
            } else {
                // insert to res_idx and next positions by expanding vec
                arg_vec.splice(*res_idx..*res_idx + 1, result);
            }
        }
    }
    arg_vec
}

pub fn parse_args(
    shared: &Arc<SharedData>,
    args: &Vec<Arg>,
    node_index: usize,
    slot: usize,
    pred_index: usize,
    custom_res: Option<&CmTypes>,
) -> Vec<CmTypes> {
    // Pre-allocate capacity to avoid reallocations
    let mut arg_vec = Vec::with_capacity(args.len());
    for arg in args.iter() {
        // continue if arg is a condition
        if arg.is_condition() {
            continue;
        }

        let result_opt = collect_arg_result(arg, node_index, slot, pred_index, custom_res, shared);
        if let Some(result) = result_opt {
            arg_vec.extend(result);
        }
    }
    arg_vec
}

#[inline(always)]
fn handle_special_ref(
    obj_id: usize,
    node_index: usize,
    workers: &Arc<AtomicUsize>,
) -> Option<Vec<CmTypes>> {
    match obj_id {
        0 => Some(vec![CmTypes::Usize(node_index)]),
        1 => Some(vec![CmTypes::Usize(workers.load(Ordering::Relaxed))]),
        _ => None,
    }
}

#[inline(always)]
fn get_object_value(obj_vec: &[CmTypes], node_index: usize) -> CmTypes {
    if obj_vec.len() > 1 {
        obj_vec[node_index % obj_vec.len()].clone()
    } else {
        obj_vec[0].clone()
    }
}

#[inline]
pub fn collect_arg_result(
    arg: &Arg,
    node_index: usize,
    slot: usize,
    pred_index: usize,
    custom_res: Option<&CmTypes>,
    shared: &Arc<SharedData>,
) -> Option<Vec<CmTypes>> {
    match &arg.type_ {
        CmTypes::Ref(obj_id) => {
            let obj_id = *obj_id;
            if let Some(result) = handle_special_ref(obj_id, node_index, &shared.workers) {
                return Some(result);
            }

            let obj_vec = &shared.graph.init_objects.as_ref().unwrap()[obj_id as usize];
            Some(vec![get_object_value(obj_vec, node_index)])
        }
        CmTypes::Res(res_node_id) => {
            if let Some(custom_res) = custom_res {
                return Some(vec![(*custom_res).clone()]);
            }

            // Get predecessor info
            let predecessor = match arg.predecessor.as_ref() {
                Some(p) => p,
                None => return None, // Early return if no predecessor
            };

            // Special case for single index
            if predecessor.indexes.len() == 1 {
                let node_info = NodeInfo::new(*res_node_id as IdType, slot, pred_index, 0);
                if let Ok(res_read) = shared.node_results.read() {
                    if let Some(result) = res_read.get(&node_info) {
                        return Some(vec![result]);
                    }
                }
                return None;
            }

            // Batch process multiple indices
            let pred_node = &shared.graph.nodes[predecessor.id as usize];
            let pred_factor = pred_node.factor;

            // Pre-allocate vectors
            let mut indices = Vec::with_capacity(predecessor.indexes.len());
            for &pred_idx in predecessor.indexes.iter() {
                indices.push(find_pred_index(node_index, pred_idx, pred_factor));
            }

            if let Ok(res_read) = shared.node_results.read() {
                let mut result_vec = Vec::with_capacity(indices.len());

                // Batch collect all results
                for dep_idx in indices.iter() {
                    let node_info = NodeInfo::new(*res_node_id as IdType, slot, *dep_idx, 0);
                    if let Some(result) = res_read.get(&node_info) {
                        result_vec.push(result);
                    } else {
                        return None; // Early return if any result is missing
                    }
                }

                if result_vec.len() == indices.len() {
                    return Some(result_vec);
                }
            }
            None
        }
        CmTypes::Barrier(_) => None,
        _ => Some(vec![arg.type_.clone()]),
    }
}

pub fn initial_nodes(shared: &Arc<SharedData>, slots: Vec<usize>) -> Vec<NodeInfo> {
    let mut node_infos = Vec::new();
    for slot in slots {
        let initial_nodes = &shared.graph.initial_nodes;
        for node_id in initial_nodes {
            let node = &shared.graph.nodes[*node_id as usize];
            let node_factor = node.factor;
            let indexes: Vec<usize> = (0..node_factor).collect();
            for index in indexes {
                let node_info = NodeInfo::new(*node_id, slot, index, 0);
                node_infos.push(node_info);
            }
        }
    }
    node_infos
}
