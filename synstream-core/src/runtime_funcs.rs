use crate::debug::print_debug;
use crate::resolution_state::ResolutionState;
use crate::time_buffer::{TimeBufferManager, TimingMethod};
use crate::{buffers::*, graph::*, graph_struct::*, scheduler::*, IdType, Record};
use core::panic;
use crossbeam_channel::Sender;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use synstream_types::*;

/// Slot state for priority-based processing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    Active,    // Slot is actively processing and sending tasks to scheduler
    Buffering, // Slot is buffering but not sending tasks to workers
}

// Cache entry for quick node access - stores commonly accessed node fields
#[derive(Clone)]
pub struct NodeCacheEntry {
    pub factor: usize,
    pub pred_vec: Vec<usize>,
    pub name: String,
    pub func_ptr: CmPtr,
    pub arg_cache: ArgCacheEntry,
    // Pre-computed flag: true if this node is in initial_nodes
    pub is_initial: bool,
    // Pre-computed flag: true if this node is in condition_nodes
    pub is_condition: bool,
    // Pre-computed index into cond_indexes array (only valid if is_condition is true)
    pub cond_index: usize,
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

#[inline]
pub fn node_cache_entry(
    node: &Node,
    init_objects: &Vec<Vec<CmTypes>>,
    initial_nodes: &Vec<crate::IdType>,
    condition_nodes: &std::collections::HashSet<crate::IdType>,
) -> NodeCacheEntry {
    let mut rt_idxs_indexes = Vec::new();
    let mut buffer_ref_indexes = Vec::new();
    let mut buffer_values = Vec::new();
    let mut rt_workers_indexes = Vec::new();
    let mut real_res_indexes = Vec::new();
    let mut res_indexes = Vec::new();
    let mut args = vec![CmTypes::None; node.args.len()];

    let mut idx_count = 0;
    let mut pred_hash: std::collections::HashMap<IdType, Vec<usize>> =
        std::collections::HashMap::new();

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
                let pred = arg
                    .predecessor
                    .as_ref()
                    .expect("Result argument missing predecessor");
                let pred_id = pred.id;
                let pred_idx_count = pred.indexes.len();

                if !pred_hash.contains_key(&pred_id) {
                    pred_hash.insert(pred_id, vec![pred_idx_count]);
                } else {
                    pred_hash.get_mut(&pred_id).unwrap().push(pred_idx_count);
                }
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

    let max_pred_id = pred_hash.keys().max().cloned().unwrap_or(0);
    let mut pred_vec = Vec::new();
    for pred_id in 0..max_pred_id + 1 {
        if let Some(pred_ids_count) = pred_hash.get(&pred_id) {
            // count unique elements in pred_ids_count
            let unique_counts: std::collections::HashSet<usize> =
                pred_ids_count.iter().cloned().collect();
            let count = unique_counts.iter().max().unwrap();
            pred_vec.push(*count);
        } else {
            pred_vec.push(0);
        }
    }

    // Pre-compute condition index for O(1) lookup
    let cond_index = if condition_nodes.contains(&node.id) {
        condition_nodes
            .iter()
            .position(|&x| x == node.id)
            .unwrap_or(0)
    } else {
        0
    };

    NodeCacheEntry {
        factor: node.factor,
        pred_vec,
        name: node.name.clone(),
        func_ptr: node.func_ptr.expect("Node function pointer is None"),
        arg_cache,
        is_initial: initial_nodes.contains(&node.id),
        is_condition: condition_nodes.contains(&node.id),
        cond_index,
    }
}

// Shared data across all SynStream threads - immutable or internally synchronized
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
    pub time_buffer: Option<Arc<TimeBufferManager>>,

    // Shared between threads
    pub scheduler: Arc<RwLock<Option<Arc<SchedulerImpl>>>>,
    pub network_scheduler: Arc<RwLock<Option<Arc<SchedulerImpl>>>>,
    pub completed_tx: Arc<RwLock<Option<Sender<Vec<(NodeInfo, CmTypes)>>>>>,
    pub workers: Arc<AtomicUsize>,
    pub recorder: Option<Arc<Mutex<HashMap<usize, Vec<Record>>>>>,
    pub base_instant: Arc<Instant>,
    pub job_counter: Arc<AtomicUsize>,
    pub core_offset: Arc<AtomicUsize>,

    // Scheduler-side batching structures
    pub batch_buffer: Arc<Mutex<Vec<(NodeInfo, CmTypes)>>>,
    pub batch_last_sent: Arc<Mutex<Instant>>,
    pub flusher_shutdown: Arc<AtomicUsize>,

    // Resolution state - abstracted for single vs multi-threaded
    pub resolution_state: Arc<dyn ResolutionState>,

    // Shared dependency tracking for multi-threaded resolution
    pub remaining_nodes: Arc<Vec<Vec<AtomicUsize>>>,
    // remaining_cond_nodes[slot][cond_rem_idx] - AtomicUsize for lock-free access
    pub remaining_cond_nodes: Arc<Vec<Vec<AtomicUsize>>>,
    pub node_id_to_rem: Arc<Vec<usize>>,
    // Maps node_id to whether it's in remaining_nodes (false) or remaining_cond_nodes (true)
    pub node_id_is_cond: Arc<Vec<bool>>,
    // Initial factors for remaining_nodes, used for reinit (remaining_init[slot][node_rem_idx])
    pub remaining_init: Arc<Vec<Vec<usize>>>,
    pub initial_prep_done: Arc<AtomicUsize>,
    pub system_threads: usize,

    // Slot priority processing state
    pub slot_states: Arc<RwLock<Vec<SlotState>>>,
    pub slot_priority_enabled: bool,
    // Per-slot buffering: holds ready nodes waiting for slot activation
    pub slot_buffers: Arc<RwLock<Vec<Vec<NodeInfo>>>>,
}

#[inline(always)]
fn execute_task(
    shared: &Arc<SharedData>,
    func: CmPtr,
    node_info: &NodeInfo,
    time_buf: &Option<Arc<TimeBufferManager>>,
    node_name: &str,
    pre_built_args: Option<Vec<CmTypes>>,
) {
    let worker_id = crate::scheduler::get_current_worker_id().unwrap_or(usize::MAX);

    // Measure argument building time separately
    let arg_build_start = if !node_info.post_node {
        Some(if let Some(tb) = time_buf {
            tb.measure_time()
        } else {
            TimingMethod::Instant(Instant::now())
        })
    } else {
        None
    };

    // Build arguments here in the worker thread
    let arg_vec = if let Some(args) = pre_built_args {
        // For post-nodes or special cases with pre-built args
        args
    } else {
        // For regular nodes, build args from cache
        let node_cache = &shared.node_cache[node_info.id as usize];
        create_node_args(
            shared,
            node_cache,
            node_info.id,
            node_info.index,
            node_info.slot,
            node_info.pred_index,
        )
    };

    if let Some(arg_start) = arg_build_start {
        if let Some(tb) = time_buf {
            let arg_end = tb.measure_time();
            let arg_duration = tb.measure_duration(arg_start, arg_end);
            tb.add_task_time(
                node_info.slot,
                &format!("{}-argbuild", node_name),
                worker_id,
                arg_duration,
            );
        }
    }

    // Start timing for actual function execution
    let start_time = if !node_info.post_node {
        Some(if let Some(tb) = time_buf {
            tb.measure_time()
        } else {
            TimingMethod::Instant(Instant::now())
        })
    } else {
        None
    };

    let result = func(arg_vec);

    if let Some(start) = start_time {
        if let Some(tb) = time_buf {
            let end_time = tb.measure_time();
            let duration = tb.measure_duration(start, end_time);
            tb.add_task_time(node_info.slot, node_name, worker_id, duration);
        }
    }

    // Add result to batch buffer and flush if full
    let scheduler_guard = shared.scheduler.read();
    if let Some(scheduler) = scheduler_guard.as_ref() {
        let batch_buffer = scheduler.get_batch_buffer();
        let batching_size = scheduler.get_batching_size();
        let batch_last_sent = scheduler.get_batch_last_sent();
        let completed_tx_ref = scheduler.get_completed_tx_ref();

        let should_flush = {
            let mut batch = batch_buffer.lock();
            batch.push((node_info.clone(), result));
            batch.len() >= batching_size
        };

        if should_flush {
            let mut batch = batch_buffer.lock();
            if batch.len() >= batching_size {
                let batch_to_send =
                    std::mem::replace(&mut *batch, Vec::with_capacity(batching_size));
                drop(batch);
                *batch_last_sent.lock() = std::time::Instant::now();

                if let Some(tx) = completed_tx_ref.lock().as_ref() {
                    let _ = tx.send(batch_to_send);
                }
            }
        }
    }
}

#[inline]
pub fn send_to_scheduler(
    shared: &Arc<SharedData>,
    nodes_to_schedule: &Vec<NodeInfo>,
    pre_built_args_vec: &Vec<Option<Vec<CmTypes>>>,
    custom_func_vec: &Vec<Option<CmPtr>>,
    use_network_scheduler: bool,
) {
    for (i, node_info) in nodes_to_schedule.iter().enumerate() {
        let (func_ptr, node_name) = {
            if node_info.post_node {
                let nodes = &shared
                    .graph
                    .post_nodes
                    .as_ref()
                    .expect("Post nodes not initialized");

                let node = &nodes[node_info.id as usize];

                let func = {
                    if custom_func_vec[i].is_some() {
                        custom_func_vec[i].unwrap()
                    } else {
                        node.func_ptr.expect("Post node function pointer is None")
                    }
                };

                let node_name = node.name.clone();
                (func, node_name)
            } else {
                let cache_entry = &shared.node_cache[node_info.id as usize];
                let func = {
                    if custom_func_vec[i].is_some() {
                        custom_func_vec[i].unwrap()
                    } else {
                        cache_entry.func_ptr
                    }
                };

                (func, cache_entry.name.clone())
            }
        };

        let time_buf = shared.time_buffer.clone();
        let shared_clone = Arc::clone(shared);

        // Select scheduler based on use_network_scheduler flag
        let scheduler = if use_network_scheduler {
            let network_scheduler_guard = shared.network_scheduler.read();
            match network_scheduler_guard.as_ref() {
                Some(s) => Arc::clone(s),
                None => {
                    // Fallback to main scheduler if network scheduler not initialized
                    let scheduler_guard = shared.scheduler.read();
                    match scheduler_guard.as_ref() {
                        Some(s) => Arc::clone(s),
                        None => {
                            eprintln!("No scheduler is initialized");
                            return;
                        }
                    }
                }
            }
        } else {
            let scheduler_guard = shared.scheduler.read();
            match scheduler_guard.as_ref() {
                Some(s) => Arc::clone(s),
                None => {
                    eprintln!("Scheduler is not initialized");
                    return;
                }
            }
        };

        // Spawn task
        let meta_data = (node_info.id, node_info.slot, node_info.index);
        let node_info = node_info.clone();
        let pre_built_args = pre_built_args_vec[i].clone();
        scheduler.spawn_task_with_meta(Some(meta_data), move || {
            execute_task(
                &shared_clone,
                func_ptr,
                &node_info,
                &time_buf,
                &node_name,
                pre_built_args,
            )
        });
    }
}

#[inline]
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

#[inline]
pub fn process_slot_completion(shared: &Arc<SharedData>, slot: usize) -> bool {
    // Complete timing - use unwrap_or to handle errors gracefully
    if let Some(tb) = &shared.time_buffer {
        if let Err(e) = tb.finish_slot_processing(slot) {
            eprintln!("Warning: Failed to finish slot {} timing: {}", slot, e);
        }
    }

    // Count currently active/processing streams (excluding this completing slot)
    let currently_active_streams = {
        let available_slots = shared.available_stream_slots.read();
        available_slots
            .iter()
            .enumerate()
            .filter(|(s_id, &stream)| *s_id != slot && stream != usize::MAX)
            .count()
    };

    // Increment global completion counter
    let completed_streams = shared
        .stream_complete_counter
        .fetch_add(1, Ordering::SeqCst)
        + 1;

    // Total streams in-flight or completed
    let total_streams_processed = completed_streams + currently_active_streams;

    // Decide whether to start a new stream on this slot
    let can_restart = total_streams_processed < shared.max_streams;

    if can_restart {
        print_debug(|| {
            format!(
                "Slot {} completed stream. Starting new: completed={}, active={}, total={}, max={}",
                slot,
                completed_streams,
                currently_active_streams,
                total_streams_processed,
                shared.max_streams
            )
        });

        // Release the slot
        release_slot(shared, slot);

        // Clear completed nodes for this slot to allow restart
        shared
            .node_results
            .write()
            .reinit_slot(&shared.graph.nodes, slot, None);

        // In slot-priority mode: Keep slot Active if it's currently active (immediate restart)
        // This allows the completing slot to immediately start processing the next stream
        // without being buffered. Round-robin activation will happen when multiple slots
        // are waiting, not when a slot is actively restarting.

        true // Signal to caller: slot should restart
    } else {
        print_debug(|| {
            format!(
                "Slot {} completed. Max streams ({}) reached: completed={}, active={}",
                slot, shared.max_streams, completed_streams, currently_active_streams
            )
        });

        false // Signal to caller: no restart needed
    }
}

#[inline]
pub fn assign_stream_to_available_slot(shared: &Arc<SharedData>, stream: usize) -> usize {
    let mut available_slots = shared.available_stream_slots.write();

    // Check if this stream is already mapped to a slot
    let mut av_slot_id: usize = usize::MAX;
    for (slot_id, &real_stream) in available_slots.iter().enumerate() {
        if real_stream == stream {
            print_debug(|| format!("Stream: {} is already assigned to slot {}", stream, slot_id));
            return slot_id;
        } else if real_stream == std::usize::MAX && av_slot_id == std::usize::MAX {
            av_slot_id = slot_id;
        }
    }

    // If no available slot found, try again (in case of race condition)
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

    // For slot-priority: assign stream N to slot N when possible
    // This ensures streams map to specific slots in slot-priority mode
    let preferred_slot = stream % shared.slots;
    if shared.slot_priority_enabled && available_slots[preferred_slot] == std::usize::MAX {
        av_slot_id = preferred_slot;
        print_debug(|| {
            format!(
                "Slot-priority: Assigning stream {} to preferred slot {}",
                stream, preferred_slot
            )
        });
    }

    available_slots[av_slot_id] = stream; // Mark as busy
    print_debug(|| {
        format!(
            "Assigned stream: {} to available slot {}",
            stream, av_slot_id
        )
    });
    // Start Slot Timing
    if let Some(tb) = &shared.time_buffer {
        tb.start_slot_processing(av_slot_id);
    }
    return av_slot_id;
}

pub fn release_slot(shared: &Arc<SharedData>, slot: usize) {
    let mut available_slots = shared.available_stream_slots.write();

    let old_stream = available_slots[slot];
    available_slots[slot] = std::usize::MAX; // Mark as available
    print_debug(|| format!("Released slot {} (had stream: {})", slot, old_stream));
}

#[inline]
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
                print_debug(|| {
                    format!(
                        "ID function determined stream {} for {:?}",
                        new_stream, node_info
                    )
                });
                return Some(new_stream);
            } else {
                panic!("ID function did not return a valid number for stream");
            }
        }
    }
    // find real stream belonging to this slot
    let available_slots = shared.available_stream_slots.read();
    for (slot_id, &real_stream) in available_slots.iter().enumerate() {
        if slot_id == node_info.slot {
            if real_stream == usize::MAX {
                return Some(node_info.slot);
            }
            return Some(real_stream);
        }
    }
    panic!("Could not find stream for node");
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
        // let loop_read = self.loop_nodes.read();
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

#[inline]
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
                let res_node = &shared.graph.nodes[*res_node_id as usize];
                let res_factor = res_node.factor;
                let node_info =
                    NodeInfo::new(*res_node_id as IdType, slot, pred_index % res_factor, 0);
                let res_read = shared.node_results.read();
                if let Some(result) = res_read.get(&node_info) {
                    return Some(vec![result]);
                } else {
                    panic!(
                        "Missing result for node_info: {:?} when collecting argument",
                        node_info
                    );
                }
            }

            // Batch process multiple indices
            let pred_node = &shared.graph.nodes[predecessor.id as usize];
            let pred_factor = pred_node.factor;

            // Pre-allocate vectors
            let mut indices = Vec::with_capacity(predecessor.indexes.len());
            for &pred_idx in predecessor.indexes.iter() {
                indices.push(find_pred_index(node_index, pred_idx, pred_factor));
            }

            let res_read = shared.node_results.read();
            let mut result_vec = Vec::with_capacity(indices.len());

            // Batch collect all results
            for dep_idx in indices.iter() {
                let node_info = NodeInfo::new(*res_node_id as IdType, slot, *dep_idx, 0);
                if let Some(result) = res_read.get(&node_info) {
                    result_vec.push(result);
                } else {
                    panic!(
                        "Missing result for node_info: {:?} when collecting argument",
                        node_info
                    );
                }
            }

            if result_vec.len() == indices.len() {
                return Some(result_vec);
            }
            None
        }
        CmTypes::Barrier(_) => None,
        _ => Some(vec![arg.type_.clone()]),
    }
}

/// Check if a slot is active (ready to send tasks to scheduler)
#[inline]
pub fn is_slot_active(shared: &Arc<SharedData>, slot: usize) -> bool {
    if !shared.slot_priority_enabled {
        return true; // All slots active when feature disabled
    }
    let states = shared.slot_states.read();
    states[slot] == SlotState::Active
}

/// Activate the next buffering slot (if any) and return its buffered nodes
/// Transition a slot from Active to Buffering state (for round-robin rotation)
pub fn transition_slot_to_buffering(shared: &Arc<SharedData>, slot: usize) {
    let mut states = shared.slot_states.write();
    if states[slot] == SlotState::Active {
        states[slot] = SlotState::Buffering;
        print_debug(|| format!("Round-Robin: Slot {} transitioned to Buffering state", slot));
    }
}

/// Activate the next buffering slot in round-robin order
/// Returns the slot ID that was activated and its buffered nodes
/// When slot-priority is enabled, automatically uses round-robin activation
pub fn activate_next_slot(
    shared: &Arc<SharedData>,
    completing_slot: Option<usize>,
) -> Option<Vec<NodeInfo>> {
    if !shared.slot_priority_enabled {
        return None;
    }
    print_debug(|| format!("Activating next slot"));

    // Find and activate next buffering slot in round-robin order
    let activated_slot = {
        let mut states = shared.slot_states.write();

        // Round-robin: find next buffering slot in circular order after completing_slot
        if let Some(completed) = completing_slot {
            let num_slots = states.len();
            // Search for next Buffering slot starting from (completed + 1)
            let mut found_slot = None;
            for offset in 1..=num_slots {
                let candidate = (completed + offset) % num_slots;
                if states[candidate] == SlotState::Buffering {
                    states[candidate] = SlotState::Active;
                    print_debug(|| {
                        format!(
                            "Slot-Priority: Activated slot {} for processing (after slot {})",
                            candidate, completed
                        )
                    });
                    found_slot = Some(candidate);
                    break;
                }
            }
            found_slot
        } else {
            // Fallback: find first buffering slot
            states
                .iter_mut()
                .enumerate()
                .find(|(_, state)| **state == SlotState::Buffering)
                .map(|(slot_id, state)| {
                    *state = SlotState::Active;
                    print_debug(|| {
                        format!("Slot-Priority: Activated slot {} for processing", slot_id)
                    });
                    slot_id
                })
        }
    };

    // Retrieve and clear buffered nodes for this slot
    activated_slot.map(|slot_id| {
        let mut slot_buffers = shared.slot_buffers.write();
        std::mem::take(&mut slot_buffers[slot_id])
    })
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
