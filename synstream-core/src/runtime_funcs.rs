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
    pub time_buffer: Arc<RwLock<TimeBufferManager>>,

    // Shared between threads
    pub scheduler: Arc<RwLock<Option<Arc<SchedulerImpl>>>>,
    pub completed_tx: Arc<RwLock<Option<Sender<(NodeInfo, CmTypes)>>>>,
    pub workers: Arc<AtomicUsize>,
}

pub fn send_to_scheduler(shared: &Arc<SharedData>, node_info: NodeInfo, arg_vec: Vec<CmTypes>) {
    let nodes = {
        if node_info.post_node {
            // Use the static graph for post nodes
            &shared.graph.post_nodes.as_ref().unwrap()
        } else {
            // Use the appropriate graph for this slot
            &shared.graph.nodes
        }
    };

    let node = &nodes[node_info.id as usize];

    let error = format!(
        "Node {} with index {} has no function pointer",
        node_info.id, node_info.index
    );
    let func: CmPtr = node.func_ptr.expect(error.as_str());

    // Schedule Task
    let completed_tx_clone = {
        let tx_lock = shared.completed_tx.read().unwrap();
        tx_lock.as_ref().unwrap().clone()
    };
    let time_buffer_clone = shared.time_buffer.clone();
    let node_name = shared.node_cache[node_info.id as usize].name.clone();

    let task = create_task(
        func,
        arg_vec,
        node_info,
        node_name,
        completed_tx_clone,
        time_buffer_clone,
    );

    // Avoid cloning Arc - use scheduler directly through read lock
    let scheduler_lock = shared.scheduler.read().unwrap();
    scheduler_lock.as_ref().unwrap().spawn_task(task);
    drop(scheduler_lock);
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
    let time_read = shared.time_buffer.read().unwrap();
    if let Err(e) = time_read.finish_slot_processing(slot) {
        eprintln!("Warning: Failed to finish slot {} timing: {}", slot, e);
    }
    drop(time_read);

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
    time_buf: Arc<RwLock<TimeBufferManager>>,
) -> impl FnOnce() {
    let task = move || {
        let time_read = time_buf.read().unwrap();
        let start_time = time_read.measure_time();
        drop(time_read);

        let result = func(arg_vec);

        if !node_info.post_node {
            let time_read = time_buf.read().unwrap();
            let end_time = time_read.measure_time();
            let duration = time_read.measure_duration(start_time, end_time);
            time_read.add_task_time(node_info.slot, &node_name, duration);
            drop(time_read);
        }
        // Send result through channel
        completed_tx.send((node_info, result)).unwrap();
    };
    task
}

pub fn create_node_args(
    shared: &Arc<SharedData>,
    node: &Node,
    node_index: usize,
    slot: usize,
    pred_index: usize,
) -> Vec<CmTypes> {
    let args = {
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
        &node.args
    };

    let arg_vec = parse_args(shared, args, node_index, slot, pred_index, None);

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
        if let Some(mut result) = result_opt {
            arg_vec.reserve(result.len());
            arg_vec.append(&mut result);
        }
    }
    arg_vec
}

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
            let init_objects = &shared.graph.init_objects.as_ref().unwrap();
            // Argument may be node index
            if obj_id == 0 {
                // reserved for $index
                return Some(vec![CmTypes::Usize(node_index)]);
            }
            // Argument may be worker num
            if obj_id == 1 {
                // reserved for $workers
                return Some(vec![CmTypes::Usize(shared.workers.load(Ordering::SeqCst))]);
            }

            // object may be either buffer indexed by node_index
            // or just variable indexed by 0
            let obj_vec = &init_objects[obj_id as usize];
            let obj = {
                if obj_vec.len() > 1 {
                    // If the object is a buffer, get the object according to node_index
                    let index = node_index % obj_vec.len();
                    obj_vec[index].clone()
                } else {
                    // If the object is a variable, get the first element
                    obj_vec[0].clone()
                }
            };
            return Some(vec![obj]);
        }
        CmTypes::Res(res_node_id) => {
            if let Some(custom_res) = custom_res {
                return Some(vec![(*custom_res).clone()]);
            }
            let mut indices = arg
                .predecessor
                .as_ref()
                .unwrap()
                .indexes
                .iter()
                .map(|&pred_idx| {
                    // Get the predecessor node factor
                    let nodes = &shared.graph.nodes;
                    let pred_node: &Node = &nodes[arg.predecessor.as_ref().unwrap().id as usize];
                    let pred_factor = pred_node.factor;

                    // Find the index of the node in the results
                    let new_index = find_pred_index(node_index, pred_idx, pred_factor);
                    new_index
                })
                .collect::<Vec<usize>>();

            if indices.len() == 1 {
                indices[0] = pred_index;
            }

            let mut result_vec = Vec::with_capacity(indices.len());
            // Acquire lock once for all results
            let res_read = shared.node_results.read().unwrap();
            for dep_idx in indices.iter() {
                // for each task index, retrieve the
                // corresponding results
                // (must exist since they are completed)
                let node_info = NodeInfo::new(*res_node_id as IdType, slot, *dep_idx, 0);
                let result = res_read.get(&node_info).unwrap();
                result_vec.push(result);
            }
            drop(res_read);
            return Some(result_vec);
        }
        CmTypes::Barrier(_) => {
            // Barrier does not require any arguments
            return None;
        }
        _ => return Some(vec![arg.type_.clone()]),
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
