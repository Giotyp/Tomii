use crate::debug::print_debug;
use crate::network::{NetworkSocket, PacketMessage};
use crate::resolution_state::ResolutionState;
use crate::time_buffer::{TimeBufferManager, TimingMethod};
use crate::{buffers::*, graph::*, graph_struct::*, scheduler::*, IdType};
use core::panic;
use flume::{Receiver, Sender};
use parking_lot::RwLock;
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use synstream_types::*;

thread_local! {
    static ARG_BUF: RefCell<Vec<CmTypes>> = RefCell::new(Vec::with_capacity(16));
}

/// Slot state for priority-based processing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    Active,    // Slot is actively processing and sending tasks to scheduler
    Buffering, // Slot is buffering with tasks
    Inactive,  // Slot is inactive with no tasks
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
    // Phase 3B: Number of successors (for inline execution optimization)
    // Allows fast lookup without traversing successors list
    pub successor_count: usize,
    // Node-level condition cache (new format)
    pub node_condition: Option<NodeConditionCache>,
}

#[derive(Clone)]
pub struct NodeConditionCache {
    pub operation: CondOp,
    pub eval_value: CmTypes,
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

impl Default for ArgCacheEntry {
    fn default() -> Self {
        ArgCacheEntry {
            args: Vec::new(),
            buffer_ref_indexes: Vec::new(),
            buffer_values: Vec::new(),
            rt_idxs_indexes: Vec::new(),
            rt_workers_indexes: Vec::new(),
            res_indexes: Vec::new(),
            real_res_indexes: Vec::new(),
        }
    }
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
    print_debug(|| {
        format!(
            "Creating node cache entry for node {} name {}",
            node.id, node.name
        )
    });

    // For network node, create empty cache entry
    if node.name == "$network" {
        return NodeCacheEntry {
            factor: node.factor,
            pred_vec: Vec::new(),
            name: node.name.clone(),
            func_ptr: CmTypes::default_pointer(),
            arg_cache: ArgCacheEntry::default(),
            is_initial: false,
            is_condition: false,
            cond_index: 0,
            successor_count: 0,
            node_condition: None,
        };
    }

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

    // Parse node-level condition if present
    let node_condition = if let Some(cond) = &node.condition {
        // Build arg cache for condition args
        let mut cond_rt_idxs_indexes = Vec::new();
        let mut cond_buffer_ref_indexes = Vec::new();
        let mut cond_buffer_values = Vec::new();
        let mut cond_rt_workers_indexes = Vec::new();
        let mut cond_real_res_indexes = Vec::new();
        let mut cond_res_indexes = Vec::new();
        let mut cond_args_vec = vec![CmTypes::None; cond.args.len()];

        let mut cond_idx_count = 0;
        for (idx, arg) in cond.args.iter().enumerate() {
            match &arg.type_ {
                CmTypes::Ref(obj_id) => {
                    if *obj_id == 0 {
                        cond_rt_idxs_indexes.push(cond_idx_count);
                    } else if *obj_id == 1 {
                        cond_rt_workers_indexes.push(cond_idx_count);
                    } else {
                        let obj_vec = &init_objects[*obj_id];
                        if obj_vec.len() > 1 {
                            cond_buffer_ref_indexes.push(cond_idx_count);
                            cond_buffer_values.push(obj_vec.clone());
                        } else {
                            cond_args_vec[cond_idx_count] = obj_vec[0].clone();
                        }
                    }
                }
                CmTypes::Res(_) => {
                    cond_res_indexes.push(cond_idx_count);
                    cond_real_res_indexes.push(idx);
                }
                CmTypes::Barrier(_) => {
                    // Ignore barriers in condition args
                }
                _ => {
                    cond_args_vec[cond_idx_count] = arg.type_.clone();
                }
            }
            cond_idx_count += 1;
        }

        let cond_arg_cache = ArgCacheEntry {
            args: cond_args_vec,
            buffer_ref_indexes: cond_buffer_ref_indexes,
            buffer_values: cond_buffer_values,
            rt_idxs_indexes: cond_rt_idxs_indexes,
            rt_workers_indexes: cond_rt_workers_indexes,
            res_indexes: cond_res_indexes,
            real_res_indexes: cond_real_res_indexes,
        };

        Some(NodeConditionCache {
            operation: cond.operation.clone(),
            eval_value: cond.eval_value.clone(),
            func_ptr: cond.func_ptr,
            arg_cache: cond_arg_cache,
        })
    } else {
        None
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
        successor_count: 0, // Will be filled by caller with successor list length
        node_condition,
    }
}

// Shared data across all SynStream threads - immutable or internally synchronized
pub struct SharedData {
    // Immutable data
    pub graph: Graph,
    pub slots: usize,
    pub max_streams: usize,
    pub max_runtime: Option<u64>,
    pub system_threads: usize,
    pub receiver_threads: usize,
    pub workers: usize,
    pub core_offset: usize,
    pub receiver_core_offset: usize,
    pub record_stream: Option<usize>,

    // Node cache for fast repeated access
    pub node_cache: Vec<NodeCacheEntry>,

    // Internally synchronized data
    pub node_results: Arc<crate::buffers::LockFreeResultMap>,
    pub stream_complete_counter: Arc<AtomicUsize>,
    // Vector to keep track of running streams. If a streams is assigned then
    // it will have an entry (stream_id, slot_id).
    pub running_streams: Arc<RwLock<Vec<(usize, usize)>>>,
    pub time_buffer: Option<Arc<TimeBufferManager>>,

    // Shared between threads
    pub scheduler: Arc<SchedulerImpl>,
    pub async_recorder: Option<Arc<crate::async_recorder::AsyncRecorder>>,
    pub base_instant: Arc<Instant>,
    pub job_counter: Arc<AtomicUsize>,

    // Flume MPSC channel for lock-free task completion delivery
    pub batch_queue_tx: Sender<(NodeInfo, CmTypes)>,
    pub batch_queue_rx: Receiver<(NodeInfo, CmTypes)>,
    pub target_batch_size: usize,
    pub batch_timeout_us: u64,

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

    pub slot_pending_tasks: Arc<Vec<AtomicUsize>>,
    pub slot_pending_cond_tasks: Arc<Vec<AtomicUsize>>,

    // Condition node spawn tracking - optimization to skip evaluation when all instances spawned
    pub cond_instances_to_spawn: Arc<Vec<Vec<AtomicUsize>>>,

    // Slot priority processing state
    pub slot_states: Arc<RwLock<Vec<SlotState>>>,
    pub last_slot_assigned: Arc<AtomicUsize>,
    pub slot_priority_enabled: bool,
    // Per-slot buffering: holds ready nodes with their packet data waiting for slot activation
    pub slot_buffers: Arc<RwLock<Vec<Vec<(NodeInfo, CmTypes)>>>>,

    // Network receiver infrastructure (optional - only present if network_config exists)
    pub receive_finished: Arc<AtomicBool>,
    /// Flume MPSC channel from network receivers to resolution threads
    pub packet_sender: Sender<PacketMessage>,
    pub packet_receiver: Receiver<PacketMessage>,
    pub receiver_sockets: Vec<NetworkSocket>,
    pub packet_drop_counters: Vec<AtomicUsize>,
    pub shutdown_flag: Arc<AtomicBool>,

    /// Per-slot packet counters - each slot tracks its own packet index independently
    /// This prevents index overflow when multiple streams are processed concurrently
    pub slot_packet_counters: Arc<Vec<AtomicUsize>>,
    pub streams_receive_counter: Arc<AtomicUsize>,

    /// Per-slot packet completion flags - ensures exactly-once completion semantics
    /// Prevents multiple threads from detecting completion for the same stream
    pub slot_packet_complete: Arc<Vec<AtomicBool>>,

    /// Per-slot in-flight batch processing counter - prevents premature slot completion
    pub slot_processing_count: Arc<Vec<AtomicUsize>>,

    /// Per-group dependency support:
    pub pred_index_filter: Arc<Vec<Vec<Option<(usize, usize)>>>>,

    /// pred_group_by[succ_id][pred_id] = Some(group_size) means predecessor instances are grouped
    /// by group_size for per-group barrier tracking. None means global (all groups decremented).
    pub pred_group_by: Arc<Vec<Vec<Option<usize>>>>,
}

#[inline(always)]
fn execute_task(
    shared: &Arc<SharedData>,
    func: CmPtr,
    node_info: &NodeInfo,
    time_buf: &Option<Arc<TimeBufferManager>>,
    node_name: &str,
    pre_built_args: Option<Vec<CmTypes>>,
    spawn_ns: u128,
) {
    // Capture execution start timestamp immediately
    let exec_start_ns = shared.base_instant.elapsed().as_nanos();

    let worker_id = crate::scheduler::get_current_worker_id().unwrap_or(usize::MAX);

    // Record scheduling latency if async recorder enabled and slot should be recorded
    if shared.async_recorder.is_some() {
        if should_record_slot(shared, node_info.slot) {
            let job_id = shared.job_counter.fetch_add(1, Ordering::SeqCst);
            crate::async_recorder::submit_record(crate::Record {
                slot: node_info.slot,
                job_id,
                start_ns: spawn_ns,
                end_ns: exec_start_ns,
                worker: worker_id,
                task_id: IdType::MAX - 3 * (node_info.id as IdType),
                index: node_info.index,
            });
        }
    }

    // Measure argument building time separately
    // let arg_build_start = if !node_info.post_node {
    //     Some(if let Some(tb) = time_buf {
    //         tb.measure_time()
    //     } else {
    //         TimingMethod::Instant(Instant::now())
    //     })
    // } else {
    //     None
    // };

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

    let result = if let Some(ref args) = pre_built_args {
        // For post-nodes or special cases with pre-built args
        func(args)
    } else {
        // For regular nodes, build args from cache using thread-local buffer
        let node_cache = &shared.node_cache[node_info.id as usize];
        ARG_BUF.with(|buf_cell| {
            let mut buf = buf_cell.borrow_mut();
            buf.clear();
            populate_cached_args_into(
                &mut buf,
                shared,
                &node_cache.arg_cache,
                node_info.id,
                node_info.index,
                node_info.slot,
                node_info.pred_index,
            );
            let r = func(&buf);
            buf.clear(); // release Arc refs promptly
            r
        })
    };

    if let Some(start) = start_time {
        if let Some(tb) = time_buf {
            let end_time = tb.measure_time();
            let duration = tb.measure_duration(start, end_time);
            tb.add_task_time(node_info.slot, node_name, worker_id, duration);
        }
    }

    // Direct lock-free push to batch_queue - no mutex, no batching logic
    let _ = shared.batch_queue_tx.try_send((node_info.clone(), result));
}

#[inline]
pub fn send_to_scheduler(
    shared: &Arc<SharedData>,
    nodes_to_schedule: &Vec<NodeInfo>,
    pre_built_args_vec: &Vec<Option<Vec<CmTypes>>>,
    custom_func_vec: &Vec<Option<CmPtr>>,
) {
    for (i, node_info) in nodes_to_schedule.iter().enumerate() {
        let (func_ptr, node_name, node_priority, node_use_workers) = {
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
                (func, node_name, node.priority, node.use_workers.clone())
            } else {
                let node = &shared.graph.nodes[node_info.id as usize];
                let func = {
                    if custom_func_vec[i].is_some() {
                        custom_func_vec[i].unwrap()
                    } else {
                        shared.node_cache[node_info.id as usize].func_ptr
                    }
                };

                (
                    func,
                    node.name.clone(),
                    node.priority,
                    node.use_workers.clone(),
                )
            }
        };

        let time_buf = shared.time_buffer.clone();
        let shared_clone = Arc::clone(shared);

        // Determine if we should record this task based on stream filter
        let should_record = should_record_slot(shared, node_info.slot);

        // Spawn task - route to network pool if requested
        let meta_data = (node_info.id, node_info.slot, node_info.index, should_record);
        let node_info = node_info.clone();
        let pre_built_args = pre_built_args_vec[i].clone();
        // Capture spawn timestamp before any processing
        let spawn_ns = shared.base_instant.elapsed().as_nanos();

        // Convert NodePriority to scheduler Priority
        use crate::custom_scheduler::Priority;
        use crate::graph_struct::NodePriority;

        let task_priority = match node_priority {
            NodePriority::High => Priority::High,
            NodePriority::Normal => Priority::Normal,
            NodePriority::Low => Priority::Low,
        };

        // Create the task closure
        let task = move || {
            execute_task(
                &shared_clone,
                func_ptr,
                &node_info,
                &time_buf,
                &node_name,
                pre_built_args,
                spawn_ns,
            )
        };

        // Route task based on use_workers affinity
        // - None: Use global queue (group 0 - any available workers)
        // - Some(Count(N)): Use global queue (group 0 - any N available workers)
        // - Some(Range(start-end)): Route to dedicated exclusive group for that range
        let affinity_group = shared
            .scheduler
            .get_affinity_group(node_use_workers.as_ref());

        if affinity_group > 0 {
            // Range-based affinity - spawn to dedicated exclusive group
            // These workers ONLY handle tasks with this specific range
            shared.scheduler.spawn_to_group_with_meta(
                affinity_group,
                task_priority,
                Some(meta_data),
                task,
            );
        } else {
            // No affinity OR count-based - spawn to global queue
            // Global workers handle: None specs, Count specs, and any non-range tasks
            shared
                .scheduler
                .spawn_task_with_meta_priority(task_priority, Some(meta_data), task);
        }
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
        let node_factor = shared.graph.nodes[node_info.id as usize].factor;
        let result = &collect_arg_result(
            arg,
            node_info.id,
            node_info.index,
            node_factor,
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

/// Evaluate node-level condition (new format)
/// Returns true if condition passes (node should be scheduled)
#[inline]
pub fn evaluate_node_condition(
    shared: &Arc<SharedData>,
    node_info: &NodeInfo,
    cond_cache: &NodeConditionCache,
    node_cond: &NodeCondition,
) -> bool {
    // Build condition args using cached arg data
    let cond_args = parse_cached_args(
        shared,
        &cond_cache.arg_cache,
        node_info.id,
        node_info.index,
        node_info.slot,
        node_info.pred_index,
        None,
    );

    // Execute condition function to get result
    let cond_result = (cond_cache.func_ptr)(&cond_args);

    // Evaluate result against expected value using operation
    node_cond.evaluate(&cond_result)
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
        let slot_states = shared.slot_states.read();
        slot_states
            .iter()
            .enumerate()
            .filter(|(s_id, &state)| {
                *s_id != slot && (state == SlotState::Active || state == SlotState::Buffering)
            })
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
        println!(
                "SynRt -- Slot {} completed stream. Starting new: completed={}, active={}, total={}, max={}",
                slot,
                completed_streams,
                currently_active_streams,
                total_streams_processed,
                shared.max_streams
            );

        // Release the slot
        release_slot(shared, slot);

        // Clear completed nodes for this slot to allow restart - lock-free atomic clear
        shared
            .node_results
            .reinit_slot(&shared.graph.nodes, slot, None);

        true // Signal to caller: slot should restart
    } else {
        println!(
            "SynRt -- Slot {} completed. Max streams ({}) reached: completed={}, active={}",
            slot, shared.max_streams, completed_streams, currently_active_streams
        );

        // Release the slot
        release_slot(shared, slot);

        false // Signal to caller: no restart needed
    }
}

#[inline]
pub fn assign_stream_to_available_slot(shared: &Arc<SharedData>, stream: usize) -> (usize, bool) {
    // Get write access to have updated view of running streams
    let mut running_streams = shared.running_streams.write();

    // Check if this stream is already mapped to a slot
    for (stream_id, slot_id) in running_streams.iter() {
        if *stream_id == stream {
            return (*slot_id, false); // Already assigned, not newly activated
        }
    }

    let last_slot_assigned = shared.last_slot_assigned.load(Ordering::SeqCst);
    let mut slot_states = shared.slot_states.write();

    // Check last assigned first
    if slot_states[last_slot_assigned] == SlotState::Inactive {
        slot_states[last_slot_assigned] = SlotState::Active; // Mark slot as active immediately
        running_streams.push((stream, last_slot_assigned));
        print_debug(|| {
            format!(
                "Assigned stream {} to slot {} (Inactive) -> Active (last assigned)",
                stream, last_slot_assigned
            )
        });
        drop(running_streams); // Release lock before returning

        // Start timing for the slot immediately upon assignment
        if let Some(tb) = &shared.time_buffer {
            tb.start_slot_processing(last_slot_assigned);
        }

        return (last_slot_assigned, true); // Newly activated from Inactive → Active
    }

    for i in 1..shared.slots {
        let slot_id = (last_slot_assigned + i) % shared.slots;
        let state = slot_states.get_mut(slot_id).unwrap();
        if *state == SlotState::Inactive {
            *state = SlotState::Buffering; // Mark slot as Buffering
            running_streams.push((stream, slot_id));
            shared.last_slot_assigned.store(slot_id, Ordering::SeqCst);
            print_debug(|| {
                format!(
                    "Assigned stream {} to slot {} (Inactive) -> Buffering",
                    stream, slot_id
                )
            });
            drop(running_streams); // Release lock before returning
            return (slot_id, false); // Assigned but Buffering, not Active
        }
    }

    panic!("No available slots to assign stream: {}", stream);
}

pub fn release_slot(shared: &Arc<SharedData>, slot: usize) {
    let mut running_streams = shared.running_streams.write();
    let mut slot_states = shared.slot_states.write();

    let old_state = slot_states[slot];
    slot_states[slot] = SlotState::Inactive; // Mark as inactive

    // Remove from running streams
    if let Some(pos) = running_streams.iter().position(|&(_, s_id)| s_id == slot) {
        let (stream_id, _) = running_streams.remove(pos);
        print_debug(|| {
            format!(
                "Released slot {} from stream {} (had state: {:?})",
                slot, stream_id, old_state
            )
        });
    } else {
        print_debug(|| {
            format!(
                "Released slot {} with no assigned stream (had state: {:?})",
                slot, old_state
            )
        });
    }
    drop(slot_states);
    drop(running_streams);
}

#[inline]
pub fn process_id_function(shared: &Arc<SharedData>, result: &CmTypes) -> Option<usize> {
    let network_config_opt = shared.graph.network_config();

    if let Some(network_config) = network_config_opt {
        let id_function = network_config.id_function.unwrap();
        // Call the id function - wrap single result in Vec as expected by signature
        let id_result = id_function(&[result.clone()]);

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
    } else {
        None
    }
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
    let args_cache = &node.arg_cache;

    // All argument resolution is handled uniformly in parse_cached_args
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
        shared.workers
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

        let node_factor = shared.graph.nodes[node_id as usize].factor;
        let result_opt = collect_arg_result(
            arg,
            node_id,
            node_index,
            node_factor,
            slot,
            pred_index,
            custom_res,
            shared,
        );
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

/// Populate args directly into a provided buffer, avoiding heap allocation.
/// Mirrors `parse_cached_args` but reuses the caller's Vec instead of allocating a new one.
#[inline(always)]
fn populate_cached_args_into(
    buf: &mut Vec<CmTypes>,
    shared: &Arc<SharedData>,
    args_cache: &ArgCacheEntry,
    node_id: IdType,
    node_index: usize,
    slot: usize,
    pred_index: usize,
) {
    buf.extend(args_cache.args.iter().cloned());

    if args_cache.buffer_ref_indexes.is_empty()
        && args_cache.rt_idxs_indexes.is_empty()
        && args_cache.rt_workers_indexes.is_empty()
        && args_cache.res_indexes.is_empty()
    {
        return;
    }

    let workers = if !args_cache.rt_workers_indexes.is_empty() {
        shared.workers
    } else {
        0
    };

    process_buffer_refs(buf, args_cache, node_index);
    process_runtime_refs(buf, args_cache, node_index, workers);

    for (res_idx, real_idx) in args_cache
        .res_indexes
        .iter()
        .zip(args_cache.real_res_indexes.iter())
    {
        let arg = shared.graph.nodes[node_id as usize]
            .args
            .get(*real_idx)
            .expect("Argument index out of bounds");

        let node_factor = shared.graph.nodes[node_id as usize].factor;
        let result_opt = collect_arg_result(
            arg,
            node_id,
            node_index,
            node_factor,
            slot,
            pred_index,
            None,
            shared,
        );
        if let Some(mut result) = result_opt {
            if result.len() == 1 {
                buf[*res_idx] = result.remove(0);
            } else {
                buf.splice(*res_idx..*res_idx + 1, result);
            }
        }
    }
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

        let result_opt =
            collect_arg_result(arg, 0, node_index, 0, slot, pred_index, custom_res, shared);
        if let Some(result) = result_opt {
            arg_vec.extend(result);
        }
    }
    arg_vec
}

#[inline(always)]
fn handle_special_ref(obj_id: usize, node_index: usize, workers: usize) -> Option<Vec<CmTypes>> {
    match obj_id {
        0 => Some(vec![CmTypes::Usize(node_index)]),
        1 => Some(vec![CmTypes::Usize(workers)]),
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
    node_id: IdType,
    node_index: usize,
    node_factor: usize,
    slot: usize,
    pred_index: usize,
    custom_res: Option<&CmTypes>,
    shared: &Arc<SharedData>,
) -> Option<Vec<CmTypes>> {
    match &arg.type_ {
        CmTypes::Ref(obj_id) => {
            let obj_id = *obj_id;
            if let Some(result) = handle_special_ref(obj_id, node_index, shared.workers) {
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

            // Single explicit index: use the declared index, NOT pred_index.
            // The triggering predecessor may differ from the $res predecessor
            // (e.g., demul's $res reads fft[0] but demul can be triggered by beam).
            if predecessor.indexes.len() == 1 {
                let res_node = &shared.graph.nodes[*res_node_id as usize];
                let res_factor = res_node.factor;
                let current_node = &shared.graph.nodes[node_id as usize];

                let dep_idx = if let Some(ngs) = current_node.group_size {
                    // Current node is grouped: map through symbol level.
                    // symbol = which group/symbol this instance belongs to
                    let symbol = node_index / ngs;
                    // Predecessor's effective group size: its own group_size,
                    // or the barrier's group_by, or fall back to full factor
                    let pred_eff_gs = res_node.group_size.unwrap_or_else(|| {
                        shared.pred_group_by[node_id as usize][*res_node_id as usize]
                            .unwrap_or(res_factor)
                    });
                    let offset = predecessor.indexes[0] as usize;
                    symbol * pred_eff_gs + offset
                } else {
                    find_pred_index(node_index, predecessor.indexes[0], res_factor)
                };

                let node_info = NodeInfo::new(*res_node_id as IdType, slot, dep_idx, 0);
                if let Some(result) = shared.node_results.get(&node_info) {
                    return Some(vec![result]);
                } else {
                    panic!(
                        "Missing result for node_info: {:?} when collecting argument",
                        node_info
                    );
                }
            }

            // 1:1 mapping: indexes.len() == node_factor means each instance
            // reads exactly one predecessor result via pred_index (the triggering
            // predecessor IS the $res predecessor in this case).
            if predecessor.indexes.len() > 1 && predecessor.indexes.len() == node_factor {
                let res_node = &shared.graph.nodes[*res_node_id as usize];
                let res_factor = res_node.factor;
                let node_info =
                    NodeInfo::new(*res_node_id as IdType, slot, pred_index % res_factor, 0);
                if let Some(result) = shared.node_results.get(&node_info) {
                    return Some(vec![result]);
                } else {
                    panic!(
                        "Missing result for node_info: {:?} when collecting argument",
                        node_info
                    );
                }
            }

            // Collect-all path: factor != indexes.len() (e.g., write_res)
            let pred_node = &shared.graph.nodes[predecessor.id as usize];
            let pred_factor = pred_node.factor;

            // Pre-allocate vectors
            let mut indices = Vec::with_capacity(predecessor.indexes.len());
            for &pred_idx in predecessor.indexes.iter() {
                indices.push(find_pred_index(node_index, pred_idx, pred_factor));
            }

            // Lock-free atomic loads - no RwLock contention
            let mut result_vec = Vec::with_capacity(indices.len());

            // Batch collect all results
            for dep_idx in indices.iter() {
                let node_info = NodeInfo::new(*res_node_id as IdType, slot, *dep_idx, 0);
                if let Some(result) = shared.node_results.get(&node_info) {
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
    let states = shared.slot_states.read();
    states[slot] == SlotState::Active
}

/// Activate the next buffering slot in round-robin order
/// Returns (activated_slot_id, buffered_nodes) for processing
/// When slot-priority is enabled, automatically uses round-robin activation
pub fn activate_next_slot(
    shared: &Arc<SharedData>,
    completing_slot: Option<usize>,
) -> Option<(usize, Vec<(NodeInfo, CmTypes)>)> {
    if !shared.slot_priority_enabled {
        return None;
    }

    // 1. Acquire running_streams (Read) FIRST
    let running_streams = shared.running_streams.read();

    // 2. Then acquire slot_states (Write)
    let mut states = shared.slot_states.write();

    // Find and activate next buffering slot in round-robin order
    let activated_slot = if let Some(completed) = completing_slot {
        let mut found_slot = None;
        // We can safely iterate running_streams while holding the lock
        for (stream, slot) in running_streams.iter() {
            if states[*slot] == SlotState::Buffering {
                states[*slot] = SlotState::Active;
                shared.last_slot_assigned.store(*slot, Ordering::SeqCst);
                print_debug(|| {
                    format!(
                        "Round-Robin: Activated slot {} for stream {} after completing slot {}",
                        slot, stream, completed
                    )
                });
                found_slot = Some(*slot);
                break; // Activate only ONE slot per completion
            }
        }
        found_slot
    } else {
        None
    };

    // Retrieve buffered nodes while still holding slot_states lock
    if let Some(slot_id) = activated_slot {
        let mut slot_buffers = shared.slot_buffers.write();
        let buffered = std::mem::take(&mut slot_buffers[slot_id]);

        // Drop locks in LIFO order
        drop(slot_buffers);
        drop(states);
        drop(running_streams); // Release the first lock last

        if let Some(tb) = &shared.time_buffer {
            tb.start_slot_processing(slot_id);
        }

        Some((slot_id, buffered))
    } else {
        drop(states);
        drop(running_streams);
        None
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

/// Check if we should record for a given slot based on its current stream ID.
/// Returns true if recording is enabled for all streams (None) or if the slot's
/// current stream matches the target stream.
#[inline(always)]
pub fn should_record_slot(shared: &Arc<SharedData>, slot: usize) -> bool {
    match shared.record_stream {
        None => true, // Record all streams
        Some(target_stream) => {
            // Get current stream for this slot
            let running_streams = shared.running_streams.read();
            for (stream_id, slot_id) in running_streams.iter() {
                if *slot_id == slot {
                    return *stream_id == target_stream;
                }
            }
            false
        }
    }
}

/// Sequential helper: Collect successors for a single node (read-only)
///
/// This function extracts the inner loop from the original sequential resolution
/// loop, enabling parallel processing. It contains no side effects - only reads
/// from immutable graph/cache structures and performs atomic loads (with proper
/// Acquire ordering for synchronization).
///
/// # Arguments
/// * `shared` - Shared runtime data
/// * `node_info` - Information about the node being processed
///
/// # Returns
/// Vector of (successor_node_info, has_condition, successor_id) tuples
/// representing all successors of the given node that have remaining dependencies.
#[inline]
/// Collect successor descriptors for `node_info`, appending into `out` (cleared first).
/// Avoids a heap allocation on the hot path when the caller supplies a reusable buffer.
pub fn collect_successors_for_node_into(
    shared: &Arc<SharedData>,
    node_info: &NodeInfo,
    out: &mut Vec<(NodeInfo, bool, IdType, Option<usize>)>,
) {
    out.clear();

    let node_id_usize = node_info.id as usize;

    // Get successor list for this node (immutable, pre-computed)
    let successors: &Vec<IdType> = {
        if node_id_usize >= shared.graph.successors.len() {
            &Vec::new()
        } else {
            &shared.graph.successors[node_id_usize]
        }
    };

    // Collect info for each successor without locks
    for succ_id in successors {
        let succ_id = *succ_id;
        let succ_id_usize = succ_id as usize;

        // Predecessor index range filter: skip if this predecessor instance is outside
        // the declared index range for this successor
        if let Some(Some((start, end))) = shared
            .pred_index_filter
            .get(succ_id_usize)
            .and_then(|v| v.get(node_id_usize))
        {
            if node_info.index < *start || node_info.index >= *end {
                continue; // Predecessor instance outside declared range
            }
        }

        let succ_cache = &shared.node_cache[succ_id_usize];

        // Use pre-computed flag for lock-free check
        let has_condition = succ_cache.is_condition;

        // Compute predecessor group for group_by barriers
        let pred_group: Option<usize> = {
            if let Some(Some(gb)) = shared
                .pred_group_by
                .get(succ_id_usize)
                .and_then(|v| v.get(node_id_usize))
            {
                // Compute relative index within the declared range
                let range_start = shared
                    .pred_index_filter
                    .get(succ_id_usize)
                    .and_then(|v| v.get(node_id_usize))
                    .and_then(|f| f.map(|(s, _)| s))
                    .unwrap_or(0);
                let relative_idx = node_info.index - range_start;
                Some(relative_idx / gb)
            } else {
                None // No group_by → global decrement
            }
        };

        // Determine which indices of the successor to create.
        let succ_indexes = {
            if pred_group.is_some() {
                // Group-based dependency: placeholder entry (index 0) for decrement
                vec![0]
            } else if node_info.id == 0 {
                // $network node: 1:1 index mapping for pred_index_filter routing
                vec![node_info.index]
            } else {
                // Single entry per (successor, pred_group) pair
                vec![0]
            }
        };

        // Add successor node info for each instance
        for succ_index in succ_indexes {
            let succ_info = NodeInfo::new(succ_id, node_info.slot, succ_index, node_info.index);
            out.push((succ_info, has_condition, succ_id, pred_group));
        }
    }
}

pub fn collect_successors_for_node(
    shared: &Arc<SharedData>,
    node_info: &NodeInfo,
) -> Vec<(NodeInfo, bool, IdType, Option<usize>)> {
    let mut out = Vec::new();
    collect_successors_for_node_into(shared, node_info, &mut out);
    out
}
