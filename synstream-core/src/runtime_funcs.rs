use crate::debug::print_debug;
use crate::network::{NetworkSocket, PacketMessage};
use crate::resolution_state::ResolutionState;
use crate::time_buffer::{TimeBufferManager, TimingMethod};
use crate::{buffers::*, graph::*, graph_struct::*, scheduler::*, IdType};
use core::panic;
use flume::{Receiver, Sender};
use parking_lot::{Mutex, RwLock};
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use synstream_types::*;

/// Type aliases for the crossbeam batch_queue channel used by workers → resolution threads.
pub type BatchQueueTx = crossbeam_channel::Sender<crate::buffers::NodeInfo>;
pub type BatchQueueRx = crossbeam_channel::Receiver<crate::buffers::NodeInfo>;

thread_local! {
    static ARG_BUF: RefCell<Vec<CmTypes>> = RefCell::new(Vec::with_capacity(16));
    // Stale-task signaling for collect_arg_result → execute_task.
    // Set by collect_arg_result when a gen mismatch is detected during arg collection.
    // Checked by execute_task after populate_cached_args_into returns.
    static STALE_TASK_DETECTED: RefCell<bool> = RefCell::new(false);
    static EXECUTING_SLOT: RefCell<usize> = RefCell::new(usize::MAX);
    static EXECUTING_GEN: RefCell<u32> = RefCell::new(0);

    // Worker-side dependency resolution buffers.
    // Used by worker_resolve_successors to avoid heap allocation on the hot path.
    static WORKER_SUCC_BUF: RefCell<Vec<(NodeInfo, bool, IdType, Option<usize>)>> =
        RefCell::new(Vec::with_capacity(32));
    static WORKER_READY_BUF: RefCell<Vec<usize>> = RefCell::new(Vec::with_capacity(32));
    static WORKER_SCHED_BUF: RefCell<Vec<NodeInfo>> = RefCell::new(Vec::with_capacity(32));
    static WORKER_ARGS_BUF: RefCell<Vec<Option<Vec<CmTypes>>>> = RefCell::new(Vec::with_capacity(32));

    /// Set by worker_resolve_successors when inline_continuation is enabled.
    /// Consumed by the send_to_scheduler trampoline after execute_task returns.
    static INLINE_CONTINUATION: RefCell<Option<NodeInfo>> = RefCell::new(None);
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
    // Pre-computed scheduler priority (avoids per-task conversion from NodePriority)
    pub priority: crate::custom_scheduler::Priority,
    // Pre-computed scheduler affinity group (avoids per-task use_workers.clone() + lookup)
    pub affinity_group: usize,
    // Pre-computed flag: true if all successors are non-condition nodes,
    // meaning worker threads can resolve dependencies directly without
    // going through the resolution thread's batch_queue.
    pub worker_resolvable: bool,
    // Pre-computed flag: true if any successor reads this node's result via $res.
    // When false, no successor consumes the result and the node_results.set() call
    // can be elided entirely, saving a hash-map write on the hot path.
    pub needs_result_store: bool,
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
            priority: crate::custom_scheduler::Priority::Normal,
            affinity_group: 0,
            worker_resolvable: false,
            needs_result_store: false, // Computed later in SynRt::new
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
            CmTypes::Res(_) | CmTypes::Dep(_) => {
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
                CmTypes::Res(_) | CmTypes::Dep(_) => {
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
        // Defaults; overwritten in SynRt::new after scheduler is available
        priority: crate::custom_scheduler::Priority::Normal,
        affinity_group: 0,
        worker_resolvable: false, // Computed in SynRt::new after successors are known
        needs_result_store: false, // Computed in SynRt::new after successors are known
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

    // Crossbeam bounded channel for lock-free task completion delivery.
    // Ring-buffer internals (no per-send Box::new). Workers pre-store results in
    // node_results before sending, so only the NodeInfo token travels through the
    // queue (no CmTypes copy in the hot path).
    pub batch_queue_tx: BatchQueueTx,
    pub batch_queue_rx: BatchQueueRx,
    pub target_batch_size: usize,
    pub batch_timeout_us: u64,

    // Resolution state - abstracted for single vs multi-threaded
    pub resolution_state: Arc<dyn ResolutionState>,

    pub initial_prep_done: Arc<AtomicUsize>,

    pub slot_pending_tasks: Arc<Vec<AtomicUsize>>,
    pub slot_pending_cond_tasks: Arc<Vec<AtomicUsize>>,

    // Condition node spawn tracking - optimization to skip evaluation when all instances spawned.
    // Each AtomicU64 packs (gen: u32, remaining_spawns: u32). Generation mismatch triggers
    // lazy reinit to nc.factor, eliminating the O(cond_nodes) reset loop at slot completion.
    pub cond_instances_to_spawn: Arc<Vec<Vec<AtomicU64>>>,

    // Slot generation counter - incremented on slot completion to lazily reinitialise all
    // NodeDependencyEntry and cond_instances_to_spawn entries for that slot.
    // Upper 32 bits unused; lower 32 bits used as u32 generation ID.
    pub slot_generation: Arc<Vec<AtomicU64>>,

    // Slot priority processing state
    pub slot_states: Arc<RwLock<Vec<SlotState>>>,
    pub last_slot_assigned: Arc<AtomicUsize>,
    pub slot_priority_enabled: bool,
    // Per-slot buffering: holds ready nodes with their packet data waiting for slot activation.
    // CmTypes is Some(result) for network packets (result inline), None for compute tasks (pre-stored).
    pub slot_buffers: Arc<RwLock<Vec<Vec<(NodeInfo, Option<CmTypes>)>>>>,

    // Network receiver infrastructure (optional - only present if network_config exists)
    pub receive_finished: Arc<AtomicBool>,
    /// Flume MPSC channel from network receivers to resolution threads
    pub packet_sender: Sender<PacketMessage>,
    pub packet_receiver: Receiver<PacketMessage>,
    pub receiver_sockets: Vec<NetworkSocket>,
    pub packet_drop_counters: Vec<AtomicUsize>,
    pub shutdown_flag: Arc<AtomicBool>,
    /// Per-socket buffer return channels: resolution thread → receiver thread.
    /// After packet_process_func returns, resolution sends the reclaimed buffer to the
    /// originating receiver's return channel instead of a shared mutex pool.
    /// Indexed by socket_id. Eliminates all shared-mutex contention on the receive hot path.
    pub buffer_return_senders: Vec<Sender<Vec<u8>>>,
    /// Receiver ends of the per-socket return channels.
    /// Each entry is taken exactly once when the corresponding receiver thread is spawned.
    pub buffer_return_receivers: Vec<Mutex<Option<Receiver<Vec<u8>>>>>,

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

    /// pred_succ_1to1_offset[succ_id][pred_id] = Some(k) for non-barrier, single-index $res deps
    /// where succ_factor == pred_factor. k = indexes[0]. Used to compute the specific successor
    /// instance that reads a completing predecessor: specific_idx = (pred_idx - k + f) % f.
    /// This ensures 1:1 pipelines dispatch the correct successor without spin_wait deadlock.
    pub pred_succ_1to1_offset: Arc<Vec<Vec<Option<isize>>>>,

    // Graph-constant slot completion thresholds — computed once at init, used in check_slots.
    pub total_tasks: usize,
    pub total_cond_tasks: usize,

    // Per-slot stream ID for lock-free should_record_slot — avoids running_streams RwLock.
    // usize::MAX means no stream assigned.
    pub slot_stream_id: Arc<Vec<AtomicUsize>>,

    // Bitmap of slots currently in Active state — avoids per-slot RwLock read in check_slots.
    // Bit i is set iff slot i is Active. Updated under slot_states write lock.
    pub active_slots_bitmap: Arc<AtomicU64>,

    /// When true, barrier fan-outs with N > workers ready instances are chunked into
    /// `workers` bulk tasks instead of N individual ones.  Only enable for fine-grained
    /// workloads (per-task compute << spawn overhead, e.g. wavefront ~5 ns/cell).
    /// Leave false (default) for coarse-grained workloads (MIMO, PageRank) where
    /// serialising instances inside a bulk task would increase latency.
    pub coalesce_barriers: bool,

    /// When true, after resolving successors in worker_resolve_successors, one ready
    /// successor is reserved for inline execution on the current worker thread instead of
    /// spawning all via the scheduler.  Eliminates scheduler round-trip for chain-dominant
    /// subgraphs (factor=1 chains A→B→C→…).  Must NOT enable for coarse-grained
    /// workloads (MIMO) where serialising a successor increases slot-window latency.
    pub inline_continuation: bool,

    /// When slots == 1, only one stream runs at a time — no cross-stream ordering races
    /// are possible.  Enables AcqRel instead of SeqCst for the per-task hot-path atomics
    /// in worker_resolve_successors, and skips the stale-task TLS writes in execute_task.
    /// Multi-slot mode keeps SeqCst unconditionally for correctness.
    pub single_slot_mode: bool,
}

/// When a barrier node's instances all become ready simultaneously, this helper
/// creates `min(ready.len(), num_workers)` bulk `NodeInfo`s instead of one per instance.
/// Requires that ready indices form a contiguous range (guaranteed for single-group barriers).
/// Falls back to individual dispatch for small fan-outs or non-contiguous indices.
fn push_ready_chunked(
    ready: &[usize],
    succ_id: IdType,
    slot: usize,
    pred_index: usize,
    num_workers: usize,
    coalesce: bool,
    sched: &mut Vec<NodeInfo>,
) {
    if ready.is_empty() {
        return;
    }
    let start = ready[0];
    let contiguous = ready.iter().enumerate().all(|(i, &r)| r == start + i);

    if coalesce && contiguous && num_workers > 0 && ready.len() > num_workers {
        // Chunk into num_workers bulk tasks
        let total = ready.len();
        let num_chunks = num_workers;
        let base = total / num_chunks;
        let extra = total % num_chunks;
        let mut offset = start;
        for c in 0..num_chunks {
            let count = base + if c < extra { 1 } else { 0 };
            let mut ni = NodeInfo::new(succ_id, slot, offset, pred_index);
            ni.bulk_count = count;
            sched.push(ni);
            offset += count;
        }
    } else {
        for &idx in ready {
            sched.push(NodeInfo::new(succ_id, slot, idx, pred_index));
        }
    }
}

/// Returns the appropriate load ordering for slot completion counters.
///
/// With `single_slot_mode` (slots == 1) only one stream runs at a time, so AcqRel
/// pairwise synchronisation is sufficient.  Multi-slot mode requires SeqCst for total
/// ordering across concurrent slot reinitialisation.
#[inline(always)]
fn slot_load_ordering(shared: &SharedData) -> Ordering {
    if shared.single_slot_mode {
        Ordering::Acquire
    } else {
        Ordering::SeqCst
    }
}

/// Returns the appropriate read-modify-write ordering for slot completion counters.
#[inline(always)]
fn slot_rmw_ordering(shared: &SharedData) -> Ordering {
    if shared.single_slot_mode {
        Ordering::AcqRel
    } else {
        Ordering::SeqCst
    }
}

/// Worker-side dependency resolution: resolves successors directly on the worker
/// thread that completed the task, bypassing the batch_queue → resolution thread
/// round-trip. Only called for nodes where all successors are non-condition
/// (worker_resolvable == true), ensuring correctness without condition evaluation.
#[inline]
fn worker_resolve_successors(shared: &Arc<SharedData>, node_info: &NodeInfo) {
    let slot = node_info.slot;

    // Step 1: Increment processing_count to prevent premature completion detection.
    shared.slot_processing_count[slot].fetch_add(1, slot_rmw_ordering(shared));

    // Step 2: Verify generation — if slot was recycled, bail out.
    let current_gen = shared.slot_generation[slot].load(slot_load_ordering(shared)) as u32;
    if current_gen != node_info.gen {
        shared.slot_processing_count[slot].fetch_sub(1, slot_rmw_ordering(shared));
        return;
    }

    // Step 3: Decrement task counters (Phase 2 equivalent).
    // For bulk tasks, decrement by bulk_count to account for all instances handled.
    let node_cache_entry = &shared.node_cache[node_info.id as usize];
    if node_cache_entry.is_condition {
        shared.slot_pending_cond_tasks[slot].fetch_sub(node_info.bulk_count, slot_rmw_ordering(shared));
    } else if !node_cache_entry.is_initial {
        shared.slot_pending_tasks[slot].fetch_sub(node_info.bulk_count, slot_rmw_ordering(shared));
    }

    // Steps 4-6: Collect successors, resolve dependencies, schedule ready nodes.
    WORKER_SUCC_BUF.with(|sbuf| {
        WORKER_READY_BUF.with(|rbuf| {
            WORKER_SCHED_BUF.with(|tbuf| {
                WORKER_ARGS_BUF.with(|abuf| {
                    let mut succ_buf = sbuf.borrow_mut();
                    let mut ready = rbuf.borrow_mut();
                    let mut sched = tbuf.borrow_mut();
                    let mut args_buf = abuf.borrow_mut();
                    sched.clear();

                    // Step 4: Collect successors.
                    collect_successors_for_node_into(shared, node_info, &mut succ_buf);

                    // Load slot generation once for all successors.
                    let slot_gen = shared.slot_generation[slot].load(slot_load_ordering(shared)) as u32;

                    // Step 5: Resolve dependencies for each successor (all non-condition).
                    for (_succ_info, _has_cond, succ_id, pred_group) in succ_buf.iter() {
                        let succ_node_id = *succ_id as usize;

                        // For 1:1 non-barrier deps, fire the specific successor instance
                        // that reads this predecessor (result guaranteed available).
                        let specific_succ_idx = shared
                            .pred_succ_1to1_offset
                            .get(succ_node_id)
                            .and_then(|v| v.get(node_info.id as usize))
                            .and_then(|o| *o)
                            .map(|k| {
                                let f = shared.node_cache[succ_node_id].factor;
                                ((node_info.index as isize - k).rem_euclid(f as isize)) as usize
                            });

                        shared.resolution_state.decrease_and_get_ready_into(
                            slot,
                            succ_node_id,
                            slot_gen,
                            *pred_group,
                            node_info.bulk_count,
                            specific_succ_idx,
                            &mut ready,
                        );

                        push_ready_chunked(
                            &ready,
                            succ_node_id as IdType,
                            slot,
                            node_info.index,
                            shared.workers,
                            shared.coalesce_barriers,
                            &mut sched,
                        );
                    }

                    // Inline continuation: reserve one ready successor for this worker
                    // thread instead of spawning it through the scheduler.
                    // Stamp slot_gen so the trampoline's stale check passes on streams > 0.
                    let inline = if shared.inline_continuation && !sched.is_empty() {
                        sched.pop().map(|mut ni| { ni.gen = slot_gen; ni })
                    } else {
                        None
                    };

                    // Step 6: Schedule remaining ready successors.
                    if !sched.is_empty() {
                        args_buf.clear();
                        args_buf.resize(sched.len(), None);
                        send_to_scheduler(shared, &sched, &args_buf, None);
                    }

                    if let Some(ni) = inline {
                        INLINE_CONTINUATION.with(|c| *c.borrow_mut() = Some(ni));
                    }
                });
            });
        });
    });

    // Step 7: Decrement processing_count AFTER all successor processing.
    shared.slot_processing_count[slot].fetch_sub(1, slot_rmw_ordering(shared));
}

#[inline(always)]
fn execute_task(
    shared: &Arc<SharedData>,
    func: CmPtr,
    node_info: &NodeInfo,
    pre_built_args: Option<Vec<CmTypes>>,
    spawn_ns: u128,
) {
    // Stale-task guard: if the slot's generation has advanced since this task was
    // scheduled, the slot was recycled (stream completed + reassigned) while the
    // task sat in the Rayon queue.  Executing it would read cleared predecessor
    // results → panic, or corrupt the new stream's dependency counters.
    // Post-nodes are exempt (gen is always 0 and they run after all streams finish).
    if !node_info.post_node {
        let current_gen =
            shared.slot_generation[node_info.slot].load(Ordering::Acquire) as u32;
        if current_gen != node_info.gen {
            print_debug(|| {
                format!(
                    "Stale task dropped: node {} slot {} index {} gen {} (current {})",
                    node_info.id, node_info.slot, node_info.index,
                    node_info.gen, current_gen
                )
            });
            return;
        }
    }

    // Bulk execute path: run multiple consecutive instances in a tight loop on this worker.
    // Spawned by push_ready_chunked when a barrier fan-out produces N > num_workers ready
    // instances simultaneously (e.g. wavefront diagonal completion).  Each bulk task covers
    // a contiguous range `index..index+bulk_count`, eliminating O(N) individual Rayon spawns.
    if node_info.bulk_count > 1 {
        // Set stale-detection TLS context — required by populate_cached_args_into.
        // In single_slot_mode mid-execution slot recycling is impossible; skip TLS writes.
        if !shared.single_slot_mode {
            STALE_TASK_DETECTED.with(|f| *f.borrow_mut() = false);
            EXECUTING_SLOT.with(|s| *s.borrow_mut() = node_info.slot);
            EXECUTING_GEN.with(|g| *g.borrow_mut() = node_info.gen);
        }

        let node_cache = &shared.node_cache[node_info.id as usize];
        ARG_BUF.with(|buf_cell| {
            let mut buf = buf_cell.borrow_mut();
            for inst_idx in node_info.index..node_info.index + node_info.bulk_count {
                buf.clear();
                populate_cached_args_into(
                    &mut buf,
                    shared,
                    &node_cache.arg_cache,
                    node_info.id,
                    inst_idx,
                    node_info.slot,
                    node_info.pred_index,
                );
                if !shared.single_slot_mode && STALE_TASK_DETECTED.with(|f| *f.borrow()) {
                    buf.clear();
                    return; // Slot recycled mid-bulk — drop remaining instances
                }
                let result = func(&buf);
                buf.clear(); // release Arc refs promptly
                // Store result for this specific instance using a per-instance NodeInfo
                if node_cache.needs_result_store {
                    let mut inst_info = node_info.clone();
                    inst_info.index = inst_idx;
                    inst_info.bulk_count = 1;
                    shared.node_results.set(&inst_info, result);
                }
            }
        });

        // Stale check: if slot was recycled during bulk execution, skip completion accounting
        if !shared.single_slot_mode && STALE_TASK_DETECTED.with(|f| *f.borrow()) {
            return;
        }

        // Single call to worker_resolve_successors accounts for all bulk_count instances
        // via the bulk_count-aware counter decrements and decrease_and_get_ready_into call.
        worker_resolve_successors(shared, node_info);
        return;
    }

    // Initialize stale-detection context for collect_arg_result.
    // In single_slot_mode, mid-execution slot recycling is impossible; skip TLS writes.
    if !shared.single_slot_mode {
        STALE_TASK_DETECTED.with(|f| *f.borrow_mut() = false);
        EXECUTING_SLOT.with(|s| *s.borrow_mut() = node_info.slot);
        EXECUTING_GEN.with(|g| *g.borrow_mut() = node_info.gen);
    }

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

    // Look up time_buf from shared (avoids Option<Arc> clone per task in closure capture)
    let time_buf = &shared.time_buffer;

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
        let result_opt = ARG_BUF.with(|buf_cell| {
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
            // Check stale BEFORE calling func (result missing due to reinit_slot race)
            if !shared.single_slot_mode && STALE_TASK_DETECTED.with(|f| *f.borrow()) {
                buf.clear();
                return None::<CmTypes>;
            }
            let r = func(&buf);
            buf.clear(); // release Arc refs promptly
            Some(r)
        });
        match result_opt {
            Some(r) => r,
            None => {
                print_debug(|| {
                    format!(
                        "Stale task dropped during arg collection: node {} slot {} index {} gen {}",
                        node_info.id, node_info.slot, node_info.index, node_info.gen
                    )
                });
                return; // Silently drop — slot was recycled during arg collection
            }
        }
    };

    if let Some(start) = start_time {
        if let Some(tb) = time_buf {
            let end_time = tb.measure_time();
            let duration = tb.measure_duration(start, end_time);
            // Look up node_name from cache (avoids String clone per task in closure capture)
            let node_name = &shared.node_cache[node_info.id as usize].name;
            tb.add_task_time(node_info.slot, node_name, worker_id, duration);
        }
    }

    // Pre-store result in node_results before resolving or sending completion token.
    // Skip the store when no successor reads this result via $res (barrier-only successors).
    if shared.node_cache[node_info.id as usize].needs_result_store {
        shared.node_results.set(node_info, result);
    }

    // Worker-side dependency resolution: if all successors are non-condition,
    // resolve dependencies and schedule successors directly on this worker thread,
    // bypassing the batch_queue → resolution thread round-trip (~5-15μs → ~100-500ns).
    if !node_info.post_node && shared.node_cache[node_info.id as usize].worker_resolvable {
        worker_resolve_successors(shared, node_info);
    } else {
        // Send the lightweight NodeInfo token. Use blocking send (not try_send) so that
        // a full queue causes the worker to wait rather than silently dropping the token.
        // Dropping a token would leave slot_pending_tasks > 0 forever → hang.
        let _ = shared.batch_queue_tx.send(node_info.clone());
    }
}

#[inline]
pub fn send_to_scheduler(
    shared: &Arc<SharedData>,
    nodes_to_schedule: &Vec<NodeInfo>,
    pre_built_args_vec: &[Option<Vec<CmTypes>>],
    custom_func_vec: Option<&[Option<CmPtr>]>,
) {
    for (i, node_info) in nodes_to_schedule.iter().enumerate() {
        // Look up func_ptr, priority, and affinity from pre-computed cache.
        // Post-nodes use the cold path since they're rare (end-of-run only).
        let custom_func = custom_func_vec.and_then(|v| v[i]);
        let (func_ptr, task_priority, affinity_group) = if node_info.post_node {
            let nodes = &shared
                .graph
                .post_nodes
                .as_ref()
                .expect("Post nodes not initialized");
            let node = &nodes[node_info.id as usize];

            let func = custom_func
                .unwrap_or_else(|| node.func_ptr.expect("Post node function pointer is None"));

            use crate::custom_scheduler::Priority;
            use crate::graph_struct::NodePriority;
            let priority = match node.priority {
                NodePriority::High => Priority::High,
                NodePriority::Normal => Priority::Normal,
                NodePriority::Low => Priority::Low,
            };
            let group = shared
                .scheduler
                .get_affinity_group(node.use_workers.as_ref());
            (func, priority, group)
        } else {
            let cache = &shared.node_cache[node_info.id as usize];
            let func = custom_func.unwrap_or(cache.func_ptr);
            (func, cache.priority, cache.affinity_group)
        };

        let shared_clone = Arc::clone(shared);
        let should_record = should_record_slot(shared, node_info.slot);
        let meta_data = (node_info.id, node_info.slot, node_info.index, should_record);
        let mut node_info = node_info.clone();
        // Stamp the current slot generation so execute_task can detect stale tasks.
        // Post-nodes are exempt: they run after all streams complete and have no generation risk.
        if !node_info.post_node {
            node_info.gen =
                shared.slot_generation[node_info.slot].load(Ordering::Acquire) as u32;
        }
        let pre_built_args = pre_built_args_vec[i].clone();

        // Per-task spawn timestamp for accurate scheduling latency measurement.
        let spawn_ns = shared.base_instant.elapsed().as_nanos();
        let task = move || {
            let mut current = node_info;
            let mut current_func = func_ptr;
            let mut first = true;
            loop {
                let args = if first { pre_built_args.clone() } else { None };
                first = false;
                execute_task(&shared_clone, current_func, &current, args, spawn_ns);
                match INLINE_CONTINUATION.with(|c| c.borrow_mut().take()) {
                    Some(next) => {
                        current_func = shared_clone.node_cache[next.id as usize].func_ptr;
                        current = next;
                    }
                    None => break,
                }
            }
        };

        if affinity_group > 0 {
            shared.scheduler.spawn_to_group_with_meta(
                affinity_group,
                task_priority,
                Some(meta_data),
                task,
            );
        } else {
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

        // Clear completed nodes BEFORE releasing the slot.
        // reinit_slot must finish before release_slot makes the slot available for a new
        // stream assignment.  If release_slot ran first, assign_stream_to_available_slot
        // could pick up the Inactive slot, spawn initial tasks (storing results), and then
        // reinit_slot would clear those new-stream results → panic in legitimate tasks.
        shared
            .node_results
            .reinit_slot(&shared.graph.nodes, slot, None);

        // Release the slot (makes it available for next stream assignment)
        release_slot(shared, slot);

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
        shared.active_slots_bitmap.fetch_or(1u64 << last_slot_assigned, Ordering::Release);
        running_streams.push((stream, last_slot_assigned));
        shared.slot_stream_id[last_slot_assigned].store(stream, Ordering::Relaxed);
        print_debug(|| {
            format!(
                "Assigned stream {} to slot {} (Inactive) -> Active (last assigned)",
                stream, last_slot_assigned
            )
        });
        drop(running_streams); // Release lock before returning

        // Bump slot generation for the new stream — lazily reinitialises all
        // NodeDependencyEntry, instances_sent, and cond_instances_to_spawn entries.
        // Done here (new-stream start, Inactive → Active) rather than in the slot
        // completion path so that old-stream in-flight tasks retain the old generation
        // and cannot spuriously spawn or corrupt the new stream's dependency counters.
        shared.slot_generation[last_slot_assigned].fetch_add(1, Ordering::SeqCst);

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
            shared.slot_stream_id[slot_id].store(stream, Ordering::Relaxed);
            shared.last_slot_assigned.store(slot_id, Ordering::SeqCst);
            print_debug(|| {
                format!(
                    "Assigned stream {} to slot {} (Inactive) -> Buffering",
                    stream, slot_id
                )
            });
            drop(running_streams); // Release lock before returning
            // In non-network mode, initial nodes are spawned immediately for
            // Buffering slots too (see initial_nodes call site). Without this
            // start_slot_processing call the timing controller panics when
            // finish_slot_processing is called at stream completion because it
            // never saw a StartSlotProcessing for this slot.
            // In network mode, activate_next_slot will call start_slot_processing
            // again (overwriting the start time) when the slot transitions to
            // Active — that is fine; the later timestamp is more accurate there.
            if let Some(tb) = &shared.time_buffer {
                tb.start_slot_processing(slot_id);
            }
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
    shared.active_slots_bitmap.fetch_and(!(1u64 << slot), Ordering::Release);
    shared.slot_stream_id[slot].store(usize::MAX, Ordering::Relaxed);

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

/// Spin-wait for a predecessor result that is temporarily absent because its
/// producer task is still executing on a parallel worker.
///
/// This handles the race where the threshold-based dispatcher fires a successor
/// (e.g. copy_op[0]) after *any* predecessor completes, even though the specific
/// predecessor instance that this successor reads (e.g. gen_b[0]) has not yet
/// stored its result.  With a single worker the ordering is serial and the race
/// cannot occur; with multiple workers it can.
///
/// Returns `Some(result)` once the result is visible, or `None` if the slot
/// generation changes (slot recycled → task is stale and should be dropped).
#[cold]
#[inline(never)]
fn spin_wait_for_result(
    shared: &Arc<SharedData>,
    node_info: &NodeInfo,
) -> Option<synstream_types::CmTypes> {
    let mut spin_count: u32 = 0;
    loop {
        if let Some(result) = shared.node_results.get(node_info) {
            return Some(result);
        }
        let exec_slot = EXECUTING_SLOT.with(|s| *s.borrow());
        let exec_gen = EXECUTING_GEN.with(|g| *g.borrow());
        if exec_slot != usize::MAX {
            let current_gen =
                shared.slot_generation[exec_slot].load(Ordering::Acquire) as u32;
            if exec_gen != current_gen {
                if !shared.single_slot_mode {
                    STALE_TASK_DETECTED.with(|f| *f.borrow_mut() = true);
                }
                return None;
            }
        }
        spin_count += 1;
        if spin_count < 64 {
            std::hint::spin_loop();
        } else if spin_count < 256 {
            std::thread::yield_now();
        } else {
            std::thread::park_timeout(std::time::Duration::from_nanos(100));
        }
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
        CmTypes::Dep(_) => {
            // Ordering-only dep: no result fetch needed, provide None directly.
            // The predecessor edge is tracked for scheduling purposes but the
            // result value is not consumed by this successor.
            return Some(vec![CmTypes::None]);
        }
        CmTypes::Res(res_node_id) => {
            // Short-circuit: if a previous arg already detected stale, skip remaining
            if !shared.single_slot_mode && STALE_TASK_DETECTED.with(|f| *f.borrow()) {
                return None;
            }

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
                }
                // Result temporarily absent: predecessor may still be executing on a
                // parallel worker (threshold dispatch fired before its store completed).
                // Spin-wait until the result arrives or the slot becomes stale.
                return match spin_wait_for_result(shared, &node_info) {
                    Some(result) => Some(vec![result]),
                    None => None,
                };
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
                }
                // Spin-wait: predecessor may still be in-flight on another worker.
                return match spin_wait_for_result(shared, &node_info) {
                    Some(result) => Some(vec![result]),
                    None => None,
                };
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
                    // Spin-wait: predecessor may still be in-flight on another worker.
                    match spin_wait_for_result(shared, &node_info) {
                        Some(result) => result_vec.push(result),
                        None => return None, // Stale
                    }
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
) -> Option<(usize, Vec<(NodeInfo, Option<CmTypes>)>)> {
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
                shared.active_slots_bitmap.fetch_or(1u64 << *slot, Ordering::Release);
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

        // Bump slot generation for the new stream — lazily reinitialises all
        // NodeDependencyEntry, instances_sent, and cond_instances_to_spawn entries.
        // Done here (new-stream start, Buffering → Active) so that old-stream tasks
        // still in the batch_queue use the old generation and cannot corrupt the
        // new stream's dependency counters or cause spurious task spawning.
        shared.slot_generation[slot_id].fetch_add(1, Ordering::SeqCst);

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
            shared.slot_stream_id[slot].load(Ordering::Relaxed) == target_stream
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
