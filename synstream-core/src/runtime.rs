use core_affinity;
use crossbeam_channel::Receiver;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{sleep, spawn};
use std::time::{Duration, Instant};

use crate::debug::print_debug;
use crate::graph::*;
use crate::graph_struct::*;
use crate::resolution_state::{MultiThreadedState, ResolutionState, SingleThreadedState};
use crate::runtime_funcs::*;
use crate::scheduler::{Scheduler, SchedulerImpl};
use crate::time_buffer::{TimeBufferManager, TimingMethod};
use crate::{buffers::*, IdType, Record};
use synstream_types::*;

pub const RUN_SLEEP: Duration = Duration::from_secs(10);

// Main SynStream Runtime struct with shared context
pub struct SynRt {
    shared: Arc<SharedData>,
}

impl SynRt {
    pub fn new(
        app_graph: &Graph,
        slots: usize,
        max_streams: usize,
        max_runtime: Option<u64>,
        use_rdtsc: bool,
        record: bool,
        timing_enabled: bool,
        system_threads: usize,
        _nrx: usize,
        slot_priority_enabled: bool,
    ) -> SynRt {
        // Initialize stream completion counters
        let stream_completion_counts = Arc::new(RwLock::new(Vec::new()));
        let mut completion_counts = stream_completion_counts.write();
        completion_counts.clear();

        let slots = std::cmp::min(slots, max_streams);
        for _ in 0..slots {
            completion_counts.push(AtomicUsize::new(0));
        }
        drop(completion_counts);

        let available_stream_slots = Arc::new(RwLock::new(Vec::new()));
        let mut available_write = available_stream_slots.write();
        for _ in 0..slots {
            available_write.push(std::usize::MAX); // real stream id
        }
        drop(available_write);

        // Build node cache for fast repeated access
        let node_cache: Vec<NodeCacheEntry> = app_graph
            .nodes
            .iter()
            .map(|node| {
                node_cache_entry(
                    node,
                    app_graph.init_objects.as_ref().unwrap(),
                    &app_graph.initial_nodes,
                    &app_graph.condition_nodes,
                )
            })
            .collect();

        // Allocate slots + system_threads for TimeBuffer (slots for worker streams, system_threads for system threads)
        let time_buffer = if timing_enabled {
            Some(Arc::new(TimeBufferManager::new_async(
                slots + system_threads,
                system_threads,
                use_rdtsc,
            )))
        } else {
            None
        };

        let recorder = if record {
            Some(Arc::new(Mutex::new(HashMap::new())))
        } else {
            None
        };
        let base_instant = Arc::new(Instant::now());
        let job_counter = Arc::new(AtomicUsize::new(0));
        // core_offset is updated in run()
        let core_offset = Arc::new(AtomicUsize::new(0));

        // Initialize batch buffer for scheduler-side batching
        let batch_buffer = Arc::new(Mutex::new(Vec::new()));
        let batch_last_sent = Arc::new(Mutex::new(Instant::now()));
        let flusher_shutdown = Arc::new(AtomicUsize::new(0));

        // Initialize shared dependency tracking structures
        let dependency_count_vec: Vec<usize> = app_graph.dependency_count_vec();

        // Compute max_factor for flat index computation
        let max_factor = node_cache.iter().map(|n| n.factor).max().unwrap_or(1);
        let num_nodes = app_graph.nodes.len();

        // Choose resolution state implementation based on system_threads
        let resolution_state: Arc<dyn ResolutionState> = if system_threads == 1 {
            println!("Using single-threaded resolution state (no locks)");
            Arc::new(SingleThreadedState::new(
                num_nodes,
                slots,
                max_factor,
                dependency_count_vec.clone(),
                &app_graph.nodes,
            ))
        } else {
            println!("Using multi-threaded resolution state (lock-free atomics)");
            Arc::new(MultiThreadedState::new(
                num_nodes,
                slots,
                max_factor,
                dependency_count_vec.clone(),
                &app_graph.nodes,
            ))
        };

        // Initialize remaining nodes trackers with AtomicUsize for thread-safe access
        let mut remaining_nodes = Vec::new();
        let mut remaining_cond_nodes = Vec::new();
        let mut remaining_init = Vec::new(); // Store initial values for reinit
        let mut node_id_to_rem = vec![0; app_graph.nodes.len()];
        let mut node_id_is_cond = vec![false; app_graph.nodes.len()]; // Track which nodes are condition nodes

        for _slot in 0..slots {
            let mut slot_remaining = Vec::new();
            let mut slot_cond_remaining = Vec::new();
            let mut slot_remaining_init = Vec::new();

            for node_id in 0..app_graph.nodes.len() {
                if app_graph.initial_nodes.contains(&(node_id as IdType)) {
                    slot_remaining.push(AtomicUsize::new(0));
                    slot_remaining_init.push(0);
                    node_id_to_rem[node_id] = slot_remaining.len() - 1;
                    node_id_is_cond[node_id] = false;
                } else if !app_graph.condition_nodes.contains(&(node_id as IdType)) {
                    let factor = node_cache[node_id].factor;
                    slot_remaining.push(AtomicUsize::new(factor));
                    slot_remaining_init.push(factor);
                    node_id_to_rem[node_id] = slot_remaining.len() - 1;
                    node_id_is_cond[node_id] = false;
                } else {
                    slot_cond_remaining.push(AtomicUsize::new(node_cache[node_id].factor));
                    node_id_to_rem[node_id] = slot_cond_remaining.len() - 1;
                    node_id_is_cond[node_id] = true;
                }
            }
            remaining_nodes.push(slot_remaining);
            remaining_cond_nodes.push(slot_cond_remaining);
            remaining_init.push(slot_remaining_init);
        }

        // Initialize slot states for priority processing
        let slot_states = Arc::new(RwLock::new(Vec::new()));
        {
            let mut states = slot_states.write();
            for slot_id in 0..slots {
                if slot_priority_enabled && slot_id == 0 {
                    // Only slot 0 is active initially
                    states.push(SlotState::Active);
                } else if slot_priority_enabled {
                    // All other slots start buffering
                    states.push(SlotState::Buffering);
                } else {
                    // When disabled, all slots are active
                    states.push(SlotState::Active);
                }
            }
        }

        // Initialize per-slot buffering queues
        let slot_buffers = Arc::new(RwLock::new(vec![Vec::new(); slots]));

        let shared = Arc::new(SharedData {
            graph: app_graph.clone(),
            slots,
            max_streams,
            max_runtime,
            node_cache,
            node_results: Arc::new(RwLock::new(VecMap::new(CmTypes::Init))),
            stream_complete_counter: Arc::new(AtomicUsize::new(0)),
            available_stream_slots,
            time_buffer,
            scheduler: Arc::new(RwLock::new(None)),
            network_scheduler: Arc::new(RwLock::new(None)),
            completed_tx: Arc::new(RwLock::new(None)),
            workers: Arc::new(AtomicUsize::new(1)), // Will be set in run()
            recorder,
            base_instant,
            job_counter,
            core_offset,
            resolution_state,
            remaining_nodes: Arc::new(remaining_nodes),
            remaining_cond_nodes: Arc::new(remaining_cond_nodes),
            node_id_to_rem: Arc::new(node_id_to_rem),
            node_id_is_cond: Arc::new(node_id_is_cond),
            remaining_init: Arc::new(remaining_init),
            initial_prep_done: Arc::new(AtomicUsize::new(0)),
            system_threads,
            batch_buffer,
            batch_last_sent,
            flusher_shutdown,
            slot_states,
            slot_priority_enabled,
            slot_buffers,
        });

        SynRt { shared }
    }

    pub fn base_instant(&self) -> Instant {
        *self.shared.base_instant
    }

    pub fn run(&mut self, scheduler: SchedulerImpl, system_threads: usize) {
        // Overwrite workers
        self.shared
            .workers
            .store(scheduler.workers(), Ordering::SeqCst);

        // create completed channel for batched results
        let (completed_tx, completed_rx) =
            crossbeam_channel::unbounded::<Vec<(NodeInfo, CmTypes)>>();

        // Set completed_tx in SharedData
        {
            let mut tx_lock = self.shared.completed_tx.write();
            *tx_lock = Some(completed_tx.clone());
        }

        // Set completed_tx in the scheduler and start the flusher thread
        scheduler.set_completed_tx(completed_tx);

        // Store scheduler
        let core_offset: usize;
        {
            let mut scheduler_lock = self.shared.scheduler.write();
            core_offset = scheduler.core_offset().unwrap_or(0);
            *scheduler_lock = Some(Arc::new(scheduler));
        }

        self.shared.core_offset.store(core_offset, Ordering::SeqCst);

        // Initialize node_results
        self.init_results(self.shared.slots);

        // Initiate synstream-runtime timing for system thread slots only
        for thread_id in 0..system_threads {
            let system_slot = self.shared.slots + thread_id;
            if let Some(tb) = &self.shared.time_buffer {
                tb.start_slot_processing(system_slot);
            }
        }

        // Spawn multiple resolution threads
        let mut resolution_handles = Vec::new();
        for thread_id in 0..system_threads {
            let shared_for_resolution = Arc::clone(&self.shared);
            let completed_rx_clone = completed_rx.clone();
            let thread_core = core_offset + thread_id;
            // Each system thread gets its own slot: slots + thread_id
            let thread_slot = self.shared.slots + thread_id;

            let resolution_handle = spawn(move || {
                if core_affinity::set_for_current(core_affinity::CoreId { id: thread_core }) {
                    println!(
                        "Resolution thread {} pinned to core {:?} with slot {}",
                        thread_id, thread_core, thread_slot
                    );
                } else {
                    println!(
                        "Failed to pin resolution thread {} to core {:?}",
                        thread_id, thread_core
                    );
                }

                Self::resolution(
                    shared_for_resolution,
                    completed_rx_clone,
                    thread_core,
                    thread_id,
                    thread_slot,
                );
            });
            resolution_handles.push(resolution_handle);
        }
        print_debug(|| format!("{} Resolution threads spawned", system_threads));

        let start_time = Instant::now();
        // Check for max_runtime
        print_debug(|| "Max runtime check started".to_string());
        if let Some(max_runtime) = self.shared.max_runtime {
            let scheduler_guard = self.shared.scheduler.read();

            let scheduler = match scheduler_guard.as_ref() {
                Some(s) => s,
                None => {
                    eprintln!("Scheduler is not initialized");
                    return;
                }
            };
            let mut prev_completed_jobs = scheduler.total_jobs_completed();
            let mut prev_spawned_jobs = scheduler.total_jobs_spawned();
            drop(scheduler_guard);
            sleep(RUN_SLEEP);
            loop {
                let scheduler_guard = self.shared.scheduler.read();

                let scheduler = match scheduler_guard.as_ref() {
                    Some(s) => s,
                    None => {
                        eprintln!("Scheduler is not initialized");
                        return;
                    }
                };

                let curr_completed_jobs = scheduler.total_jobs_completed();
                let curr_spawned_jobs = scheduler.total_jobs_spawned();
                let pending_jobs = scheduler.pending_jobs();

                let completed = {
                    if pending_jobs == 0
                        && curr_completed_jobs > 0
                        && curr_completed_jobs == prev_completed_jobs
                        && curr_spawned_jobs == prev_spawned_jobs
                    {
                        true
                    } else {
                        false
                    }
                };

                prev_completed_jobs = curr_completed_jobs;
                prev_spawned_jobs = curr_spawned_jobs;

                drop(scheduler_guard);

                if start_time.elapsed().as_secs() > max_runtime || completed {
                    // set exit signal
                    println!("Max runtime reached or graph completed, exiting...");
                    // Process post-nodes if any
                    println!("Processing possible post-nodes...");
                    self.schedule_post_nodes();
                    // Signal flusher thread to shut down (will be done after loop)
                    break;
                }
                sleep(RUN_SLEEP);
                //
            }
        }

        // Shutdown and flush the flusher thread in the scheduler
        {
            let scheduler_guard = self.shared.scheduler.read();
            if let Some(scheduler) = scheduler_guard.as_ref() {
                scheduler.shutdown_flusher();
            }
        }

        // Drop all senders to signal resolution threads to exit
        // This will close the channel and unblock the resolution threads
        {
            let mut tx_lock = self.shared.completed_tx.write();
            *tx_lock = None; // Drop the sender in SharedData
        }
        {
            let scheduler_guard = self.shared.scheduler.read();
            if let Some(scheduler) = scheduler_guard.as_ref() {
                let tx_ref = scheduler.get_completed_tx_ref();
                let mut tx_lock = tx_ref.lock();
                *tx_lock = None; // Drop the sender in scheduler
            }
        }

        // Wait for all resolution threads to finish (they will unblock when channel closes)
        for handle in resolution_handles {
            handle.join().unwrap();
        }

        // Finish timing for system thread slots only
        for thread_id in 0..system_threads {
            let system_slot = self.shared.slots + thread_id;
            if let Some(tb) = &self.shared.time_buffer {
                let _ = tb.finish_slot_processing(system_slot);
            }
        }
    }
}

// Execution Threads
impl SynRt {
    fn preparation(
        shared: &Arc<SharedData>,
        nodes_to_schedule: &Vec<NodeInfo>,
        thread_core: usize,
        thread_slot: usize,
        use_network_scheduler: bool,
    ) {
        let start_time = if let Some(tb) = &shared.time_buffer {
            tb.measure_time()
        } else {
            TimingMethod::Instant(Instant::now())
        };
        let start_ns = shared.base_instant.elapsed().as_nanos();
        print_debug(|| format!("Preparing {:?} nodes", nodes_to_schedule.len()));

        // Schedule Task - args will be built in the worker thread
        let pre_built_args_vec = vec![None; nodes_to_schedule.len()];
        let custom_func_vec = vec![None; nodes_to_schedule.len()];
        send_to_scheduler(
            &shared,
            nodes_to_schedule,
            &pre_built_args_vec,
            &custom_func_vec,
            use_network_scheduler,
        );

        if let Some(tb) = &shared.time_buffer {
            let end_time = tb.measure_time();
            let duration = tb.measure_duration(start_time, end_time);
            tb.add_task_time(thread_slot, "Preparation Thread", usize::MAX, duration);
        }

        if let Some(rec) = &shared.recorder {
            let end_ns = shared.base_instant.elapsed().as_nanos();
            let job_id = shared.job_counter.fetch_add(1, Ordering::SeqCst);
            let mut map = rec.lock();
            let vec = map.entry(thread_slot).or_insert_with(Vec::new);
            vec.push(Record {
                job_id,
                start_ns,
                end_ns,
                worker: thread_core,
                task_id: IdType::MAX - 1,
                index: 0,
            });
        }
    }

    fn resolution(
        shared: Arc<SharedData>,
        completed_rx: Receiver<Vec<(NodeInfo, CmTypes)>>,
        thread_core: usize,
        thread_id: usize,
        thread_slot: usize,
    ) {
        let all_slots: Vec<usize> = (0..shared.slots).collect();
        let network_init_nodes = initial_nodes(&shared, all_slots.clone());
        if thread_id == 0 {
            // Ensure only one thread does initial preparation
            if shared
                .initial_prep_done
                .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                print_debug(|| {
                    format!(
                        "Thread {} in Core {} performing initial preparation",
                        thread_id, thread_core
                    )
                });
                println!(
                    "Thread {} in Core {} performing initial preparation",
                    thread_id, thread_core
                );

                // Network nodes (nx=true) run for ALL slots to receive and buffer packets
                let mut network_nodes = Vec::new();
                for node_info in &network_init_nodes {
                    let node = &shared.graph.nodes[node_info.id as usize];
                    if node.nx {
                        network_nodes.push(node_info.clone());
                    }
                }

                // Send network nodes to network scheduler (all slots)
                if !network_nodes.is_empty() {
                    if shared.slot_priority_enabled {
                        print_debug(|| {
                            format!(
                            "Slot-Priority: Starting {:?} network nodes for all slots to buffer packets: {:?}",
                            network_nodes.len(), all_slots
                        )
                        });
                    }
                    Self::preparation(&shared, &network_nodes, thread_core, thread_slot, true);
                }

                // Compute nodes (nx=false) follow slot-priority - only active slots
                let active_slots: Vec<usize> = (0..shared.slots)
                    .filter(|&slot| is_slot_active(&shared, slot))
                    .collect();

                if shared.slot_priority_enabled {
                    print_debug(|| {
                        format!(
                            "Slot-Priority: Starting compute nodes for active slots: {:?}",
                            active_slots
                        )
                    });
                }

                let compute_init_nodes = initial_nodes(&shared, active_slots);
                let mut regular_nodes = Vec::new();
                for node_info in compute_init_nodes {
                    let node = &shared.graph.nodes[node_info.id as usize];
                    if !node.nx {
                        regular_nodes.push(node_info);
                    }
                }

                // Send compute nodes to regular scheduler (only active slots)
                if !regular_nodes.is_empty() {
                    Self::preparation(&shared, &regular_nodes, thread_core, thread_slot, false);
                }
            }
        }

        // denote slots for which network nodes have been initially spawned
        let mut network_init_slots = Vec::new();
        for node_info in &network_init_nodes {
            let node = &shared.graph.nodes[node_info.id as usize];
            if node.nx {
                network_init_slots.push(node_info.slot);
            }
        }

        // prefetch cond indexes for efficiency
        let cond_indexes = shared.graph.get_condition_indexes();

        // Persistent completion tracking across all batches for this stream
        let mut stream_slot_activity: HashMap<usize, bool> = HashMap::new();

        // Process completed nodes with dynamic batching from scheduler
        loop {
            // Receive batch from scheduler (blocking)
            let batch = match completed_rx.recv() {
                Ok(batch_from_scheduler) => batch_from_scheduler,
                Err(_) => return, // Channel closed
            };

            print_debug(|| {
                format!(
                    "Thread {:?} -- Processing batch of {} nodes",
                    thread_id,
                    batch.len()
                )
            });

            // Local tracking for THIS batch only
            let mut nodes_sent_in_slot: HashMap<usize, usize> = HashMap::new();

            let start_ns = shared.base_instant.elapsed().as_nanos();
            let start_time = if let Some(tb) = &shared.time_buffer {
                tb.measure_time()
            } else {
                TimingMethod::Instant(Instant::now())
            };

            // Process the entire batch
            for (mut node_info, result) in batch {
                print_debug(|| {
                    format!(
                        "Thread {:?} -- Processing Completed {:?}",
                        thread_id, node_info
                    )
                });

                if node_info.post_node {
                    // Store Result
                    shared.node_results.write().set(&node_info, result);
                    continue;
                }

                // Get Id function and validate slot
                let new_stream_opt = process_id_function(&shared, &node_info, &result);
                if let Some(new_stream) = new_stream_opt {
                    // Assign streams to an available stream slot
                    node_info.slot = assign_stream_to_available_slot(&shared, new_stream);
                } else {
                    // ID function failed, skip processing this node
                    print_debug(|| {
                        format!(
                            "Thread {:?} -- Skipping further processing of node {:?} due to ID function failure",
                            thread_id, node_info
                        )
                    });
                    continue;
                }

                // store result - single lock acquisition
                shared.node_results.write().set(&node_info, result);

                // Mark this slot as having activity (for persistent completion tracking)
                stream_slot_activity.insert(node_info.slot, true);

                // Decrement remaining_nodes counter now that this task is confirmed completed
                // Using pre-computed node_id_is_cond flag for lock-free branch
                let node_id_usize = node_info.id as usize;
                let node_id_to_rem_idx = shared.node_id_to_rem[node_id_usize];
                let node_cache_entry = &shared.node_cache[node_id_usize];

                // Lock-free access using pre-computed is_condition flag
                if node_cache_entry.is_condition {
                    shared.remaining_cond_nodes[node_info.slot][node_id_to_rem_idx]
                        .fetch_sub(1, Ordering::Release);
                } else if !node_cache_entry.is_initial {
                    shared.remaining_nodes[node_info.slot][node_id_to_rem_idx]
                        .fetch_sub(1, Ordering::Release);
                }

                // Get successors
                let successors: &Vec<IdType> = {
                    if node_id_usize >= shared.graph.successors.len() {
                        &Vec::new()
                    } else {
                        &shared.graph.successors[node_id_usize]
                    }
                };

                print_debug(|| {
                    format!(
                        "Thread {:?} -- Successors of node {:?}: {:?}",
                        thread_id, node_info, successors
                    )
                });

                let mut nodes_to_schedule: Vec<NodeInfo> = Vec::new();

                // Collect all potential successors with their info
                let mut succ_updates: Vec<(NodeInfo, bool, IdType)> = Vec::new();

                for succ_id in successors {
                    let succ_id = *succ_id;
                    let succ_cache = &shared.node_cache[succ_id as usize];

                    // Use pre-computed flag for lock-free check
                    let has_condition = succ_cache.is_condition;

                    // Lock-free remaining count access
                    let remaining = {
                        let succ_id_to_rem_idx = shared.node_id_to_rem[succ_id as usize];
                        if has_condition {
                            shared.remaining_cond_nodes[node_info.slot][succ_id_to_rem_idx]
                                .load(Ordering::Acquire)
                        } else {
                            shared.remaining_nodes[node_info.slot][succ_id_to_rem_idx]
                                .load(Ordering::Acquire)
                        }
                    };

                    if remaining == 0 {
                        continue;
                    }

                    let succ_factor = succ_cache.factor;
                    let node_factor = node_cache_entry.factor;

                    let pred_count = succ_cache
                        .pred_vec
                        .get(node_info.id as usize)
                        .cloned()
                        .unwrap_or(0);

                    let succ_indexes = {
                        if succ_factor == node_factor && pred_count <= 1 {
                            vec![node_info.index]
                        } else if !has_condition {
                            let num_indexes = std::cmp::max(succ_factor, remaining);
                            (0..num_indexes).collect::<Vec<_>>()
                        } else {
                            vec![node_info.index % succ_factor]
                        }
                    };

                    print_debug(|| {
                        format!(
                            "Thread {:?} -- Processing successor id {} - {:?} of node {:?}",
                            thread_id, succ_id, succ_indexes, node_info
                        )
                    });

                    for succ_index in succ_indexes {
                        let succ_info =
                            NodeInfo::new(succ_id, node_info.slot, succ_index, node_info.index);
                        succ_updates.push((succ_info, has_condition, succ_id));
                    }
                }

                // Batch process dependency decrements using resolution state
                // Collect condition nodes to check OUTSIDE the lock to avoid nested locking
                let mut cond_nodes_to_check: Vec<(NodeInfo, usize)> = Vec::new();

                // if not exist, init nodes_sent for slot to 0
                let nodes_sent: &mut usize = nodes_sent_in_slot.entry(node_info.slot).or_insert(0);

                for (succ_info, has_cond, succ_id) in succ_updates {
                    if let Some(dep) = shared.resolution_state.decrease_dependency(&succ_info) {
                        if dep == 0 {
                            // Try to atomically claim the slot via resolution state
                            if shared.resolution_state.try_mark_sent(
                                node_info.slot,
                                succ_id as usize,
                                succ_info.index,
                            ) {
                                if !has_cond {
                                    nodes_to_schedule.push(succ_info);
                                    *nodes_sent += 1;
                                } else {
                                    // Collect condition nodes - will evaluate outside lock
                                    let cond_idx = shared.node_cache[succ_id as usize].cond_index;
                                    cond_nodes_to_check.push((succ_info, cond_idx));
                                }
                            }
                        }
                    }
                }

                // Evaluate conditions OUTSIDE the locks - conditions_met takes node_results.read()
                for (succ_info, cond_idx) in cond_nodes_to_check {
                    if conditions_met(&shared, &succ_info, &cond_indexes[cond_idx]) {
                        nodes_to_schedule.push(succ_info.clone());
                        *nodes_sent += 1;
                    } else {
                        // Reset the flag via resolution state
                        shared.resolution_state.reset_sent(
                            node_info.slot,
                            succ_info.id as usize,
                            succ_info.index,
                        );
                    }
                }

                // Separate nodes by slot state: active slots → scheduler, buffering slots → buffer
                let mut active_nodes = Vec::new();
                let mut buffered_by_slot: Vec<Vec<NodeInfo>> = vec![Vec::new(); shared.slots];

                for node_info in nodes_to_schedule {
                    if is_slot_active(&shared, node_info.slot) {
                        active_nodes.push(node_info);
                    } else {
                        buffered_by_slot[node_info.slot].push(node_info);
                    }
                }

                if !active_nodes.is_empty() {
                    // Increment nodes_sent for each active slot
                    for node_info in &active_nodes {
                        *nodes_sent_in_slot.entry(node_info.slot).or_insert(0) += 1;
                    }
                    Self::preparation(&shared, &active_nodes, thread_core, thread_slot, false);
                }

                // Buffer nodes from inactive slots
                if !buffered_by_slot.is_empty() {
                    let mut slot_buffers = shared.slot_buffers.write();
                    for (slot, nodes) in buffered_by_slot.iter().enumerate() {
                        if nodes.is_empty() {
                            continue;
                        }
                        slot_buffers[slot].extend(nodes.clone());
                        // Mark that this slot had activity (for completion check) - both persistent and per-batch
                        nodes_sent_in_slot.entry(slot).or_insert(0);
                        stream_slot_activity.insert(slot, true);

                        print_debug(|| {
                            format!(
                                "Thread {:?} -- Buffered {:?} nodes for slot {:?} ",
                                thread_id,
                                nodes.len(),
                                slot
                            )
                        });
                    }
                }
            } // End of batch processing loop

            // Check for stream completion - iterate over ALL slots with activity, not just current batch
            // Collect slots first to avoid borrow issues when mutating the map
            let slots_to_check: Vec<usize> = stream_slot_activity.keys().copied().collect();

            print_debug(|| {
                format!(
                    "Stream_slot_activity has {} slots to check: {:?}",
                    slots_to_check.len(),
                    slots_to_check
                )
            });

            for proc_slot in slots_to_check {
                print_debug(|| {
                    format!(
                        "Checking slot {} for completion (completed={}, active={})",
                        proc_slot,
                        shared.resolution_state.is_slot_completed(proc_slot),
                        if shared.slot_priority_enabled {
                            is_slot_active(&shared, proc_slot).to_string()
                        } else {
                            "N/A".to_string()
                        }
                    )
                });

                // Skip slots already marked as completed
                if shared.resolution_state.is_slot_completed(proc_slot) {
                    continue;
                }

                // Skip buffering slots - they cannot complete until activated
                if shared.slot_priority_enabled && !is_slot_active(&shared, proc_slot) {
                    continue;
                }

                // Check if all nodes in this slot have been processed (lock-free)
                // This works across batch boundaries now because stream_slot_activity is persistent
                let all_nodes_processed = shared.remaining_nodes[proc_slot]
                    .iter()
                    .all(|count| count.load(Ordering::Acquire) == 0);

                print_debug(|| {
                    let counts: Vec<usize> = shared.remaining_nodes[proc_slot]
                        .iter()
                        .map(|c| c.load(Ordering::Acquire))
                        .collect();
                    format!(
                        "Slot {} remaining_nodes: {:?}, all_processed={}",
                        proc_slot, counts, all_nodes_processed
                    )
                });

                if all_nodes_processed {
                    print_debug(|| {
                        format!(
                            "Thread {:?} -- Completed iteration at slot {}",
                            thread_id, proc_slot
                        )
                    });

                    // Mark this slot as completed
                    shared.resolution_state.mark_slot_completed(proc_slot);

                    // CRITICAL: Reset ALL state BEFORE checking process_slot_completion
                    // This prevents race conditions where new nodes complete before state is clean
                    print_debug(|| {
                        format!(
                            "Resetting all state for slot {} before starting new iteration",
                            proc_slot
                        )
                    });

                    // Reset dependency_map for this slot via resolution state
                    shared
                        .resolution_state
                        .reinit_dependencies(&shared.graph.nodes, proc_slot);

                    // Reinit remaining_nodes for this slot using pre-computed init values (lock-free)
                    let slot_remaining = &shared.remaining_nodes[proc_slot];
                    let slot_init = &shared.remaining_init[proc_slot];
                    for (node_rem_idx, init_val) in slot_init.iter().enumerate() {
                        slot_remaining[node_rem_idx].store(*init_val, Ordering::Release);
                    }

                    // Reinit remaining_cond_nodes for this slot (reset to factor values)
                    let slot_cond_remaining = &shared.remaining_cond_nodes[proc_slot];
                    for node_id in 0..shared.graph.nodes.len() {
                        if shared.node_id_is_cond[node_id] {
                            let node_id_to_rem_idx = shared.node_id_to_rem[node_id];
                            let factor = shared.node_cache[node_id].factor;
                            slot_cond_remaining[node_id_to_rem_idx]
                                .store(factor, Ordering::Release);
                        }
                    }

                    // Clear nodes_sent_to_queue for this slot - MUST happen before new nodes spawn
                    shared.resolution_state.clear_slot_sent_flags(proc_slot);
                    print_debug(|| {
                        format!(
                            "Cleared all state for slot {} before spawning new stream",
                            proc_slot
                        )
                    });

                    // In slot-priority mode: rotate active slot and activate next buffered slot
                    let buffered_nodes = if shared.slot_priority_enabled {
                        transition_slot_to_buffering(&shared, proc_slot);
                        print_debug(|| format!("Calling activate_next_slot()"));
                        activate_next_slot(&shared, Some(proc_slot))
                    } else {
                        None
                    };

                    // Flush buffered nodes from newly activated slot (if any)
                    if let Some(nodes) = buffered_nodes {
                        if !nodes.is_empty() {
                            print_debug(|| {
                                format!(
                                    "Slot-Priority: Flushing {} buffered nodes from activated slot",
                                    nodes.len()
                                )
                            });
                            Self::preparation(&shared, &nodes, thread_core, thread_slot, false);
                        }
                    }

                    // Check if we should start a new iteration
                    if process_slot_completion(&shared, proc_slot) {
                        print_debug(|| {
                            format!(
                                "Starting new iteration for slot {} - spawning initial nodes",
                                proc_slot
                            )
                        });

                        // Remove from completed set since we're starting again
                        shared.resolution_state.unmark_slot_completed(proc_slot);

                        // Reset network node tracking for new stream iteration
                        network_init_slots.clear();
                        print_debug(|| "Resetting network_init_slots for new stream".to_string());

                        // Clear activity tracking for this slot's new stream iteration
                        stream_slot_activity.remove(&proc_slot);
                        print_debug(|| {
                            format!(
                                "Cleared stream_slot_activity for slot {} to start new stream iteration",
                                proc_slot
                            )
                        });

                        // Spawn initial nodes for the restarting slot
                        let init_nodes = initial_nodes(&shared, vec![proc_slot]);

                        // Separate nodes by nx flag
                        let mut network_nodes = Vec::new();
                        let mut regular_nodes = Vec::new();

                        for node_info in init_nodes {
                            let node = &shared.graph.nodes[node_info.id as usize];
                            if node.nx {
                                if !network_init_slots.contains(&node_info.slot) {
                                    network_nodes.push(node_info);
                                } else {
                                    // remove from network_init_slots to avoid re-sending
                                    network_init_slots.retain(|&s| s != node_info.slot);
                                }
                            } else {
                                regular_nodes.push(node_info);
                            }
                        }

                        // Send network nodes to network scheduler
                        if !network_nodes.is_empty() {
                            Self::preparation(
                                &shared,
                                &network_nodes,
                                thread_core,
                                thread_slot,
                                true,
                            );
                        }

                        // Send regular nodes to scheduler only if slot is active; otherwise buffer
                        if !regular_nodes.is_empty() {
                            if shared.slot_priority_enabled && !is_slot_active(&shared, proc_slot) {
                                let mut slot_buffers = shared.slot_buffers.write();
                                slot_buffers[proc_slot].extend(regular_nodes);
                                print_debug(|| {
                                    format!(
                                        "Slot-Priority: Buffered {} init nodes for slot {}",
                                        slot_buffers[proc_slot].len(),
                                        proc_slot
                                    )
                                });
                            } else {
                                Self::preparation(
                                    &shared,
                                    &regular_nodes,
                                    thread_core,
                                    thread_slot,
                                    false,
                                );
                            }
                        }
                    }
                }
            }

            if let Some(tb) = &shared.time_buffer {
                let end_time = if let Some(tb) = &shared.time_buffer {
                    tb.measure_time()
                } else {
                    TimingMethod::Instant(Instant::now())
                };
                let duration = tb.measure_duration(start_time, end_time);
                tb.add_task_time(
                    thread_slot,
                    &format!("Resolution Thread {}", thread_id),
                    usize::MAX,
                    duration,
                );
            }

            if let Some(rec) = &shared.recorder {
                let job_id = shared.job_counter.fetch_add(1, Ordering::SeqCst);
                let end_ns = shared.base_instant.elapsed().as_nanos();
                let mut map = rec.lock();
                let vec = map.entry(thread_slot).or_insert_with(Vec::new);
                vec.push(Record {
                    job_id,
                    start_ns,
                    end_ns,
                    worker: thread_core,
                    task_id: IdType::MAX,
                    // arbitrary index value
                    index: 0,
                });
            }
        } // End of resolution processing loop
    }
}

// Helper Functions
impl SynRt {
    fn schedule_post_nodes(&mut self) {
        let nodes = &self.shared.graph.post_nodes;
        if let Some(post_nodes) = nodes {
            let stream_use = self.shared.slots + self.shared.system_threads; // Use last available slot for post-nodes
            for post_node in post_nodes {
                let mut post_schedule: Vec<NodeInfo> = Vec::new();
                let mut pre_build_args: Vec<Option<Vec<CmTypes>>> = Vec::new();
                let mut functions: Vec<Option<CmPtr>> = Vec::new();
                for index in 0..post_node.factor {
                    let mut node_info = NodeInfo::new(post_node.id, stream_use, index, 0);
                    node_info.set_post_node(true);

                    let arg_vec =
                        parse_args(&self.shared, &post_node.args, index, stream_use, 0, None);

                    let func: Option<CmPtr> = post_node.func_ptr;
                    pre_build_args.push(Some(arg_vec));
                    functions.push(func);
                    post_schedule.push(node_info);
                }
                send_to_scheduler(
                    &self.shared,
                    &post_schedule,
                    &pre_build_args,
                    &functions,
                    false,
                );
                print_debug(|| format!("Added post node: {}", post_node.name));
                // Wait until all are completed by checking node_results
                let mut completed_count = 0;
                while completed_count < post_node.factor {
                    sleep(Duration::from_millis(10));
                    completed_count = 0;
                    let results_read = self.shared.node_results.read();
                    for i in 0..post_node.factor {
                        let node_info = NodeInfo::new(post_node.id, stream_use, i, 0);
                        if results_read.result_exists(&node_info) {
                            completed_count += 1;
                        }
                    }
                    drop(results_read);
                }
            }
            print_debug(|| "All post-nodes completed".to_string());
        }
    }

    fn init_results(&mut self, slots: usize) {
        // Initialize node_results with factor entries
        let nodes = &self.shared.graph.nodes;
        let mut node_results_lock = self.shared.node_results.write();
        node_results_lock.init_map(&nodes, slots, None);

        // Initialize post_nodes if any
        let post_nodes_opt = &self.shared.graph.post_nodes;
        if let Some(post_nodes) = post_nodes_opt {
            node_results_lock.extend_map(&post_nodes);
        }
    }

    pub fn print_statistics(&self, bench_name: &str, out_file: Option<&str>) {
        if let Some(tb) = &self.shared.time_buffer {
            tb.print_stats(bench_name, out_file);
        }
    }

    pub fn write_record(&self, path: &str) {
        // Get scheduler with proper error handling
        let scheduler_guard = self.shared.scheduler.read();

        let scheduler = match scheduler_guard.as_ref() {
            Some(s) => s,
            None => {
                eprintln!("Scheduler is not initialized");
                return;
            }
        };

        scheduler.write_record(path);
        self.write_runtime_record(path);
    }

    pub fn write_runtime_record(&self, path: &str) {
        if let Some(rec) = &self.shared.recorder {
            let map = rec.lock();
            if map.is_empty() {
                println!("Runtime: no recorded events to write");
                return;
            }
            match std::fs::OpenOptions::new().append(true).open(path) {
                Ok(mut f) => {
                    for (slot, vec) in map.iter() {
                        for r in vec.iter() {
                            let _ = writeln!(
                                f,
                                "{},{},{},{},{},{},{}",
                                slot, r.job_id, r.start_ns, r.end_ns, r.worker, r.task_id, r.index
                            );
                        }
                    }
                    println!("Runtime: appended {} slots to {}", map.len(), path);
                }
                Err(e) => {
                    eprintln!("Runtime: failed to open {} for append: {}", path, e);
                }
            }
        } else {
            println!("Runtime: recorder not enabled");
        }
    }
}
