use super::network_init::process_id_function;
use super::reporting::should_record_slot;
use super::shared_data::SharedData;
use super::slot_management::{assign_stream_to_available_slot, initial_nodes};
use super::successor::{
    collect_successors_for_node_into, conditions_met, evaluate_node_condition,
};
use crate::async_recorder::{set_worker_recorder, submit_record};
use crate::buffers::*;
use crate::debug::print_debug;
use crate::network::PacketMessage;
use crate::Record;
use crate::IdType;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use synstream_types::*;

thread_local! {
    // Successor descriptors collected for a single node being processed.
    static SUCC_UPDATES_BUF: RefCell<Vec<(NodeInfo, bool, IdType, Option<usize>)>> =
        RefCell::new(Vec::with_capacity(32));
    // Nodes queued for scheduling from a single predecessor's successor set.
    static SCHEDULE_BUF: RefCell<Vec<NodeInfo>> = RefCell::new(Vec::with_capacity(32));
    // Ready instance indices returned by decrease_and_get_ready_into.
    static READY_BUF: RefCell<Vec<usize>> = RefCell::new(Vec::with_capacity(16));
    // Accumulates scheduled successor nodes during batch processing.
    // Flushed incrementally via preparation() every sched_flush_threshold items
    // so workers receive tasks while the system thread is still processing the batch.
    static BATCH_SCHED_BUF: RefCell<Vec<NodeInfo>> = RefCell::new(Vec::with_capacity(256));
    // Reusable staging buffer for task completion batches: converts Vec<NodeInfo> (from the
    // batch_queue) into Vec<(NodeInfo, Option<CmTypes>)> without per-batch heap allocation.
    // The Vec is drained (not consumed) by process_batch_resolution, so capacity is retained.
    static TASK_COMP_BUF: RefCell<Vec<(NodeInfo, Option<synstream_types::CmTypes>)>> =
        RefCell::new(Vec::with_capacity(256));
}

impl super::SynRt {
    /// Resolution Thread: Processes completed compute tasks and manages stream lifecycle
    pub(super) fn resolution(
        shared: Arc<SharedData>,
        thread_core: usize,
        thread_id: usize,
        thread_slot: usize,
    ) {
        // Initialize async recorder for system thread using universal indexing
        if let Some(ref recorder) = shared.telemetry.async_recorder {
            let channel_index = thread_core - shared.config.core_offset;
            if let Some(tx) = recorder.get_worker_sender(channel_index) {
                set_worker_recorder(tx);
            }
        }

        Self::perform_initial_preparation(&shared, thread_id, thread_core, thread_slot);

        // prefetch cond indexes for efficiency
        let cond_indexes = shared.graph.get_condition_indexes();

        // Persistent completion tracking across all batches for this stream
        let mut stream_slot_activity: HashMap<usize, bool> = HashMap::new();

        // Cached slot list for check_slots — avoids running_streams.read() every iteration.
        // Refreshed only when a stream is assigned or released (slots_dirty = true).
        let mut cached_slots: Vec<usize> = Vec::new();
        let mut slots_dirty = true; // force refresh on first check_slots call

        // Packet Process Function
        let network_config_opt = shared.graph.network_config();

        let _receive_timeout = Duration::from_micros(shared.config.batch_timeout_us);

        // Reusable drain buffer — allocated once, keeps capacity across loop iterations.
        // Avoids a Vec<PacketMessage> allocation on every drain call.
        let mut packet_buf: Vec<PacketMessage> = Vec::new();

        // Reusable batch buffer — keeps capacity warm in L1 cache across iterations.
        let mut batch_buf: Vec<NodeInfo> = Vec::with_capacity(shared.config.target_batch_size);

        // Process completed nodes with dynamic batching from scheduler
        loop {
            // Check shutdown flag first to exit immediately when signaled
            if shared.net.shutdown_flag.load(Ordering::Acquire) {
                println!(
                    "Thread {} detected shutdown signal, exiting resolution loop",
                    thread_id
                );
                break;
            }

            // Poll packet channels if there is a network config AND receivers are still active
            let should_poll_packets =
                network_config_opt.is_some() && !shared.net.receive_finished.load(Ordering::Acquire);

            if should_poll_packets {
                if let Some(network_config) = network_config_opt.as_ref() {
                    poll_and_process_network_packets(
                        &shared,
                        network_config,
                        &mut packet_buf,
                        &mut slots_dirty,
                        &cond_indexes,
                        &mut stream_slot_activity,
                        thread_core,
                        thread_id,
                        thread_slot,
                    );
                }
            }

            drain_and_process_batch_queue(
                &shared,
                &mut batch_buf,
                &cond_indexes,
                &mut stream_slot_activity,
                thread_core,
                thread_id,
                thread_slot,
            );

            // Check shutdown immediately after blocking call returns
            if shared.net.shutdown_flag.load(Ordering::Acquire) {
                println!(
                    "Thread {} detected shutdown after receive, exiting",
                    thread_id
                );
                break;
            }

            // Also check stream completion here (before processing batch)
            // This ensures threads exit promptly even if shutdown_flag hasn't been set yet
            {
                let completed_streams = shared.telemetry.stream_complete_counter.load(Ordering::Acquire);
                if completed_streams >= shared.config.max_streams {
                    println!(
                        "Thread {} detected all streams completed (after recv_batch), exiting",
                        thread_id
                    );
                    break;
                }
            }

            let start_proc = shared.telemetry.measure_start();
            // Check slots for completion
            Self::check_slots(
                &shared,
                &mut stream_slot_activity,
                thread_id,
                thread_core,
                thread_slot,
                &cond_indexes,
                &mut cached_slots,
                &mut slots_dirty,
            );
            shared.telemetry.record_timing(start_proc, thread_slot, "Slot Check", usize::MAX);

            // Check for completion of all streams
            let completed_streams = shared.telemetry.stream_complete_counter.load(Ordering::Acquire);

            if completed_streams >= shared.config.max_streams {
                println!(
                    "Thread {} detected all streams completed, exiting resolution loop",
                    thread_id
                );
                break;
            }
        }
    }

    /// Process a batch of completed nodes: store results, update dependencies, schedule successors
    /// Returns true if work was performed (for timing/recording purposes)
    /// Process a batch of completed nodes (both network packets and compute tasks).
    ///
    /// `batch` is passed as `&mut Vec` and drained in-place so the caller retains
    /// Vec capacity for reuse, eliminating per-batch heap allocation on the hot path.
    ///
    /// `result` in each tuple:
    /// - `Some(cm)` for network packets — result stored in node_results (Phase 1).
    /// - `None` for compute tasks — result already pre-stored by the worker in execute_task.
    pub(super) fn process_batch_resolution(
        shared: &Arc<SharedData>,
        batch: &mut Vec<(NodeInfo, Option<CmTypes>)>,
        thread_core: usize,
        thread_id: usize,
        thread_slot: usize,
        cond_indexes: &[Vec<usize>],
        stream_slot_activity: &mut HashMap<usize, bool>,
        start_ns: u128,
    ) {
        if batch.is_empty() {
            return;
        }

        // 6A: Track which slots are in this batch using a bitset (no heap allocation).
        let mut slots_in_batch: u64 = 0;
        for (node_info, _) in batch.iter() {
            slots_in_batch |= 1u64 << node_info.slot;
        }

        // Increment processing_count for all slots in this batch
        {
            let mut bits = slots_in_batch;
            while bits != 0 {
                let slot = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                shared.slot_data.processing_count[slot].fetch_add(1, Ordering::SeqCst);
            }
        }

        // Phases 1+2+3: For each node — store result, decrement counters, process successors.
        // All three phases run together per node while processing_count > 0, so completion
        // detection cannot fire until Phase 4 decrements processing_count after this loop.
        //
        // Opt: Successor nodes are accumulated into BATCH_SCHED_BUF across all nodes in the
        // batch; a single preparation() call is made after the loop instead of one per node.
        // This reduces scheduler submissions from O(batch_size) to O(1) per batch.
        SUCC_UPDATES_BUF.with(|sbuf| {
            SCHEDULE_BUF.with(|tbuf| {
                READY_BUF.with(|rbuf| {
                    BATCH_SCHED_BUF.with(|bbuf| {
                        process_batch_inner(
                            shared,
                            batch,
                            thread_core,
                            thread_id,
                            thread_slot,
                            cond_indexes,
                            stream_slot_activity,
                            &mut *sbuf.borrow_mut(),
                            &mut *tbuf.borrow_mut(),
                            &mut *rbuf.borrow_mut(),
                            &mut *bbuf.borrow_mut(),
                        );
                    })
                })
            })
        });

        // Lock-free recording via per-worker channel
        let should_record = shared.telemetry.async_recorder.is_some() && {
            let mut any = false;
            let mut bits = slots_in_batch;
            while bits != 0 {
                let slot = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                if should_record_slot(&shared, slot) {
                    any = true;
                    break;
                }
            }
            any
        };
        if should_record {
            let job_id = shared.telemetry.job_counter.fetch_add(1, Ordering::SeqCst);
            let end_ns = shared.telemetry.base_instant.elapsed().as_nanos();
            submit_record(Record {
                slot: thread_slot,
                job_id,
                start_ns,
                end_ns,
                worker: thread_core,
                task_id: IdType::MAX,
                index: 0,
            });
        }

        // Phase 4: Decrement processing_count AFTER all successor processing
        {
            let mut bits = slots_in_batch;
            while bits != 0 {
                let slot = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                shared.slot_data.processing_count[slot].fetch_sub(1, Ordering::SeqCst);
                shared.slot_data.needs_check[slot].store(true, Ordering::Release);
            }
        }
    }

    /// CAS-guarded initial stream setup: only the first thread (thread_id == 0) that wins
    /// the compare_exchange activates the initial set of streams and spawns their compute
    /// nodes. All other threads skip this section.
    pub(super) fn perform_initial_preparation(
        shared: &Arc<SharedData>,
        thread_id: usize,
        thread_core: usize,
        thread_slot: usize,
    ) {
        if thread_id != 0 {
            return;
        }
        // Ensure only one thread does initial preparation
        if shared
            .exec.initial_prep_done
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        print_debug(|| {
            format!(
                "Thread {} in Core {} performing initial preparation",
                thread_id, thread_core
            )
        });

        let activate_streams: Vec<usize> = if shared.config.slot_priority_enabled {
            // activate first stream
            vec![0]
        } else {
            // activate all streams
            (0..shared.config.slots).collect()
        };

        if !shared.graph.initial_nodes.is_empty() {
            // run assign_stream_to_available_slot for each stream to set slot state to Active
            let assigned_slots: Vec<usize> = activate_streams
                .iter()
                .map(|&stream_id| {
                    assign_stream_to_available_slot(shared, stream_id)
                        .expect("initial slot assignment must succeed")
                        .0
                })
                .collect();

            let compute_nodes = initial_nodes(shared, assigned_slots);
            if !compute_nodes.is_empty() {
                Self::preparation(shared, &compute_nodes, thread_core, thread_slot);
            }
        }
    }
}

/// Drains all available network packets, assigns each to a slot, and processes
/// the active ones as a single batch.  The outer `if should_poll_packets` and
/// `if let Some(network_config)` guards remain in `resolution()`; this function
/// is called only when both conditions are true.
fn poll_and_process_network_packets(
    shared: &Arc<SharedData>,
    network_config: &crate::graph_struct::GraphNetworkConfig,
    packet_buf: &mut Vec<PacketMessage>,
    slots_dirty: &mut bool,
    cond_indexes: &[Vec<usize>],
    stream_slot_activity: &mut HashMap<usize, bool>,
    thread_core: usize,
    thread_id: usize,
    thread_slot: usize,
) {
    let stream_packets = network_config.stream_packets;
    let packet_process_func = network_config.extract_packet_func.unwrap();

    // Cache index_function pointer outside packet loop to avoid
    // redundant network_config lookups per packet.
    let idx_func_ptr: Option<(synstream_types::CmPtr, &Vec<crate::graph_struct::Arg>)> =
        network_config
            .index_function
            .as_ref()
            .and_then(|idx_func| idx_func.func_ptr.map(|fp| (fp, &idx_func.args)));

    // Drain all available packets into reusable buffer (no Vec alloc per call).
    packet_buf.clear();
    packet_buf.extend(shared.net.packet_receiver.drain());
    let packet_rcv_instant = Instant::now();

    let mut active_packet_batch: Vec<(NodeInfo, Option<CmTypes>)> =
        Vec::with_capacity(packet_buf.len());

    for packet_msg in packet_buf.drain(..) {
        let receiver_core_id = packet_msg.receiver_core_id;
        let packet_timestamp = packet_msg.timestamp;
        shared.telemetry.with_timing(|tb| {
            let dur = packet_rcv_instant.duration_since(packet_timestamp);
            tb.add_task_time(thread_slot, "Packet Received", usize::MAX, dur);
        });

        let packet_cm = decode_packet(shared, packet_msg, packet_process_func, thread_slot);

        let start_id = shared.telemetry.measure_start();
        let new_stream_opt = process_id_function(shared, &packet_cm);
        shared.telemetry.record_timing(start_id, thread_slot, "ID Function", usize::MAX);

        let Some(new_stream) = new_stream_opt else {
            print_debug(|| {
                format!("Thread {:?} -- Skipping packet: ID function returned None", thread_id)
            });
            continue;
        };

        let Some(node_info) = assign_packet_to_slot(
            shared,
            new_stream,
            &packet_cm,
            idx_func_ptr,
            slots_dirty,
            thread_core,
            thread_slot,
        ) else {
            continue;
        };

        let slot_is_active =
            shared.slot_data.active_bitmap.load(Ordering::Acquire) & (1u64 << node_info.slot) != 0;
        if slot_is_active {
            active_packet_batch.push((node_info.clone(), Some(packet_cm)));
        } else {
            let mut slot_buffers = shared.slot_data.buffers.write();
            slot_buffers[node_info.slot].push((node_info.clone(), Some(packet_cm)));
            drop(slot_buffers);
        }

        if shared.telemetry.async_recorder.is_some()
            && should_record_slot(shared, node_info.slot)
        {
            let receiver_slot = shared.config.slots + shared.config.system_threads;
            let job_id = shared.telemetry.job_counter.fetch_add(1, Ordering::SeqCst);
            let packet_rcv = packet_timestamp
                .duration_since(*shared.telemetry.base_instant)
                .as_nanos();
            submit_record(Record {
                slot: receiver_slot,
                job_id,
                start_ns: packet_rcv,
                end_ns: packet_rcv + 10000u128, // small delta for graph visibility
                worker: receiver_core_id,
                task_id: 0,
                index: node_info.index,
            });
        }

        check_stream_completion(shared, node_info.slot, thread_id, stream_packets);
    }

    if !active_packet_batch.is_empty() {
        let start_ns_batch = shared.telemetry.base_instant.elapsed().as_nanos();
        let start_proc = shared.telemetry.measure_start();
        super::SynRt::process_batch_resolution(
            shared,
            &mut active_packet_batch,
            thread_core,
            thread_id,
            thread_slot,
            cond_indexes,
            stream_slot_activity,
            start_ns_batch,
        );
        shared.telemetry.record_timing(start_proc, thread_slot, "Batch Resolution", usize::MAX);
    }
}

/// Decodes raw packet bytes through `packet_process_func` and reclaims the
/// underlying buffer back to the originating receiver thread via its SPSC channel.
fn decode_packet(
    shared: &Arc<SharedData>,
    packet_msg: PacketMessage,
    packet_process_func: synstream_types::CmPtr,
    thread_slot: usize,
) -> CmTypes {
    let socket_id = packet_msg.socket_id;
    // Bytes variant avoids Arc/RwLock/Box overhead.
    // Keep received_bytes_cm alive (not moved) so we can reclaim its Vec<u8> below.
    let received_bytes_cm = CmTypes::from_bytes(packet_msg.packet_bytes);
    let start_proc = shared.telemetry.measure_start();
    // Pass a clone (cheap Arc increment) so received_bytes_cm stays alive for reclaim.
    let packet_cm = packet_process_func(&[received_bytes_cm.clone()]);
    shared.telemetry.record_timing(start_proc, thread_slot, "Packet Processing", usize::MAX);
    // try_unwrap succeeds when plugin only borrowed via &[CmTypes] and did not clone the Arc.
    // Routes via per-socket SPSC return channel; try_send is non-blocking.
    if let CmTypes::Bytes(arc) = received_bytes_cm {
        if let Ok(buf) = Arc::try_unwrap(arc) {
            if let Some(tx) = shared.net.buffer_return_senders.get(socket_id) {
                let _ = tx.try_send(buf);
            }
        }
    }
    packet_cm
}

/// Assigns `new_stream` to an available slot, spawns initial nodes if the slot was
/// newly activated, and computes the packet's per-slot index.
///
/// Returns `Some(NodeInfo)` with `slot` and `index` populated, or `None` if the
/// frame was dropped or all slots are occupied.
fn assign_packet_to_slot(
    shared: &Arc<SharedData>,
    new_stream: usize,
    packet_cm: &CmTypes,
    idx_func_ptr: Option<(synstream_types::CmPtr, &Vec<crate::graph_struct::Arg>)>,
    slots_dirty: &mut bool,
    thread_core: usize,
    thread_slot: usize,
) -> Option<NodeInfo> {
    // Fast-path: frame already dropped — discard without touching any shared state.
    if new_stream < shared.net.frame_dropped.len()
        && shared.net.frame_dropped[new_stream].load(Ordering::Acquire)
    {
        return None;
    }

    let start_sa = shared.telemetry.measure_start();
    let (assigned_slot, newly_activated) = match assign_stream_to_available_slot(shared, new_stream) {
        Some(v) => v,
        None => {
            // All slots occupied — drop this frame, mark exactly once.
            if new_stream < shared.net.frame_dropped.len() {
                let already_marked =
                    shared.net.frame_dropped[new_stream].swap(true, Ordering::AcqRel);
                if !already_marked {
                    shared.telemetry.stream_complete_counter.fetch_add(1, Ordering::SeqCst);
                    let dropped =
                        shared.net.dropped_streams.fetch_add(1, Ordering::Relaxed) + 1;
                    eprintln!(
                        "Frame {} dropped: no available slots ({} dropped total)",
                        new_stream, dropped
                    );
                }
            }
            return None;
        }
    };
    shared.telemetry.record_timing(start_sa, thread_slot, "Slot Assignment", usize::MAX);

    if newly_activated {
        *slots_dirty = true;
        // Spawn initial nodes immediately so workers start while remaining packets arrive.
        let init_nodes = initial_nodes(shared, vec![assigned_slot]);
        if !init_nodes.is_empty() {
            print_debug(|| {
                format!(
                    "Slot {} newly activated (stream {}), spawning {} initial nodes",
                    assigned_slot,
                    new_stream,
                    init_nodes.len()
                )
            });
            super::SynRt::preparation(shared, &init_nodes, thread_core, thread_slot);
        }
    }

    let packet_index = if let Some((idx_fn, idx_args)) = idx_func_ptr {
        let additional_args = super::arg_resolution::parse_args(
            shared,
            idx_args,
            0,             // node_index (network node)
            assigned_slot,
            0,             // pred_index
            None,
        );
        let mut full_args = Vec::with_capacity(1 + additional_args.len());
        full_args.push(packet_cm.clone());
        full_args.extend(additional_args);
        let idx_result = idx_fn(&full_args);
        shared.slot_data.packet_counters[assigned_slot].fetch_add(1, Ordering::SeqCst);
        idx_result
            .valid_number_to_usize()
            .expect("index_function must return usize")
    } else {
        shared.slot_data.packet_counters[assigned_slot].fetch_add(1, Ordering::SeqCst)
    };

    Some(NodeInfo::new(0, assigned_slot, packet_index, 0))
}

/// Checks whether all expected packets for `slot` have been received.  On first
/// completion, increments the streams-received counter and signals receivers to
/// stop if all streams are done.
fn check_stream_completion(
    shared: &Arc<SharedData>,
    slot: usize,
    thread_id: usize,
    stream_packets: usize,
) {
    let packet_count = shared.slot_data.packet_counters[slot].load(Ordering::SeqCst);
    if packet_count != stream_packets {
        return;
    }

    // Exactly-once semantics: atomically claim completion ownership.
    let already_completed =
        shared.slot_data.packet_complete[slot].swap(true, Ordering::SeqCst);
    if already_completed {
        print_debug(|| {
            format!(
                "Thread {:?} -- Slot {} completion already claimed by another thread",
                thread_id, slot
            )
        });
        return;
    }

    let pending_tasks = shared.slot_data.pending_tasks[slot].load(Ordering::SeqCst);
    let pending_cond = shared.slot_data.pending_cond_tasks[slot].load(Ordering::SeqCst);
    print_debug(|| {
        format!(
            "Thread {:?} -- All {} packets received for slot {} | pending_tasks={}, pending_cond={}",
            thread_id, stream_packets, slot, pending_tasks, pending_cond
        )
    });

    let completed_streams =
        shared.net.streams_receive_counter.fetch_add(1, Ordering::AcqRel) + 1;
    if completed_streams >= shared.config.max_streams {
        println!(
            "All {} streams received ({} packets each) - receivers will shutdown",
            shared.config.max_streams, stream_packets
        );
        shared.net.receive_finished.store(true, Ordering::Release);
    }
}

/// Drains up to `target_batch_size` items from the batch queue (with a brief spin),
/// filters stale tasks, and delegates to `process_batch_resolution`.
///
/// The two early-exit checks (shutdown_flag, stream_complete_counter) remain in
/// `resolution()` immediately after this call so the loop can `break` directly.
fn drain_and_process_batch_queue(
    shared: &Arc<SharedData>,
    batch_buf: &mut Vec<NodeInfo>,
    cond_indexes: &[Vec<usize>],
    stream_slot_activity: &mut HashMap<usize, bool>,
    thread_core: usize,
    thread_id: usize,
    thread_slot: usize,
) {
    // Pull up to target_batch_size items from batch_queue.
    // With worker-side resolution, most compute completions bypass batch_queue,
    // so we must not block here — check_slots needs to run promptly to detect
    // slot completion. Use non-blocking try_iter only; the spin+recv_timeout
    // path is only taken when no worker-resolvable nodes exist (all traffic
    // flows through batch_queue).
    batch_buf.clear();
    batch_buf
        .extend(shared.exec.batch_queue_rx.try_iter().take(shared.config.target_batch_size));
    if batch_buf.is_empty() {
        // Brief spin to catch burst completions landing just after try_iter()
        for _ in 0..shared.config.spin_iterations {
            std::hint::spin_loop();
            if let Ok(item) = shared.exec.batch_queue_rx.try_recv() {
                batch_buf.push(item);
                batch_buf.extend(
                    shared
                        .exec
                        .batch_queue_rx
                        .try_iter()
                        .take(shared.config.target_batch_size - 1),
                );
                break;
            }
        }
    }

    if batch_buf.is_empty() {
        return;
    }

    let start_ns_batch = shared.telemetry.base_instant.elapsed().as_nanos();
    let start_proc = shared.telemetry.measure_start();
    TASK_COMP_BUF.with(|tbuf| {
        let mut comp_batch = tbuf.borrow_mut();
        comp_batch.clear();
        // Extend with (NodeInfo, None): result is already in node_results (pre-stored
        // by execute_task). The None signals process_batch_resolution to skip Phase 1.
        // Filter out stale tasks: workers that passed the gen check in execute_task
        // before the slot's generation was bumped will complete and submit to
        // batch_queue with the old gen. Processing these would corrupt the new
        // stream's pending counters and dependency state (Bug #31).
        // Cache per-slot generation locally — reduces ~256 SeqCst loads to ~1-2 per unique slot.
        let mut gen_cache: [u32; 64] = [0; 64];
        let mut gen_loaded: u64 = 0;
        comp_batch.extend(
            batch_buf
                .drain(..)
                .filter(|n| {
                    if gen_loaded & (1u64 << n.slot) == 0 {
                        gen_cache[n.slot] = shared.slot_data.generation[n.slot]
                            .load(Ordering::SeqCst) as u32;
                        gen_loaded |= 1u64 << n.slot;
                    }
                    if n.gen != gen_cache[n.slot] {
                        print_debug(|| {
                            format!(
                                "Stale batch completion dropped: node {} slot {} index {} gen {} (current {})",
                                n.id, n.slot, n.index, n.gen, gen_cache[n.slot]
                            )
                        });
                        return false;
                    }
                    true
                })
                .map(|n| (n, None)),
        );
        super::SynRt::process_batch_resolution(
            shared,
            &mut *comp_batch,
            thread_core,
            thread_id,
            thread_slot,
            cond_indexes,
            stream_slot_activity,
            start_ns_batch,
        );
        // comp_batch is now empty (drained); capacity is retained for the next call.
    });
    shared.telemetry.record_timing(start_proc, thread_slot, "Batch Resolution", usize::MAX);
}

/// Inner body of `process_batch_resolution` executed with all four thread-local buffers
/// already borrowed. Separated from the outer function to eliminate the 4-deep
/// `thread_local!` `.with()` nesting while keeping the thread-local declarations intact.
fn process_batch_inner(
    shared: &Arc<SharedData>,
    batch: &mut Vec<(NodeInfo, Option<CmTypes>)>,
    thread_core: usize,
    thread_id: usize,
    thread_slot: usize,
    cond_indexes: &[Vec<usize>],
    stream_slot_activity: &mut HashMap<usize, bool>,
    succ_buf: &mut Vec<(NodeInfo, bool, IdType, Option<usize>)>,
    sched: &mut Vec<NodeInfo>,
    ready: &mut Vec<usize>,
    batch_sched: &mut Vec<NodeInfo>,
) {
    batch_sched.clear();

    for (node_info, result_opt) in batch.drain(..) {
        // Mark stream activity for all nodes (including network nodes id=0)
        stream_slot_activity.insert(node_info.slot, true);

        if node_info.post_node {
            // For post_nodes: result pre-stored by execute_task (None here).
            // Network post_nodes (rare) carry Some(result) and store it now.
            if let Some(r) = result_opt {
                shared.exec.node_results.set(&node_info, r);
            }
            continue;
        }

        // Phase 1: Store result for network packets (Some).
        // Compute task results are already stored by execute_task (None).
        if let Some(r) = result_opt {
            shared.exec.node_results.set(&node_info, r);
        }

        // Phase 2: Decrement task counters
        let node_id_usize = node_info.id as usize;
        let node_cache_entry = &shared.graph_cache.node_cache[node_id_usize];

        if node_cache_entry.is_condition {
            let prev_cond = shared.slot_data.pending_cond_tasks[node_info.slot]
                .fetch_sub(1, Ordering::SeqCst);
            if prev_cond <= 10 || prev_cond % 100 == 0 {
                print_debug(|| {
                    format!(
                        "COND task completed: slot={}, node_id={} ({}), prev_pending_cond={}, new={}",
                        node_info.slot, node_id_usize, node_cache_entry.name,
                        prev_cond, prev_cond - 1
                    )
                });
            }
        } else if !node_cache_entry.is_initial {
            let _ = shared.slot_data.pending_tasks[node_info.slot]
                .fetch_sub(1, Ordering::SeqCst);
        }

        // Phase 3: Collect successors and process them (no allocations)
        collect_successors_for_node_into(shared, &node_info, succ_buf);
        sched.clear();

        // Load slot generation once per node (all successors share same slot)
        let slot_gen = shared.slot_data.generation[node_info.slot]
            .load(Ordering::SeqCst) as u32;

        for (_succ_info, has_cond, succ_id, pred_group) in succ_buf.iter() {
            let succ_node_id = *succ_id as usize;

            // Skip condition evaluation if all instances already spawned.
            // Use generational lazy check: if stored gen != slot_gen, treat as full factor.
            if *has_cond {
                let packed = shared.slot_data.cond_instances_to_spawn[node_info.slot][succ_node_id]
                    .load(Ordering::SeqCst);
                let stored_gen = crate::buffers::gen_unpack_gen(packed);
                let remaining_spawns = if stored_gen == slot_gen {
                    crate::buffers::gen_unpack_val(packed)
                } else {
                    shared.graph_cache.node_cache[succ_node_id].factor as u32 // stale gen → full factor
                };
                if remaining_spawns == 0 {
                    continue;
                }
            }

            // Decrement dependency counter; ready indices written into `ready`.
            // For 1:1 non-barrier deps, pass specific_succ_idx so the
            // exact successor instance that reads this predecessor fires,
            // guaranteeing its result is available (no spin_wait needed).
            let specific_succ_idx = shared
                .graph_cache.pred_succ_1to1_offset
                .get(succ_node_id)
                .and_then(|v| v.get(node_info.id as usize))
                .and_then(|o| *o)
                .map(|k| {
                    let f = shared.graph_cache.node_cache[succ_node_id].factor;
                    ((node_info.index as isize - k).rem_euclid(f as isize)) as usize
                });
            shared.exec.resolution_state.decrease_and_get_ready_into(
                node_info.slot,
                succ_node_id,
                slot_gen,
                *pred_group,
                1,
                specific_succ_idx,
                ready,
            );

            for &ready_index in ready.iter() {
                let scheduled_succ_info = NodeInfo::new(
                    succ_node_id as IdType,
                    node_info.slot,
                    ready_index,
                    node_info.index,
                );

                if !has_cond {
                    sched.push(scheduled_succ_info);
                } else {
                    dispatch_condition_successor(
                        shared,
                        &node_info,
                        scheduled_succ_info,
                        succ_node_id,
                        slot_gen,
                        cond_indexes,
                        sched,
                    );
                }
            }
        }

        // Accumulate this node's successors into the batch buffer.
        batch_sched.extend_from_slice(sched);
        print_debug(|| {
            format!(
                "Thread {:?} -- Processed node {} in slot {}, scheduled: {:?}",
                thread_id,
                node_info.id,
                node_info.slot,
                sched.iter().map(|n| (n.id, n.index)).collect::<Vec<_>>()
            )
        });

        // Incremental flush: dispatch accumulated successors to workers
        // as soon as we have enough, rather than waiting for the entire
        // batch to finish. This eliminates the dead zone where workers
        // idle while the system thread processes a large batch.
        if batch_sched.len() >= shared.config.sched_flush_threshold {
            super::SynRt::preparation(shared, batch_sched, thread_core, thread_slot);
            batch_sched.clear();
        }
    }

    // Final flush for any remaining successors after the batch loop.
    if !batch_sched.is_empty() {
        super::SynRt::preparation(shared, batch_sched, thread_core, thread_slot);
    }
}

/// Evaluates the condition for a successor node that has `has_cond == true` and either
/// pushes it onto `sched` (condition passed) or increments its dependency counter and
/// resets its sent flag (condition failed).
fn dispatch_condition_successor(
    shared: &Arc<SharedData>,
    node_info: &NodeInfo,
    scheduled_succ_info: NodeInfo,
    succ_node_id: usize,
    slot_gen: u32,
    cond_indexes: &[Vec<usize>],
    sched: &mut Vec<NodeInfo>,
) {
    let cond_idx = shared.graph_cache.node_cache[succ_node_id].cond_index;
    let succ_cache = &shared.graph_cache.node_cache[succ_node_id];

    let condition_passed = if let Some(cond_cache) = &succ_cache.node_condition {
        let node_cond = shared.graph.nodes[succ_node_id]
            .condition
            .as_ref()
            .unwrap();
        evaluate_node_condition(shared, &scheduled_succ_info, cond_cache, node_cond)
    } else {
        conditions_met(shared, &scheduled_succ_info, &cond_indexes[cond_idx])
    };

    if condition_passed {
        sched.push(scheduled_succ_info.clone());
        // Decrement cond_instances_to_spawn with generational lazy reinit
        let factor = shared.graph_cache.node_cache[succ_node_id].factor as u32;
        let prev_packed = shared.slot_data.cond_instances_to_spawn[node_info.slot]
            [succ_node_id]
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |packed| {
                let stored_gen = crate::buffers::gen_unpack_gen(packed);
                let current = if stored_gen == slot_gen {
                    crate::buffers::gen_unpack_val(packed)
                } else {
                    factor // lazy reinit
                };
                Some(crate::buffers::gen_pack(slot_gen, current.saturating_sub(1)))
            })
            .unwrap();
        let prev_spawns = {
            let sg = crate::buffers::gen_unpack_gen(prev_packed);
            if sg == slot_gen {
                crate::buffers::gen_unpack_val(prev_packed) as usize
            } else {
                factor as usize
            }
        };
        print_debug(|| {
            format!(
                "Condition passed for node {} ({}) index {}: remaining spawns {} -> {}",
                succ_node_id,
                succ_cache.name,
                scheduled_succ_info.index,
                prev_spawns,
                prev_spawns.saturating_sub(1)
            )
        });
    } else {
        shared
            .exec.resolution_state
            .increment_dependency(&scheduled_succ_info, slot_gen);
        shared.exec.resolution_state.reset_sent(
            node_info.slot,
            scheduled_succ_info.id as usize,
            scheduled_succ_info.index,
            slot_gen,
        );
    }
}
