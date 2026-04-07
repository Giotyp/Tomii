use super::batch_resolution::process_batch_inner;
use super::packet_processing::poll_and_process_network_packets;
use super::reporting::should_record_slot;
use super::shared_data::SharedData;
use super::slot_management::{assign_stream_to_available_slot, initial_nodes};
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
use std::time::Duration;
use synstream_types::*;

/// Reusable scratch buffers for `process_batch_resolution` → `process_batch_inner`.
/// Bundled into one struct so a single `thread_local!` entry replaces four,
/// collapsing the former 4-deep `.with()` nesting to one level.
struct BatchInnerBuffers {
    succ_updates: Vec<(NodeInfo, bool, IdType, Option<usize>)>,
    schedule:     Vec<NodeInfo>,
    ready:        Vec<usize>,
    batch_sched:  Vec<NodeInfo>,
}

thread_local! {
    // Batch resolution inner buffers — all four in one allocation.
    static BATCH_INNER_BUFS: RefCell<BatchInnerBuffers> = RefCell::new(BatchInnerBuffers {
        succ_updates: Vec::with_capacity(32),
        schedule:     Vec::with_capacity(32),
        ready:        Vec::with_capacity(16),
        batch_sched:  Vec::with_capacity(256),
    });
    // Staging buffer for task completions drained from batch_queue.
    // Kept separate from BATCH_INNER_BUFS because drain_and_process_batch_queue holds
    // this borrow while calling process_batch_resolution (which borrows BATCH_INNER_BUFS),
    // so merging them would cause a re-entrant borrow panic.
    static TASK_COMP_BUF: RefCell<Vec<(NodeInfo, Option<CmTypes>)>> =
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

        let _receive_timeout = Duration::from_micros(shared.config.batch.timeout_us);

        // Reusable drain buffer — allocated once, keeps capacity across loop iterations.
        // Avoids a Vec<PacketMessage> allocation on every drain call.
        let mut packet_buf: Vec<PacketMessage> = Vec::new();

        // Reusable batch buffer — keeps capacity warm in L1 cache across iterations.
        let mut batch_buf: Vec<NodeInfo> = Vec::with_capacity(shared.config.batch.target_size);

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
        BATCH_INNER_BUFS.with(|bufs| {
            let mut bufs = bufs.borrow_mut();
            let BatchInnerBuffers { succ_updates, schedule, ready, batch_sched } = &mut *bufs;
            process_batch_inner(
                shared,
                batch,
                thread_core,
                thread_id,
                thread_slot,
                cond_indexes,
                stream_slot_activity,
                succ_updates,
                schedule,
                ready,
                batch_sched,
            );
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
        .extend(shared.exec.batch_queue_rx.try_iter().take(shared.config.batch.target_size));
    if batch_buf.is_empty() {
        // Brief spin to catch burst completions landing just after try_iter()
        for _ in 0..shared.config.batch.poll_spin_iters {
            std::hint::spin_loop();
            if let Ok(item) = shared.exec.batch_queue_rx.try_recv() {
                batch_buf.push(item);
                batch_buf.extend(
                    shared
                        .exec
                        .batch_queue_rx
                        .try_iter()
                        .take(shared.config.batch.target_size - 1),
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
