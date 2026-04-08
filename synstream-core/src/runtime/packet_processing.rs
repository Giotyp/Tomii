/// Per-packet network processing: slot assignment, indexing, and completion tracking.
use super::network_init::process_id_function;
use super::reporting::should_record_slot;
use super::shared_data::SharedData;
use super::slot_management::{assign_stream_to_available_slot, initial_nodes};
use crate::async_recorder::submit_record;
use crate::buffers::NodeInfo;
use crate::debug::print_debug;
use crate::network::PacketMessage;
use crate::Record;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;
use synstream_types::*;

/// Drains all available network packets, assigns each to a slot, and processes
/// the active ones as a single batch.  The outer `if should_poll_packets` and
/// `if let Some(network_config)` guards remain in `resolution()`; this function
/// is called only when both conditions are true.
pub(super) fn poll_and_process_network_packets(
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
        shared
            .telemetry
            .record_timing(start_id, thread_slot, "ID Function", usize::MAX);

        let Some(new_stream) = new_stream_opt else {
            print_debug(|| {
                format!(
                    "Thread {:?} -- Skipping packet: ID function returned None",
                    thread_id
                )
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
            && should_record_slot(&shared.config, &shared.slot_data, node_info.slot)
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
        shared
            .telemetry
            .record_timing(start_proc, thread_slot, "Batch Resolution", usize::MAX);
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
    shared
        .telemetry
        .record_timing(start_proc, thread_slot, "Packet Processing", usize::MAX);
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
    let (assigned_slot, newly_activated) = match assign_stream_to_available_slot(shared, new_stream)
    {
        Some(v) => v,
        None => {
            // All slots occupied — drop this frame, mark exactly once.
            if new_stream < shared.net.frame_dropped.len() {
                let already_marked =
                    shared.net.frame_dropped[new_stream].swap(true, Ordering::AcqRel);
                if !already_marked {
                    shared
                        .telemetry
                        .stream_complete_counter
                        .fetch_add(1, Ordering::SeqCst);
                    let dropped = shared.net.dropped_streams.fetch_add(1, Ordering::Relaxed) + 1;
                    eprintln!(
                        "Frame {} dropped: no available slots ({} dropped total)",
                        new_stream, dropped
                    );
                }
            }
            return None;
        }
    };
    shared
        .telemetry
        .record_timing(start_sa, thread_slot, "Slot Assignment", usize::MAX);

    if newly_activated {
        *slots_dirty = true;
        // Spawn initial nodes immediately so workers start while remaining packets arrive.
        let init_nodes = initial_nodes(&shared.graph, vec![assigned_slot]);
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
            0, // node_index (network node)
            assigned_slot,
            0, // pred_index
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
    let already_completed = shared.slot_data.packet_complete[slot].swap(true, Ordering::SeqCst);
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

    let completed_streams = shared
        .net
        .streams_receive_counter
        .fetch_add(1, Ordering::AcqRel)
        + 1;
    if completed_streams >= shared.config.max_streams {
        println!(
            "All {} streams received ({} packets each) - receivers will shutdown",
            shared.config.max_streams, stream_packets
        );
        shared.net.receive_finished.store(true, Ordering::Release);
    }
}
