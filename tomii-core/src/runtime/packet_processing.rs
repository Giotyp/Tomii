//! Per-packet network processing: decoding, slot assignment, index computation, and receive
//! completion tracking. Active only when the `network` feature is enabled.
use super::network_init::process_id_function;
use super::reporting::should_record_slot;
use super::shared_data::{PendingPacket, SharedData};
use super::slot_management::{assign_frame_to_available_slot, initial_nodes};
use crate::async_recorder::submit_record;
use crate::buffers::NodeInfo;
use crate::debug::print_debug;
use crate::network::PacketMessage;
use crate::Record;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;
use tomii_types::*;

/// Drains all available network packets, assigns each to a slot, and processes
/// the active ones as a single batch.  The outer `if should_poll_packets` and
/// `if let Some(network_config)` guards remain in `resolution()`; this function
/// is called only when both conditions are true.
#[allow(clippy::too_many_arguments)]
pub(super) fn poll_and_process_network_packets(
    shared: &Arc<SharedData>,
    network_config: &crate::graph_struct::GraphNetworkConfig,
    packet_buf: &mut Vec<PacketMessage>,
    slots_dirty: &mut bool,
    cond_indexes: &[Vec<usize>],
    frame_slot_activity: &mut HashMap<usize, bool>,
    thread_core: usize,
    thread_id: usize,
    thread_slot: usize,
) {
    let frame_packets = network_config.frame_packets;
    // Publish for the eviction check in check_slots (cheap; once per drain).
    shared
        .net
        .frame_packets
        .store(frame_packets, Ordering::Relaxed);
    let packet_process_func = network_config.extract_packet_func.unwrap();

    // Cache index_function pointer outside packet loop to avoid
    // redundant network_config lookups per packet.
    let idx_func_ptr: Option<(tomii_types::CmPtr, &Vec<crate::graph_struct::Arg>)> = network_config
        .index_function
        .as_ref()
        .and_then(|idx_func| idx_func.func_ptr.map(|fp| (fp, &idx_func.args)));

    // Drain all available packets into reusable buffer (no Vec alloc per call).
    packet_buf.clear();
    packet_buf.extend(shared.net.packet_receiver.drain());
    let packet_rcv_instant = Instant::now();

    let mut active_packet_batch: Vec<(NodeInfo, Option<CmTypes>)> =
        Vec::with_capacity(packet_buf.len());

    // Re-admit parked packets whose frames have entered the admission window.
    // Done before the fresh drain so older (parked) packets are processed first.
    // Relaxed is only an emptiness hint — a stale read delays this by one poll.
    if shared.net.pending_count.load(Ordering::Relaxed) > 0 {
        reinject_pending_frames(
            shared,
            idx_func_ptr,
            slots_dirty,
            &mut active_packet_batch,
            thread_core,
            thread_id,
            thread_slot,
            frame_packets,
        );
    }

    for packet_msg in packet_buf.drain(..) {
        let receiver_core_id = packet_msg.receiver_core_id;
        let packet_timestamp = packet_msg.timestamp;
        shared.telemetry.with_timing(|tb| {
            let dur = packet_rcv_instant.duration_since(packet_timestamp);
            tb.add_task_time(thread_slot, "Packet Received", usize::MAX, dur);
        });

        let packet_cm = decode_packet(shared, packet_msg, packet_process_func, thread_slot);

        let start_id = shared.telemetry.measure_start();
        let new_frame_opt = process_id_function(shared, &packet_cm);
        shared
            .telemetry
            .record_timing(start_id, thread_slot, "ID Function", usize::MAX);

        let Some(new_frame) = new_frame_opt else {
            print_debug(|| {
                format!(
                    "Thread {:?} -- Skipping packet: ID function returned None",
                    thread_id
                )
            });
            continue;
        };

        // Admission window: a frame at or beyond `completed + slots` cannot be
        // assigned a slot yet.  Park it for re-injection once the window advances —
        // dropping its head packets here would leave the frame permanently
        // incomplete and wedge the slot it is later assigned to.
        let window_end = shared
            .telemetry
            .frame_complete_counter
            .load(Ordering::SeqCst)
            + shared.config.slots;
        if new_frame >= window_end {
            park_pending_packet(
                shared,
                new_frame,
                packet_cm,
                packet_timestamp,
                receiver_core_id,
                frame_packets,
            );
            continue;
        }

        admit_packet(
            shared,
            new_frame,
            packet_cm,
            packet_timestamp,
            receiver_core_id,
            idx_func_ptr,
            slots_dirty,
            &mut active_packet_batch,
            thread_core,
            thread_id,
            thread_slot,
            frame_packets,
        );
    }

    if !active_packet_batch.is_empty() {
        let start_ns_batch = shared.telemetry.base_instant.elapsed().as_nanos();
        let start_proc = shared.telemetry.measure_start();
        // Route through the pluggable resolution strategy (the batch-protocol seam).
        let _ = shared.exec.resolution_strategy.drive_batch(
            shared,
            &mut active_packet_batch,
            thread_core,
            thread_id,
            thread_slot,
            cond_indexes,
            frame_slot_activity,
            start_ns_batch,
        );
        shared
            .telemetry
            .record_timing(start_proc, thread_slot, "Batch Resolution", usize::MAX);
    }
}

/// Admits one decoded, in-window packet: assigns it to a slot, routes it into the
/// active batch (or the slot buffer for `Buffering` slots), records receive
/// telemetry, and checks frame completion.  Shared by the fresh-packet drain
/// loop and pending-frame re-injection.
#[allow(clippy::too_many_arguments)]
fn admit_packet(
    shared: &Arc<SharedData>,
    new_frame: usize,
    packet_cm: CmTypes,
    packet_timestamp: Instant,
    receiver_core_id: usize,
    idx_func_ptr: Option<(tomii_types::CmPtr, &Vec<crate::graph_struct::Arg>)>,
    slots_dirty: &mut bool,
    active_packet_batch: &mut Vec<(NodeInfo, Option<CmTypes>)>,
    thread_core: usize,
    thread_id: usize,
    thread_slot: usize,
    frame_packets: usize,
) {
    let node_info = match assign_packet_to_slot(
        shared,
        new_frame,
        &packet_cm,
        idx_func_ptr,
        slots_dirty,
        thread_core,
        thread_slot,
    ) {
        SlotAssignment::Assigned(node_info) => node_info,
        SlotAssignment::NoSlotYet => {
            // In-window but every slot is still occupied — typically the gap
            // between the completion counter advancing and the finished slot
            // being released.  Park and retry on a later poll; dropping here
            // permanently lost frames that were milliseconds from a free slot.
            park_pending_packet(
                shared,
                new_frame,
                packet_cm,
                packet_timestamp,
                receiver_core_id,
                frame_packets,
            );
            return;
        }
        SlotAssignment::Dropped => return,
    };

    // Route the packet to the active batch if its slot is running, or into the slot
    // buffer if the slot is still Buffering (awaiting promotion).
    //
    // The active case stays lock-free: an already-active slot is never concurrently
    // drained, so an unsynchronised bitmap read is safe.
    //
    // The buffering case must be serialised against `release_and_activate_next`,
    // which promotes a Buffering slot and drains its buffer while holding
    // `states.write()` (flip → drain happen under one held lock). Previously this
    // path read the bitmap without a lock and then pushed into the slot buffer, so a
    // promote+drain could land between the read and the push: the slot flipped to
    // Active, its buffer was drained, and this packet was then pushed into a buffer
    // nothing would ever drain again — the packet was counted but its task never ran,
    // stalling the frame permanently (findings #28; likely also the radar stall #22
    // and the old 64x16 drops). Take `states.read()` and RE-CHECK the bitmap under it:
    // holding the read lock excludes the promoter's `states.write()`, so either the
    // promotion has not happened yet (push into the still-Buffering slot; the promoter
    // drains it afterwards) or it already completed (re-check sees Active; route to the
    // active batch). Lock order is states → buffers, matching the global protocol.
    let slot = node_info.slot;
    let is_active = |shared: &Arc<SharedData>| {
        shared.slot_data.active_bitmap.load(Ordering::Acquire) & (1u64 << slot) != 0
    };
    if is_active(shared) {
        active_packet_batch.push((node_info.clone(), Some(packet_cm)));
    } else {
        let slot_states = shared.slot_data.states.read();
        if is_active(shared) {
            // Promoted between the first check and acquiring the lock — its buffer has
            // already been drained, so route to the active batch instead.
            drop(slot_states);
            active_packet_batch.push((node_info.clone(), Some(packet_cm)));
        } else {
            let mut slot_buffers = shared.slot_data.buffers.write();
            slot_buffers[slot].push((node_info.clone(), Some(packet_cm)));
            drop(slot_buffers);
            drop(slot_states);
        }
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

    check_frame_completion(shared, node_info.slot, thread_id, frame_packets);
}

/// Parks a decoded packet that cannot be admitted right now — either its frame
/// is ahead of the admission window, or the frame is in-window but every slot is
/// momentarily occupied.  Capacity is one extra window's worth of packets
/// (`frame_packets × slots`); on overflow the frame furthest from admission
/// (highest frame id, the incoming packet's frame included) is dropped whole via
/// [`mark_frame_dropped`] — never partially, since partial packet loss would
/// leave a frame permanently incomplete.
fn park_pending_packet(
    shared: &Arc<SharedData>,
    frame: usize,
    packet_cm: CmTypes,
    timestamp: Instant,
    receiver_core_id: usize,
    frame_packets: usize,
) {
    // Frames the drop bitmap cannot track (beyond max_frames + slots) can never
    // enter the window before shutdown; discard outright.
    if frame >= shared.net.frame_dropped.len() {
        tracing::warn!(frame, "packet beyond trackable frame range, discarded");
        return;
    }
    if shared.net.frame_dropped[frame].load(Ordering::Acquire) {
        return;
    }

    let cap = frame_packets * shared.config.slots;
    let mut pending = shared.net.pending_frames.lock();
    if shared.net.pending_count.load(Ordering::Relaxed) >= cap {
        let victim = pending
            .keys()
            .next_back()
            .copied()
            .map_or(frame, |max_parked| max_parked.max(frame));
        if let Some(evicted) = pending.remove(&victim) {
            shared
                .net
                .pending_count
                .fetch_sub(evicted.len(), Ordering::Relaxed);
        }
        mark_frame_dropped(shared, victim, "pending-frame buffer full");
        if victim == frame {
            return;
        }
    }
    pending.entry(frame).or_default().push(PendingPacket {
        packet: packet_cm,
        timestamp,
        receiver_core_id,
    });
    shared.net.pending_count.fetch_add(1, Ordering::Relaxed);
}

/// Re-admits parked packets whose frames have entered the admission window.
/// Extraction happens under the `pending_frames` lock; admission runs after it is
/// released — `admit_packet` takes slot locks and must not nest inside it.
#[allow(clippy::too_many_arguments)]
fn reinject_pending_frames(
    shared: &Arc<SharedData>,
    idx_func_ptr: Option<(tomii_types::CmPtr, &Vec<crate::graph_struct::Arg>)>,
    slots_dirty: &mut bool,
    active_packet_batch: &mut Vec<(NodeInfo, Option<CmTypes>)>,
    thread_core: usize,
    thread_id: usize,
    thread_slot: usize,
    frame_packets: usize,
) {
    let window_end = shared
        .telemetry
        .frame_complete_counter
        .load(Ordering::SeqCst)
        + shared.config.slots;
    let ready: Vec<(usize, Vec<PendingPacket>)> = {
        let mut pending = shared.net.pending_frames.lock();
        let ready_keys: Vec<usize> = pending.range(..window_end).map(|(k, _)| *k).collect();
        ready_keys
            .into_iter()
            .map(|k| {
                let packets = pending.remove(&k).unwrap();
                shared
                    .net
                    .pending_count
                    .fetch_sub(packets.len(), Ordering::Relaxed);
                (k, packets)
            })
            .collect()
    };
    for (frame, packets) in ready {
        print_debug(|| {
            format!(
                "Thread {:?} -- Re-injecting {} parked packets for frame {}",
                thread_id,
                packets.len(),
                frame
            )
        });
        for pp in packets {
            admit_packet(
                shared,
                frame,
                pp.packet,
                pp.timestamp,
                pp.receiver_core_id,
                idx_func_ptr,
                slots_dirty,
                active_packet_batch,
                thread_core,
                thread_id,
                thread_slot,
                frame_packets,
            );
        }
    }
}

/// Marks `frame` dropped exactly once: sets the drop bit (so later packets are
/// discarded on the fast path), advances the completion counter (so the admission
/// window keeps moving — the degrade-instead-of-hang invariant), and counts it.
pub(super) fn mark_frame_dropped(shared: &Arc<SharedData>, frame: usize, reason: &str) {
    if frame >= shared.net.frame_dropped.len() {
        return;
    }
    let already_marked = shared.net.frame_dropped[frame].swap(true, Ordering::AcqRel);
    if !already_marked {
        shared
            .telemetry
            .frame_complete_counter
            .fetch_add(1, Ordering::SeqCst);
        let dropped = shared.net.dropped_frames.fetch_add(1, Ordering::Relaxed) + 1;
        tracing::warn!(frame, total_dropped = dropped, reason, "frame dropped");
    }
}

/// Decodes raw packet bytes through `packet_process_func` and reclaims the
/// underlying buffer back to the originating receiver thread via its SPSC channel.
fn decode_packet(
    shared: &Arc<SharedData>,
    packet_msg: PacketMessage,
    packet_process_func: tomii_types::CmPtr,
    thread_slot: usize,
) -> CmTypes {
    let socket_id = packet_msg.socket_id;
    // Bytes variant avoids Arc/RwLock/Box overhead.
    // Keep received_bytes_cm alive (not moved) so we can reclaim its Vec<u8> below.
    let received_bytes_cm = CmTypes::from_bytes(packet_msg.packet_bytes);
    let start_proc = shared.telemetry.measure_start();
    let packet_cm = packet_process_func(std::slice::from_ref(&received_bytes_cm));
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

/// Outcome of trying to place a packet's frame on a slot.
enum SlotAssignment {
    /// Slot and per-slot index assigned; `NodeInfo` is ready for batch routing.
    Assigned(NodeInfo),
    /// Frame is admissible but every slot is occupied right now — a transient
    /// condition (e.g. a completed slot not yet released).  Caller parks the packet.
    NoSlotYet,
    /// Frame was already dropped — discard the packet.
    Dropped,
}

/// Assigns `new_frame` to an available slot, spawns initial nodes if the slot was
/// newly activated, and computes the packet's per-slot index.
fn assign_packet_to_slot(
    shared: &Arc<SharedData>,
    new_frame: usize,
    packet_cm: &CmTypes,
    idx_func_ptr: Option<(tomii_types::CmPtr, &Vec<crate::graph_struct::Arg>)>,
    slots_dirty: &mut bool,
    thread_core: usize,
    thread_slot: usize,
) -> SlotAssignment {
    // Fast-path: frame already dropped — discard without touching any shared state.
    if new_frame < shared.net.frame_dropped.len()
        && shared.net.frame_dropped[new_frame].load(Ordering::Acquire)
    {
        return SlotAssignment::Dropped;
    }

    let start_sa = shared.telemetry.measure_start();
    let (assigned_slot, newly_activated) = match assign_frame_to_available_slot(shared, new_frame) {
        Some(v) => v,
        None => return SlotAssignment::NoSlotYet,
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
                    "Slot {} newly activated (frame {}), spawning {} initial nodes",
                    assigned_slot,
                    new_frame,
                    init_nodes.len()
                )
            });
            let sctx = shared.sched_ctx();
            super::scheduling::dispatch_nodes(shared, &sctx, &init_nodes, thread_core, thread_slot);
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
        shared.slot_data.last_packet_ns[assigned_slot].store(
            shared.telemetry.base_instant.elapsed().as_nanos() as u64,
            Ordering::Relaxed,
        );
        idx_result
            .valid_number_to_usize()
            .expect("index_function must return usize")
    } else {
        shared.slot_data.last_packet_ns[assigned_slot].store(
            shared.telemetry.base_instant.elapsed().as_nanos() as u64,
            Ordering::Relaxed,
        );
        shared.slot_data.packet_counters[assigned_slot].fetch_add(1, Ordering::SeqCst)
    };

    SlotAssignment::Assigned(NodeInfo::new(0, assigned_slot, packet_index, 0))
}

/// Checks whether all expected packets for `slot` have been received.  On first
/// completion, increments the frames-received counter and signals receivers to
/// stop if all frames are done.
fn check_frame_completion(
    shared: &Arc<SharedData>,
    slot: usize,
    thread_id: usize,
    frame_packets: usize,
) {
    let packet_count = shared.slot_data.packet_counters[slot].load(Ordering::SeqCst);
    if packet_count != frame_packets {
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
            thread_id, frame_packets, slot, pending_tasks, pending_cond
        )
    });

    let completed_frames = shared
        .net
        .frames_receive_counter
        .fetch_add(1, Ordering::AcqRel)
        + 1;
    if completed_frames >= shared.config.max_frames {
        tracing::info!(
            frames = shared.config.max_frames,
            packets_per_frame = frame_packets,
            "all frames received, receivers will shut down"
        );
        shared.net.receive_finished.store(true, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// Loom model: packet-admission vs slot-promotion race (findings #28)
// ---------------------------------------------------------------------------
//
// Models the exact synchronisation used by `admit_packet`'s buffering path and
// `release_and_activate_next`'s promotion path (it does not call them — loom needs
// its own atomics/locks). If the two ever diverge from this model, update both.
//
// Shared objects mirror `SlotData`:
//   * `bitmap`  — `active_bitmap` (bit 0 = slot 0 active)
//   * `states`  — the `states` RwLock (only its read/write exclusion matters here)
//   * `buffer`  — the slot's entry in the `buffers` RwLock
//
// Promoter (release_and_activate_next): holds states.write() across the whole
// flip→drain, i.e. `bitmap |= 1` (Release) then drains `buffer` under buffers.write().
//
// Admitter (admit_packet buffering path): reads the bitmap; on "inactive" it takes
// states.read() and RE-CHECKS the bitmap before choosing buffer vs active-batch.
//
// Invariant: the single packet is accounted for exactly once — drained by the
// promoter or routed to the active batch by the admitter — and is never left in the
// buffer after the slot has gone active and been drained (that is the lost-packet
// stall). Removing the re-check (or the states.read()) makes loom find the losing
// interleaving.
#[cfg(loom)]
mod loom_tests {
    use loom::sync::atomic::{AtomicU64, Ordering};
    use loom::sync::{Arc, RwLock};

    const PACKET: u32 = 1;

    #[test]
    fn admit_vs_promote_never_loses_packet() {
        loom::model(|| {
            let bitmap = Arc::new(AtomicU64::new(0)); // slot 0 starts Buffering
            let states: Arc<RwLock<()>> = Arc::new(RwLock::new(()));
            let buffer: Arc<RwLock<Vec<u32>>> = Arc::new(RwLock::new(Vec::new()));
            // What the promoter drained (Some once it has run).
            let drained: Arc<RwLock<Option<Vec<u32>>>> = Arc::new(RwLock::new(None));
            // Whether the admitter routed the packet to the active batch.
            let routed_active: Arc<RwLock<bool>> = Arc::new(RwLock::new(false));

            // Promoter: flip → drain, all under states.write().
            let promoter = {
                let (bitmap, states, buffer, drained) = (
                    Arc::clone(&bitmap),
                    Arc::clone(&states),
                    Arc::clone(&buffer),
                    Arc::clone(&drained),
                );
                loom::thread::spawn(move || {
                    let _sg = states.write().unwrap();
                    bitmap.fetch_or(1, Ordering::Release);
                    let mut buf = buffer.write().unwrap();
                    let taken = std::mem::take(&mut *buf);
                    *drained.write().unwrap() = Some(taken);
                })
            };

            // Admitter: buffering-path routing with the under-lock re-check.
            let admitter = {
                let (bitmap, states, buffer, routed_active) = (
                    Arc::clone(&bitmap),
                    Arc::clone(&states),
                    Arc::clone(&buffer),
                    Arc::clone(&routed_active),
                );
                loom::thread::spawn(move || {
                    if bitmap.load(Ordering::Acquire) & 1 != 0 {
                        *routed_active.write().unwrap() = true; // lock-free fast path
                    } else {
                        let _sg = states.read().unwrap();
                        if bitmap.load(Ordering::Acquire) & 1 != 0 {
                            *routed_active.write().unwrap() = true;
                        } else {
                            buffer.write().unwrap().push(PACKET);
                        }
                    }
                })
            };

            promoter.join().unwrap();
            admitter.join().unwrap();

            // Accounting: the packet is either in the promoter's drained set, or was
            // routed active, or is still buffered *and will be drained later* — the
            // last case is only safe if the promoter has not already drained-empty.
            let drained = drained.read().unwrap().clone().unwrap_or_default();
            let routed_active = *routed_active.read().unwrap();
            let still_buffered = buffer.read().unwrap().contains(&PACKET);

            let drained_it = drained.contains(&PACKET);
            let accounted = drained_it || routed_active;

            // Exactly-once: never counted twice.
            let double = (drained_it && routed_active)
                || (drained_it && still_buffered)
                || (routed_active && still_buffered);
            assert!(!double, "packet double-counted: drained={drained_it} active={routed_active} buffered={still_buffered}");

            // The lost-packet stall: packet sits in the buffer while the slot is active
            // and the promoter already drained (took empty) — nothing will drain it again.
            let lost = still_buffered
                && (bitmap.load(Ordering::Acquire) & 1 != 0)
                && !drained_it;
            assert!(!lost, "packet stranded in buffer after promote+drain (findings #28)");

            // And overall it must be accounted for exactly once (unless still validly buffered
            // ahead of a promotion that has not drained it).
            assert!(
                accounted || still_buffered,
                "packet vanished: not drained, not routed active, not buffered"
            );
        });
    }
}
