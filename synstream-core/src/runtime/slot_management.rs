use super::shared_data::{SharedData, SlotState};
use crate::buffers::*;
use crate::debug::print_debug;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use synstream_types::*;

#[inline]
pub(super) fn process_slot_completion(shared: &Arc<SharedData>, slot: usize) -> bool {
    // Complete timing - use unwrap_or to handle errors gracefully
    if let Some(tb) = &shared.telemetry.time_buffer {
        if let Err(e) = tb.finish_slot_processing(slot) {
            eprintln!("Warning: Failed to finish slot {} timing: {}", slot, e);
        }
    }

    // Count currently active/processing streams (excluding this completing slot)
    let currently_active_streams = {
        let slot_states = shared.slot_data.states.read();
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
        .telemetry.stream_complete_counter
        .fetch_add(1, Ordering::SeqCst)
        + 1;

    // Total streams in-flight or completed
    let total_streams_processed = completed_streams + currently_active_streams;

    // Decide whether to start a new stream on this slot
    let can_restart = total_streams_processed < shared.config.max_streams;

    if can_restart {
        println!(
                "SynRt -- Slot {} completed stream. Starting new: completed={}, active={}, total={}, max={}",
                slot,
                completed_streams,
                currently_active_streams,
                total_streams_processed,
                shared.config.max_streams
            );

        // Clear completed nodes BEFORE releasing the slot.
        // reinit_slot must finish before release_slot makes the slot available for a new
        // stream assignment.  If release_slot ran first, assign_stream_to_available_slot
        // could pick up the Inactive slot, spawn initial tasks (storing results), and then
        // reinit_slot would clear those new-stream results → panic in legitimate tasks.
        shared
            .exec.node_results
            .reinit_slot(&shared.graph.nodes, slot, None);

        // Release the slot (makes it available for next stream assignment)
        release_slot(shared, slot);

        true // Signal to caller: slot should restart
    } else {
        println!(
            "SynRt -- Slot {} completed. Max streams ({}) reached: completed={}, active={}",
            slot, shared.config.max_streams, completed_streams, currently_active_streams
        );

        // Release the slot
        release_slot(shared, slot);

        false // Signal to caller: no restart needed
    }
}

#[inline]
/// Returns `Some((slot, newly_activated))` on success, or `None` when all slots are occupied.
/// Callers must handle `None` gracefully (drop the packet) instead of panicking.
pub(super) fn assign_stream_to_available_slot(
    shared: &Arc<SharedData>,
    stream: usize,
) -> Option<(usize, bool)> {
    // Get write access to have updated view of running streams
    let mut running_streams = shared.slot_data.running_streams.write();

    // Check if this stream is already mapped to a slot
    for (stream_id, slot_id) in running_streams.iter() {
        if *stream_id == stream {
            return Some((*slot_id, false)); // Already assigned, not newly activated
        }
    }

    let last_slot_assigned = shared.slot_data.last_assigned.load(Ordering::SeqCst);
    let mut slot_states = shared.slot_data.states.write();

    // Check last assigned first
    if slot_states[last_slot_assigned] == SlotState::Inactive {
        slot_states[last_slot_assigned] = SlotState::Active; // Mark slot as active immediately
        shared.slot_data.active_bitmap.fetch_or(1u64 << last_slot_assigned, Ordering::Release);
        shared.slot_data.needs_check[last_slot_assigned].store(true, Ordering::Release);
        running_streams.push((stream, last_slot_assigned));
        shared.slot_data.stream_id[last_slot_assigned].store(stream, Ordering::Relaxed);
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
        shared.slot_data.generation[last_slot_assigned].fetch_add(1, Ordering::SeqCst);

        // Start timing for the slot immediately upon assignment
        if let Some(tb) = &shared.telemetry.time_buffer {
            tb.start_slot_processing(last_slot_assigned);
        }

        return Some((last_slot_assigned, true)); // Newly activated from Inactive → Active
    }

    for i in 1..shared.config.slots {
        let slot_id = (last_slot_assigned + i) % shared.config.slots;
        let state = slot_states.get_mut(slot_id).unwrap();
        if *state == SlotState::Inactive {
            *state = SlotState::Buffering; // Mark slot as Buffering
            running_streams.push((stream, slot_id));
            shared.slot_data.stream_id[slot_id].store(stream, Ordering::Relaxed);
            shared.slot_data.last_assigned.store(slot_id, Ordering::SeqCst);
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
            if let Some(tb) = &shared.telemetry.time_buffer {
                tb.start_slot_processing(slot_id);
            }
            return Some((slot_id, false)); // Assigned but Buffering, not Active
        }
    }

    // All slots are occupied — signal caller to drop this frame gracefully.
    None
}

pub(super) fn release_slot(shared: &Arc<SharedData>, slot: usize) {
    let mut running_streams = shared.slot_data.running_streams.write();
    let mut slot_states = shared.slot_data.states.write();

    let old_state = slot_states[slot];
    slot_states[slot] = SlotState::Inactive; // Mark as inactive
    shared.slot_data.active_bitmap.fetch_and(!(1u64 << slot), Ordering::Release);
    shared.slot_data.stream_id[slot].store(usize::MAX, Ordering::Relaxed);

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


/// Activate the next buffering slot in round-robin order
/// Returns (activated_slot_id, buffered_nodes) for processing
/// When slot-priority is enabled, automatically uses round-robin activation
pub(super) fn activate_next_slot(
    shared: &Arc<SharedData>,
    completing_slot: Option<usize>,
) -> Option<(usize, Vec<(NodeInfo, Option<CmTypes>)>)> {
    if !shared.config.slot_priority_enabled {
        return None;
    }

    // 1. Acquire running_streams (Read) FIRST
    let running_streams = shared.slot_data.running_streams.read();

    // 2. Then acquire slot_states (Write)
    let mut states = shared.slot_data.states.write();

    // Find and activate next buffering slot in round-robin order
    let activated_slot = if let Some(completed) = completing_slot {
        let mut found_slot = None;
        // We can safely iterate running_streams while holding the lock
        for (stream, slot) in running_streams.iter() {
            if states[*slot] == SlotState::Buffering {
                states[*slot] = SlotState::Active;
                shared.slot_data.active_bitmap.fetch_or(1u64 << *slot, Ordering::Release);
                shared.slot_data.needs_check[*slot].store(true, Ordering::Release);
                shared.slot_data.last_assigned.store(*slot, Ordering::SeqCst);
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
        let mut slot_buffers = shared.slot_data.buffers.write();
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
        shared.slot_data.generation[slot_id].fetch_add(1, Ordering::SeqCst);

        if let Some(tb) = &shared.telemetry.time_buffer {
            tb.start_slot_processing(slot_id);
        }

        Some((slot_id, buffered))
    } else {
        drop(states);
        drop(running_streams);
        None
    }
}

pub(super) fn initial_nodes(shared: &Arc<SharedData>, slots: Vec<usize>) -> Vec<NodeInfo> {
    let mut node_infos = Vec::new();
    for slot in slots {
        let initial_nodes = &shared.graph.initial_nodes;
        for node_id in initial_nodes {
            let node = &shared.graph.nodes[*node_id as usize];
            let node_factor = node.factor;
            let indexes: Vec<usize> = (0..node_factor).collect();
            for index in indexes {
                node_infos.push(NodeInfo::new(*node_id, slot, index, 0));
            }
        }
    }
    node_infos
}
