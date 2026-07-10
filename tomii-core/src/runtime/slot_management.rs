//! Slot allocation primitives: assign, release, activate, and reinitialise concurrent frame slots.
//!
//! This module owns the low-level slot state machine transitions (`Inactive → Active`,
//! `Inactive → Buffering`, `Buffering → Active`, `Active → Inactive`) and the lock-ordering
//! protocol that prevents deadlocks between concurrent slot transitions.
//!
//! **Lock ordering**: all functions that hold both `running_frames` and `slot_states` must
//! acquire them in the order `running_frames` first, `slot_states` second.  Violating this
//! order causes deadlock (Bugs #11, #12 in project history).
//!
//! This module does **not** own the lifecycle orchestration logic (what to do *after* a slot
//! completes) — that lives in `slot_lifecycle`.

use super::shared_data::{SharedData, SlotState};
use crate::buffers::*;
use crate::debug::print_debug;
use crate::graph::Graph;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tomii_types::*;

/// Finalize a completed slot: record timing, increment the global completion counter, reinitialise
/// result buffers, and release the slot back to `Inactive`.
///
/// Returns `true` if the slot should be reused for a new frame (`can_restart`), `false` if
/// `max_frames` has been reached.  The caller is responsible for spawning new tasks when
/// `can_restart` is true.  `reinit_slot` is called **before** `release_slot` so that the new
/// frame cannot observe stale results (Bug #16 fix).
#[inline]
pub(super) fn process_slot_completion(shared: &Arc<SharedData>, slot: usize) -> bool {
    // Complete timing - use unwrap_or to handle errors gracefully
    shared.telemetry.with_timing(|tb| {
        if let Err(e) = tb.finish_slot_processing(slot) {
            tracing::warn!("Failed to finish slot {} timing: {}", slot, e);
        }
    });

    // Count currently active/processing frames (excluding this completing slot)
    let currently_active_frames = {
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
    let completed_frames = shared
        .telemetry
        .frame_complete_counter
        .fetch_add(1, Ordering::SeqCst)
        + 1;

    // Total frames in-flight or completed
    let total_frames_processed = completed_frames + currently_active_frames;

    // Decide whether to start a new frame on this slot
    let can_restart = total_frames_processed < shared.config.max_frames;

    if can_restart {
        tracing::info!(
            slot,
            completed = completed_frames,
            active = currently_active_frames,
            total = total_frames_processed,
            max = shared.config.max_frames,
            "slot completed frame, starting new"
        );

        // Clear completed nodes BEFORE releasing the slot.
        // reinit_slot must finish before release_slot makes the slot available for a new
        // frame assignment.  If release_slot ran first, assign_frame_to_available_slot
        // could pick up the Inactive slot, spawn initial tasks (storing results), and then
        // reinit_slot would clear those new-frame results → panic in legitimate tasks.
        shared.exec.node_results.reinit_slot(slot);

        // Release the slot (makes it available for next frame assignment)
        release_slot(shared, slot);

        true // Signal to caller: slot should restart
    } else {
        tracing::info!(
            slot,
            max = shared.config.max_frames,
            completed = completed_frames,
            active = currently_active_frames,
            "slot completed, max frames reached"
        );

        // Release the slot
        release_slot(shared, slot);

        false // Signal to caller: no restart needed
    }
}

/// Assign `frame` to an available slot and return `(slot_id, newly_activated)`.
///
/// Prefers the `last_assigned` slot (sequential assignment keeps slot 0, 1, 2, … in order so
/// `activate_next_slot` always finds the right slot on completion).  If that slot is busy,
/// searches round-robin for the next `Inactive` slot, which it marks `Buffering` instead of
/// `Active` (the slot will be promoted by `activate_next_slot` when its predecessor completes).
///
/// **Lock ordering**: acquires `running_frames` (write) first, then `slot_states` (write).
/// All other functions that hold both locks must follow the same order.
///
/// Returns `None` when every slot is occupied; callers must drop the frame gracefully.
#[inline]
pub(super) fn assign_frame_to_available_slot(
    shared: &Arc<SharedData>,
    frame: usize,
) -> Option<(usize, bool)> {
    // Get write access to have updated view of running frames
    let mut running_frames = shared.slot_data.running_frames.write();

    // Check if this frame is already mapped to a slot
    for (frame_id, slot_id) in running_frames.iter() {
        if *frame_id == frame {
            return Some((*slot_id, false)); // Already assigned, not newly activated
        }
    }

    let last_slot_assigned = shared.slot_data.last_assigned.load(Ordering::SeqCst);
    let mut slot_states = shared.slot_data.states.write();

    // Check last assigned first
    if slot_states[last_slot_assigned] == SlotState::Inactive {
        slot_states[last_slot_assigned] = SlotState::Active; // Mark slot as active immediately
        shared
            .slot_data
            .active_bitmap
            .fetch_or(1u64 << last_slot_assigned, Ordering::Release);
        shared.slot_data.needs_check[last_slot_assigned].store(true, Ordering::Release);
        running_frames.push((frame, last_slot_assigned));
        shared.slot_data.frame_id[last_slot_assigned].store(frame, Ordering::Relaxed);
        print_debug(|| {
            format!(
                "Assigned frame {} to slot {} (Inactive) -> Active (last assigned)",
                frame, last_slot_assigned
            )
        });
        drop(running_frames); // Release lock before returning

        // Bump slot generation for the new frame — lazily reinitialises all
        // NodeDependencyEntry, instances_sent, and cond_instances_to_spawn entries.
        // Done here (new-frame start, Inactive → Active) rather than in the slot
        // completion path so that old-frame in-flight tasks retain the old generation
        // and cannot spuriously spawn or corrupt the new frame's dependency counters.
        shared.slot_data.generation[last_slot_assigned].fetch_add(1, Ordering::SeqCst);

        // Start timing for the slot immediately upon assignment
        shared
            .telemetry
            .with_timing(|tb| tb.start_slot_processing(last_slot_assigned));

        return Some((last_slot_assigned, true)); // Newly activated from Inactive → Active
    }

    for i in 1..shared.config.slots {
        let slot_id = (last_slot_assigned + i) % shared.config.slots;
        let state = slot_states.get_mut(slot_id).unwrap();
        if *state == SlotState::Inactive {
            *state = SlotState::Buffering; // Mark slot as Buffering
            running_frames.push((frame, slot_id));
            shared.slot_data.frame_id[slot_id].store(frame, Ordering::Relaxed);
            shared
                .slot_data
                .last_assigned
                .store(slot_id, Ordering::SeqCst);
            print_debug(|| {
                format!(
                    "Assigned frame {} to slot {} (Inactive) -> Buffering",
                    frame, slot_id
                )
            });
            drop(running_frames); // Release lock before returning
                                  // In non-network mode, initial nodes are spawned immediately for
                                  // Buffering slots too (see initial_nodes call site). Without this
                                  // start_slot_processing call the timing controller panics when
                                  // finish_slot_processing is called at frame completion because it
                                  // never saw a StartSlotProcessing for this slot.
                                  // In network mode, activate_next_slot will call start_slot_processing
                                  // again (overwriting the start time) when the slot transitions to
                                  // Active — that is fine; the later timestamp is more accurate there.
            shared
                .telemetry
                .with_timing(|tb| tb.start_slot_processing(slot_id));
            return Some((slot_id, false)); // Assigned but Buffering, not Active
        }
    }

    // All slots are occupied — signal caller to drop this frame gracefully.
    None
}

pub(super) fn release_slot(shared: &Arc<SharedData>, slot: usize) {
    let mut running_frames = shared.slot_data.running_frames.write();
    let mut slot_states = shared.slot_data.states.write();

    let old_state = slot_states[slot];
    slot_states[slot] = SlotState::Inactive; // Mark as inactive
    shared
        .slot_data
        .active_bitmap
        .fetch_and(!(1u64 << slot), Ordering::Release);
    shared.slot_data.frame_id[slot].store(usize::MAX, Ordering::Relaxed);

    // Remove from running frames
    if let Some(pos) = running_frames.iter().position(|&(_, s_id)| s_id == slot) {
        let (frame_id, _) = running_frames.remove(pos);
        print_debug(|| {
            format!(
                "Released slot {} from frame {} (had state: {:?})",
                slot, frame_id, old_state
            )
        });
    } else {
        print_debug(|| {
            format!(
                "Released slot {} with no assigned frame (had state: {:?})",
                slot, old_state
            )
        });
    }
    drop(slot_states);
    drop(running_frames);
}

/// Activate the next buffering slot in round-robin order
/// Returns (activated_slot_id, buffered_nodes) for processing
/// When slot-priority is enabled, automatically uses round-robin activation
#[allow(clippy::type_complexity)]
pub(super) fn activate_next_slot(
    shared: &Arc<SharedData>,
    completing_slot: Option<usize>,
) -> Option<(usize, Vec<(NodeInfo, Option<CmTypes>)>)> {
    if !shared.config.slot_priority_enabled {
        return None;
    }

    // 1. Acquire running_frames (Read) FIRST
    let running_frames = shared.slot_data.running_frames.read();

    // 2. Then acquire slot_states (Write)
    let mut states = shared.slot_data.states.write();

    // Find and activate next buffering slot in round-robin order
    let activated_slot = if let Some(completed) = completing_slot {
        let mut found_slot = None;
        // We can safely iterate running_frames while holding the lock
        for (frame, slot) in running_frames.iter() {
            if states[*slot] == SlotState::Buffering {
                states[*slot] = SlotState::Active;
                shared
                    .slot_data
                    .active_bitmap
                    .fetch_or(1u64 << *slot, Ordering::Release);
                shared.slot_data.needs_check[*slot].store(true, Ordering::Release);
                shared
                    .slot_data
                    .last_assigned
                    .store(*slot, Ordering::SeqCst);
                print_debug(|| {
                    format!(
                        "Round-Robin: Activated slot {} for frame {} after completing slot {}",
                        slot, frame, completed
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
        drop(running_frames); // Release the first lock last

        // Bump slot generation for the new frame — lazily reinitialises all
        // NodeDependencyEntry, instances_sent, and cond_instances_to_spawn entries.
        // Done here (new-frame start, Buffering → Active) so that old-frame tasks
        // still in the batch_queue use the old generation and cannot corrupt the
        // new frame's dependency counters or cause spurious task spawning.
        shared.slot_data.generation[slot_id].fetch_add(1, Ordering::SeqCst);

        shared
            .telemetry
            .with_timing(|tb| tb.start_slot_processing(slot_id));

        Some((slot_id, buffered))
    } else {
        drop(states);
        drop(running_frames);
        None
    }
}

pub(super) fn initial_nodes(graph: &Graph, slots: Vec<usize>) -> Vec<NodeInfo> {
    let mut node_infos = Vec::new();
    for slot in slots {
        for node_id in &graph.initial_nodes {
            let node_factor = graph.nodes[*node_id as usize].factor;
            for index in 0..node_factor {
                node_infos.push(NodeInfo::new(*node_id, slot, index, 0));
            }
        }
    }
    node_infos
}
