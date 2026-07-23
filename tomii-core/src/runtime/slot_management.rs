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

        // Clear completed nodes BEFORE the caller releases the slot.
        // reinit_slot must finish before release makes the slot available for a
        // new frame assignment (Bug #16: releasing first lets
        // assign_frame_to_available_slot spawn new-frame tasks whose results
        // reinit would then clear).  The release itself happens in
        // release_and_activate_next, atomically with the next promotion.
        shared.exec.node_results.reinit_slot(slot);

        true // Signal to caller: slot should restart
    } else {
        tracing::info!(
            slot,
            max = shared.config.max_frames,
            completed = completed_frames,
            active = currently_active_frames,
            "slot completed, max frames reached"
        );

        false // Signal to caller: no restart needed (caller still releases)
    }
}

/// TOMII_SLOT_CHECK=1: samples how many slots are Active simultaneously.
/// Under --slot-priority the invariant is "at most one"; any sample above one
/// is a violation of the sequential round-robin handoff (diagnostic for the
/// release->promote TOCTOU race). Printed at shutdown via `dump_slot_check`.
pub(super) static SLOT_CHECK_SAMPLES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(super) static SLOT_CHECK_VIOLATIONS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(super) static SLOT_CHECK_MAX_ACTIVE: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[inline(always)]
pub(super) fn slot_check_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("TOMII_SLOT_CHECK").is_ok_and(|v| v == "1"))
}

#[inline(always)]
pub(super) fn slot_check_sample(shared: &SharedData) {
    if !slot_check_enabled() || !shared.config.slot_priority_enabled {
        return;
    }
    let active = shared
        .slot_data
        .active_bitmap
        .load(Ordering::Acquire)
        .count_ones() as u64;
    SLOT_CHECK_SAMPLES.fetch_add(1, Ordering::Relaxed);
    if active > 1 {
        SLOT_CHECK_VIOLATIONS.fetch_add(1, Ordering::Relaxed);
    }
    SLOT_CHECK_MAX_ACTIVE.fetch_max(active, Ordering::Relaxed);
}

/// Print the slot-concurrency diagnostic (call at shutdown).
pub fn dump_slot_check() {
    if slot_check_enabled() {
        eprintln!(
            "SLOT_CHECK: samples={} violations={} max_active={}",
            SLOT_CHECK_SAMPLES.load(Ordering::Relaxed),
            SLOT_CHECK_VIOLATIONS.load(Ordering::Relaxed),
            SLOT_CHECK_MAX_ACTIVE.load(Ordering::Relaxed),
        );
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

        slot_check_sample(shared);
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

/// Atomically release a completed/evicted slot and promote the next
/// `Buffering` slot — one linearizable transition under a single
/// `running_frames` + `slot_states` lock scope.
///
/// Splitting these into separate lock scopes (the old `release_slot` +
/// `activate_next_slot` pair) left a window where the released slot was
/// `Inactive` while `last_assigned` still pointed at it: a concurrently
/// admitting resolution thread could activate that slot for a brand-new frame
/// via the `assign_frame_to_available_slot` fast path, jumping the entire
/// buffering queue and running two DAGs at once under `--slot-priority`
/// (measured: ~1e-3 of TOMII_SLOT_CHECK samples at 16 slots / 2 resolution
/// threads). Holding both locks across release AND promotion closes it:
/// when the locks drop, either the buffering queue was empty (stealing the
/// free slot is then legitimate) or `last_assigned` points at the newly
/// promoted `Active` slot.
///
/// Returns `Some((promoted_slot, buffered_packets))` when a `Buffering` slot
/// was promoted (slot-priority mode only); the caller dispatches the batch.
///
/// **Lock ordering**: `running_frames` (write) → `slot_states` (write) →
/// `buffers` (write), consistent with the module protocol.
#[allow(clippy::type_complexity)]
pub(super) fn release_and_activate_next(
    shared: &Arc<SharedData>,
    slot: usize,
) -> Option<(usize, Vec<(NodeInfo, Option<CmTypes>)>)> {
    let mut running_frames = shared.slot_data.running_frames.write();
    let mut slot_states = shared.slot_data.states.write();

    // --- release `slot` (formerly release_slot) ---
    let old_state = slot_states[slot];
    slot_states[slot] = SlotState::Inactive;
    shared
        .slot_data
        .active_bitmap
        .fetch_and(!(1u64 << slot), Ordering::Release);
    shared.slot_data.frame_id[slot].store(usize::MAX, Ordering::Relaxed);
    if let Some(pos) = running_frames.iter().position(|&(_, s_id)| s_id == slot) {
        let (frame_id, _) = running_frames.remove(pos);
        print_debug(|| {
            format!(
                "Released slot {} from frame {} (had state: {:?})",
                slot, frame_id, old_state
            )
        });
    }

    if !shared.config.slot_priority_enabled {
        return None;
    }

    // --- promote the next Buffering slot, oldest admitted frame first ---
    let mut activated: Option<usize> = None;
    for (frame, s) in running_frames.iter() {
        if slot_states[*s] == SlotState::Buffering {
            slot_states[*s] = SlotState::Active;
            shared
                .slot_data
                .active_bitmap
                .fetch_or(1u64 << *s, Ordering::Release);
            shared.slot_data.needs_check[*s].store(true, Ordering::Release);
            shared.slot_data.last_assigned.store(*s, Ordering::SeqCst);
            print_debug(|| {
                format!(
                    "Round-Robin: Activated slot {} for frame {} after releasing slot {}",
                    s, frame, slot
                )
            });
            activated = Some(*s);
            break; // Activate only ONE slot per completion
        }
    }
    let slot_id = activated?;

    // Retrieve buffered packets while still holding both locks.
    let mut slot_buffers = shared.slot_data.buffers.write();
    let buffered = std::mem::take(&mut slot_buffers[slot_id]);
    drop(slot_buffers);
    drop(slot_states);
    drop(running_frames);

    // Bump slot generation for the new frame — lazily reinitialises all
    // NodeDependencyEntry, instances_sent, and cond_instances_to_spawn entries.
    // Done here (Buffering → Active) so old-frame tasks still in the batch
    // queue use the old generation and cannot corrupt the new frame's counters.
    shared.slot_data.generation[slot_id].fetch_add(1, Ordering::SeqCst);

    shared
        .telemetry
        .with_timing(|tb| tb.start_slot_processing(slot_id));

    Some((slot_id, buffered))
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
