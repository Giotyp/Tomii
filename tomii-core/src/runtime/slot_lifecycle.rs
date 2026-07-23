//! Slot lifecycle orchestration: completion detection, state reset, and post-completion routing.
//!
//! [`check_slots`] is the top-level function called unconditionally on every resolution-loop
//! iteration.  It iterates all slots that have an assigned frame, skips buffering or idle
//! slots, and delegates to four private helpers:
//! - `detect_and_claim_slot_completion` — SeqCst counter check + CAS ownership claim.
//! - `reset_slot_state` — bumps generation and resets all per-slot counters/flags.
//! - `activate_buffered_slot` — in slot-priority mode, activates the next queued slot.
//! - `restart_slot_nonnetwork` — in non-network mode, re-registers the slot for a new frame.
//!
//! This module does **not** own the slot allocation primitives (`assign_frame_to_available_slot`,
//! `release_slot`, `activate_next_slot`) — those live in `slot_management`.  The split keeps
//! "what happens when a slot completes" separate from "how slots are allocated and released".

use super::shared_data::{ExecCtx, SharedData, SlotData, SlotState};
use super::slot_management::{initial_nodes, process_slot_completion};
use crate::debug::print_debug;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// Evict incomplete network frames whose slots have gone silent.
///
/// A frame that loses even one packet can never complete: its per-packet tasks
/// never fire, `packet_complete` never trips, and the slot wedges forever.
/// When `config.frame_timeout_ms > 0`, a slot that (a) has received some but
/// not all of its packets, (b) has no task in flight, and (c) has seen no new
/// packet for the timeout, is claimed via the same `packet_complete` CAS the
/// completion path uses, its frame counted through the dropped-frames path
/// (which also advances the admission window), and the slot recycled.
#[cfg(feature = "network")]
#[allow(clippy::too_many_arguments)]
fn try_evict_stalled_slots(
    shared: &Arc<SharedData>,
    cached_slots: &[usize],
    slots_dirty: &mut bool,
    cond_indexes: &[Vec<usize>],
    frame_slot_activity: &mut HashMap<usize, bool>,
    thread_core: usize,
    thread_id: usize,
    thread_slot: usize,
) {
    let timeout_ns = shared.config.frame_timeout_ms.saturating_mul(1_000_000);
    if timeout_ns == 0 {
        return;
    }
    let frame_packets = shared.net.frame_packets.load(Ordering::Relaxed);
    if frame_packets == 0 {
        return; // non-network run: nothing to evict
    }
    let now_ns = shared.telemetry.base_instant.elapsed().as_nanos() as u64;

    for &slot in cached_slots {
        let received = shared.slot_data.packet_counters[slot].load(Ordering::SeqCst);
        if received == 0 || received >= frame_packets {
            continue; // untouched (buffering) or complete: not eviction's business
        }
        let idle_since = shared.slot_data.last_packet_ns[slot].load(Ordering::Relaxed);
        if now_ns.saturating_sub(idle_since) < timeout_ns {
            continue;
        }
        // NOTE: pending_tasks stays nonzero on a wedged frame (it counts the
        // tasks that can never fire), so it must NOT gate eviction. A task
        // actually executing shows in processing_count; anything merely queued
        // would have run long before a multi-hundred-ms idle window elapsed.
        if shared.slot_data.processing_count[slot].load(Ordering::SeqCst) != 0 {
            continue; // work still in flight — not actually stalled
        }
        // Exclusive claim: the same flag the completion path swaps. A frame
        // missing packets cannot legitimately complete, so winning this CAS
        // makes this thread the slot's sole finisher.
        if shared.slot_data.packet_complete[slot].swap(true, Ordering::SeqCst) {
            continue;
        }
        let frame = shared.slot_data.frame_id[slot].load(Ordering::Relaxed);
        tracing::warn!(
            slot,
            frame,
            received,
            expected = frame_packets,
            timeout_ms = shared.config.frame_timeout_ms,
            "evicting incomplete frame (packet loss)"
        );
        // Counts the frame as dropped AND advances frame_complete_counter, so
        // shutdown accounting and the admission window both move on.
        super::packet_processing::mark_frame_dropped(
            shared,
            frame,
            "incomplete frame evicted (timeout)",
        );

        reset_slot_state(shared, slot);
        shared.exec.node_results.reinit_slot(slot);
        *slots_dirty = true;
        frame_slot_activity.remove(&slot);

        release_and_dispatch_next(
            shared,
            slot,
            cond_indexes,
            frame_slot_activity,
            thread_core,
            thread_id,
            thread_slot,
        );
    }
}

/// Iterate all active slots and process any that have completed their frame.
///
/// Called unconditionally every resolution-loop iteration to ensure completions are
/// never missed (Bug #21 fix: conditional calling caused hangs when all threads went idle).
#[allow(clippy::too_many_arguments)]
pub(super) fn check_slots(
    shared: &Arc<SharedData>,
    frame_slot_activity: &mut HashMap<usize, bool>,
    thread_id: usize,
    thread_core: usize,
    thread_slot: usize,
    cond_indexes: &[Vec<usize>],
    cached_slots: &mut Vec<usize>,
    slots_dirty: &mut bool,
) {
    // Refresh cached slot list only when dirty (frame assigned or completed).
    // Avoids acquiring running_frames.read() on every iteration in the hot path.
    if *slots_dirty || cached_slots.is_empty() {
        let running_frames = shared.slot_data.running_frames.read();
        cached_slots.clear();
        cached_slots.extend(running_frames.iter().map(|(_, slot)| *slot));
        *slots_dirty = false;
    }

    super::slot_management::slot_check_sample(shared);

    #[cfg(feature = "network")]
    try_evict_stalled_slots(
        shared,
        &cached_slots.clone(),
        slots_dirty,
        cond_indexes,
        frame_slot_activity,
        thread_core,
        thread_id,
        thread_slot,
    );

    // Clear activity map AFTER getting slots to check (not before).
    // This prevents redundant checking while ensuring we don't miss completions.
    frame_slot_activity.clear();

    // Load active bitmap once — avoids per-slot RwLock read.
    let active_bitmap = if shared.config.slot_priority_enabled {
        shared.slot_data.active_bitmap.load(Ordering::Acquire)
    } else {
        u64::MAX // all bits set — no filtering when slot_priority is off
    };

    for proc_slot in cached_slots.iter().copied() {
        // Skip buffering slots — they cannot complete until activated.
        if active_bitmap & (1u64 << proc_slot) == 0 {
            continue;
        }

        // Skip if no task activity since last check.
        // Preserves Bug #21 fix: check_slots is still called unconditionally every
        // iteration; we only skip the expensive SeqCst loads for idle slots.
        if !shared.slot_data.needs_check[proc_slot].swap(false, Ordering::AcqRel) {
            continue;
        }

        if !detect_and_claim_slot_completion(&shared.slot_data, &shared.exec, proc_slot) {
            continue;
        }

        print_debug(|| {
            format!(
                "Thread {:?} -- Completed iteration at slot {}",
                thread_id, proc_slot
            )
        });

        reset_slot_state(shared, proc_slot);

        let can_restart = process_slot_completion(shared, proc_slot);
        frame_slot_activity.remove(&proc_slot);
        *slots_dirty = true; // release modifies running_frames

        release_and_dispatch_next(
            shared,
            proc_slot,
            cond_indexes,
            frame_slot_activity,
            thread_core,
            thread_id,
            thread_slot,
        );

        if can_restart && !shared.config.slot_priority_enabled {
            restart_slot_nonnetwork(shared, proc_slot, thread_core, thread_slot);
        }
    }
}

// ---------------------------------------------------------------------------
// Private helpers — each handles one responsibility of the slot lifecycle
// ---------------------------------------------------------------------------

/// Check if a slot has truly completed its frame and claim exclusive ownership.
///
/// Loads the three counters with SeqCst, tries a CAS via `try_complete_slot`, then
/// re-reads to rule out a stale win. Returns `true` iff this thread now owns the
/// completion (counters confirmed zero, CAS succeeded, double-check passed).
fn detect_and_claim_slot_completion(slot_data: &SlotData, exec: &ExecCtx, slot: usize) -> bool {
    let pending_regular = slot_data.pending_tasks[slot].load(Ordering::SeqCst);
    let pending_cond = slot_data.pending_cond_tasks[slot].load(Ordering::SeqCst);
    let processing_count = slot_data.processing_count[slot].load(Ordering::SeqCst);

    if pending_regular != 0 || pending_cond != 0 || processing_count != 0 {
        return false;
    }

    if !exec.resolution_state.try_complete_slot(slot) {
        return false; // Another thread already owns this completion
    }

    // Double-check after winning the CAS: re-read counters with SeqCst to rule
    // out a stale win (another thread completed and reset this slot already).
    let re_pending = slot_data.pending_tasks[slot].load(Ordering::SeqCst);
    let re_cond = slot_data.pending_cond_tasks[slot].load(Ordering::SeqCst);
    let re_proc = slot_data.processing_count[slot].load(Ordering::SeqCst);
    if re_pending != 0 || re_cond != 0 || re_proc != 0 {
        exec.resolution_state.unmark_slot_completed(slot);
        return false;
    }

    true
}

/// Reset all per-slot counters and flags for the next frame.
///
/// Must be called immediately after `detect_and_claim_slot_completion` returns `true`,
/// before any other thread can observe the reset state. Bumps generation FIRST (before
/// counter resets) so that stale tasks still queued in Rayon see gen mismatch and are
/// dropped by `execute_task` before they could decrement the freshly-reset counters.
fn reset_slot_state(shared: &SharedData, slot: usize) {
    // Bump generation BEFORE counter resets — closes the window where stale tasks
    // (gen=G) could pass the batch_queue gen filter while counters are already reset.
    shared.slot_data.generation[slot].fetch_add(1, Ordering::SeqCst);

    shared.slot_data.packet_complete[slot].store(false, Ordering::SeqCst);
    shared.slot_data.packet_counters[slot].store(0, Ordering::SeqCst);
    shared.slot_data.pending_tasks[slot].store(shared.graph_cache.total_tasks, Ordering::SeqCst);
    shared.slot_data.pending_cond_tasks[slot]
        .store(shared.graph_cache.total_cond_tasks, Ordering::SeqCst);
    shared.slot_data.needs_check[slot].store(false, Ordering::SeqCst);

    print_debug(|| {
        format!(
            "RESET slot {} counters: slot_pending_tasks={}, slot_pending_cond_tasks={}",
            slot, shared.graph_cache.total_tasks, shared.graph_cache.total_cond_tasks
        )
    });

    // Unmark so the slot can complete again for the next frame.
    shared.exec.resolution_state.unmark_slot_completed(slot);

    print_debug(|| {
        format!(
            "Cleared all state for slot {} before spawning new frame",
            slot
        )
    });
}

/// Release `completing_slot` and — in slot-priority mode — atomically promote the
/// next buffering slot, then spawn its initial nodes and process any network
/// packets that arrived while it was buffering. Release and promotion happen in
/// one lock scope (`release_and_activate_next`) so a concurrent packet-admission
/// fast path can never steal the just-released slot ahead of the buffering queue.
fn release_and_dispatch_next(
    shared: &Arc<SharedData>,
    completing_slot: usize,
    cond_indexes: &[Vec<usize>],
    frame_slot_activity: &mut HashMap<usize, bool>,
    thread_core: usize,
    thread_id: usize,
    thread_slot: usize,
) {
    let Some((activated_slot, mut buffered_batch)) =
        super::slot_management::release_and_activate_next(shared, completing_slot)
    else {
        return; // released; nothing was buffering (or slot-priority disabled)
    };

    print_debug(|| {
        format!(
            "Activated slot {} from Buffering to Active (released slot: {})",
            activated_slot, completing_slot
        )
    });

    // Spawn initial compute nodes for the activated slot first
    let initial = initial_nodes(&shared.graph, vec![activated_slot]);
    print_debug(|| {
        format!(
            "Spawning {} initial nodes for activated slot {}",
            initial.len(),
            activated_slot
        )
    });
    if !initial.is_empty() {
        let sctx = shared.sched_ctx();
        super::scheduling::dispatch_nodes(shared, &sctx, &initial, thread_core, thread_slot);
    }

    // Process buffered network packets that arrived while the slot was buffering
    if !buffered_batch.is_empty() {
        print_debug(|| {
            format!(
                "Processing {} buffered network packets for activated slot {}",
                buffered_batch.len(),
                activated_slot
            )
        });
        let start_ns = shared.telemetry.base_instant.elapsed().as_nanos();
        // Route through the pluggable resolution strategy (the batch-protocol seam).
        let _ = shared.exec.resolution_strategy.drive_batch(
            shared,
            &mut buffered_batch,
            thread_core,
            thread_id,
            thread_slot,
            cond_indexes,
            frame_slot_activity,
            start_ns,
        );
    }
}

/// In non-network mode: restart the completing slot in-place for the next frame.
///
/// `process_slot_completion` already released the slot (Inactive). This re-registers
/// it in `running_frames` and marks it Active so the new frame's completion can be
/// detected by `check_slots`.
///
/// Not called in network mode: the packet loop re-activates the slot via
/// `assign_frame_to_available_slot`, which handles gen bumps and initial spawning
/// atomically. Spawning here would race with that path and cause counter underflow.
fn restart_slot_nonnetwork(
    shared: &Arc<SharedData>,
    slot: usize,
    thread_core: usize,
    thread_slot: usize,
) {
    if shared.graph.network_config().is_some() {
        return;
    }

    // Lock ordering: running_frames → slot_states (global protocol).
    {
        let mut running_frames = shared.slot_data.running_frames.write();
        let mut slot_states = shared.slot_data.states.write();

        // Count Active/Buffering slots (proc_slot is Inactive after release_slot, so excluded).
        let currently_active = slot_states
            .iter()
            .filter(|&&s| s == SlotState::Active || s == SlotState::Buffering)
            .count();
        let completed = shared
            .telemetry
            .frame_complete_counter
            .load(Ordering::Acquire);
        // Monotonically increasing frame ID: avoids conflicts with IDs assigned during init.
        let next_frame_id = completed + currently_active;

        slot_states[slot] = SlotState::Active;
        shared
            .slot_data
            .active_bitmap
            .fetch_or(1u64 << slot, Ordering::Release);
        shared.slot_data.frame_id[slot].store(next_frame_id, Ordering::Relaxed);
        running_frames.push((next_frame_id, slot));
    }
    // slots_dirty was already set by the caller after process_slot_completion.

    shared
        .telemetry
        .with_timing(|tb| tb.start_slot_processing(slot));

    let compute_nodes = initial_nodes(&shared.graph, vec![slot]);
    print_debug(|| {
        format!(
            "Spawned {} initial nodes for restarting slot {}",
            compute_nodes.len(),
            slot
        )
    });
    if !compute_nodes.is_empty() {
        let sctx = shared.sched_ctx();
        super::scheduling::dispatch_nodes(shared, &sctx, &compute_nodes, thread_core, thread_slot);
    }
}
