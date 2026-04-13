use super::shared_data::{ExecCtx, SharedData, SlotData, SlotState};
use super::slot_management::{activate_next_slot, initial_nodes, process_slot_completion};
use crate::debug::print_debug;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

impl super::SynRt {
    /// Iterate all active slots and process any that have completed their stream.
    ///
    /// Called unconditionally every resolution-loop iteration to ensure completions are
    /// never missed (Bug #21 fix: conditional calling caused hangs when all threads went idle).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn check_slots(
        shared: &Arc<SharedData>,
        stream_slot_activity: &mut HashMap<usize, bool>,
        thread_id: usize,
        thread_core: usize,
        thread_slot: usize,
        cond_indexes: &[Vec<usize>],
        cached_slots: &mut Vec<usize>,
        slots_dirty: &mut bool,
    ) {
        // Refresh cached slot list only when dirty (stream assigned or completed).
        // Avoids acquiring running_streams.read() on every iteration in the hot path.
        if *slots_dirty || cached_slots.is_empty() {
            let running_streams = shared.slot_data.running_streams.read();
            cached_slots.clear();
            cached_slots.extend(running_streams.iter().map(|(_, slot)| *slot));
            *slots_dirty = false;
        }

        // Clear activity map AFTER getting slots to check (not before).
        // This prevents redundant checking while ensuring we don't miss completions.
        stream_slot_activity.clear();

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
            stream_slot_activity.remove(&proc_slot);
            *slots_dirty = true; // release_slot modified running_streams

            activate_buffered_slot(
                shared,
                proc_slot,
                cond_indexes,
                stream_slot_activity,
                thread_core,
                thread_id,
                thread_slot,
            );

            if can_restart && !shared.config.slot_priority_enabled {
                restart_slot_nonnetwork(shared, proc_slot, thread_core, thread_slot);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Private helpers — each handles one responsibility of the slot lifecycle
// ---------------------------------------------------------------------------

/// Check if a slot has truly completed its stream and claim exclusive ownership.
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

/// Reset all per-slot counters and flags for the next stream.
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

    // Unmark so the slot can complete again for the next stream.
    shared.exec.resolution_state.unmark_slot_completed(slot);

    print_debug(|| {
        format!(
            "Cleared all state for slot {} before spawning new stream",
            slot
        )
    });
}

/// In slot-priority mode: activate the next buffering slot, spawn its initial nodes,
/// and process any network packets that arrived while it was buffering.
fn activate_buffered_slot(
    shared: &Arc<SharedData>,
    completing_slot: usize,
    cond_indexes: &[Vec<usize>],
    stream_slot_activity: &mut HashMap<usize, bool>,
    thread_core: usize,
    thread_id: usize,
    thread_slot: usize,
) {
    if !shared.config.slot_priority_enabled {
        return;
    }

    let Some((activated_slot, mut buffered_batch)) =
        activate_next_slot(shared, Some(completing_slot))
    else {
        return;
    };

    print_debug(|| {
        format!(
            "Activated slot {} from Buffering to Active (completing slot: {})",
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
        super::SynRt::preparation(shared, &initial, thread_core, thread_slot);
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
        super::SynRt::process_batch_resolution(
            shared,
            &mut buffered_batch,
            thread_core,
            thread_id,
            thread_slot,
            cond_indexes,
            stream_slot_activity,
            start_ns,
        );
    }
}

/// In non-network mode: restart the completing slot in-place for the next stream.
///
/// `process_slot_completion` already released the slot (Inactive). This re-registers
/// it in `running_streams` and marks it Active so the new stream's completion can be
/// detected by `check_slots`.
///
/// Not called in network mode: the packet loop re-activates the slot via
/// `assign_stream_to_available_slot`, which handles gen bumps and initial spawning
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

    // Lock ordering: running_streams → slot_states (global protocol).
    {
        let mut running_streams = shared.slot_data.running_streams.write();
        let mut slot_states = shared.slot_data.states.write();

        // Count Active/Buffering slots (proc_slot is Inactive after release_slot, so excluded).
        let currently_active = slot_states
            .iter()
            .filter(|&&s| s == SlotState::Active || s == SlotState::Buffering)
            .count();
        let completed = shared
            .telemetry
            .stream_complete_counter
            .load(Ordering::Acquire);
        // Monotonically increasing stream ID: avoids conflicts with IDs assigned during init.
        let next_stream_id = completed + currently_active;

        slot_states[slot] = SlotState::Active;
        shared
            .slot_data
            .active_bitmap
            .fetch_or(1u64 << slot, Ordering::Release);
        shared.slot_data.stream_id[slot].store(next_stream_id, Ordering::Relaxed);
        running_streams.push((next_stream_id, slot));
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
        super::SynRt::preparation(shared, &compute_nodes, thread_core, thread_slot);
    }
}
