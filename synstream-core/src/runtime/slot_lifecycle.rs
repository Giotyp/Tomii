use super::shared_data::{SharedData, SlotState};
use super::slot_management::{activate_next_slot, initial_nodes, process_slot_completion};
use crate::debug::print_debug;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

impl super::SynRt {
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
        // Refresh the cached slot list only when dirty (stream assigned or completed).
        // Avoids acquiring running_streams.read() on every iteration in the hot path.
        if *slots_dirty || cached_slots.is_empty() {
            let running_streams = shared.slot_data.running_streams.read();
            cached_slots.clear();
            cached_slots.extend(running_streams.iter().map(|(_, slot)| *slot));
            *slots_dirty = false;
        }

        // Clear activity map AFTER getting slots to check (not before)
        // This prevents redundant checking while ensuring we don't miss completions
        stream_slot_activity.clear();

        // Load active bitmap once — avoids per-slot RwLock read.
        let active_bitmap = if shared.config.slot_priority_enabled {
            shared.slot_data.active_bitmap.load(Ordering::Acquire)
        } else {
            u64::MAX // all bits set — no filtering when slot_priority is off
        };

        for proc_slot in cached_slots.iter().copied() {
            // Skip buffering slots - they cannot complete until activated
            if active_bitmap & (1u64 << proc_slot) == 0 {
                continue;
            }

            // Skip if no task activity since last check.
            // Preserves Bug #21 fix: check_slots is still called unconditionally every
            // iteration; we only skip the expensive SeqCst loads for idle slots.
            if !shared.slot_data.needs_check[proc_slot].swap(false, Ordering::AcqRel) {
                continue;
            }

            // Check if all nodes in this slot have been processed
            let pending_regular = shared.slot_data.pending_tasks[proc_slot].load(Ordering::SeqCst);
            let pending_cond = shared.slot_data.pending_cond_tasks[proc_slot].load(Ordering::SeqCst);
            let processing_count = shared.slot_data.processing_count[proc_slot].load(Ordering::SeqCst);

            let all_nodes_processed =
                pending_regular == 0 && pending_cond == 0 && processing_count == 0;

            if all_nodes_processed {
                if !shared.exec.resolution_state.try_complete_slot(proc_slot) {
                    continue; // Another thread already owns this completion
                }

                // Double-check after winning the CAS: re-read counters with SeqCst to rule
                // out a stale win.
                let re_pending = shared.slot_data.pending_tasks[proc_slot].load(Ordering::SeqCst);
                let re_cond = shared.slot_data.pending_cond_tasks[proc_slot].load(Ordering::SeqCst);
                let re_proc = shared.slot_data.processing_count[proc_slot].load(Ordering::SeqCst);
                if re_pending != 0 || re_cond != 0 || re_proc != 0 {
                    // Stale win: another thread already completed and reset this slot.
                    shared.exec.resolution_state.unmark_slot_completed(proc_slot);
                    continue;
                }

                print_debug(|| {
                    format!(
                        "Thread {:?} -- Completed iteration at slot {}",
                        thread_id, proc_slot
                    )
                });

                // Bump slot generation IMMEDIATELY after confirming true completion,
                // BEFORE any counter resets.  This closes the window where stale tasks
                // (gen=G) could pass the batch_queue gen filter while counters have
                // already been reset to their initial values.  With the bump here,
                // stale completions see current_gen=G+1 != gen=G and are dropped.
                // Also invalidates old tasks still queued in Rayon (execute_task gen
                // check) before process_slot_completion clears predecessor results.
                shared.slot_data.generation[proc_slot].fetch_add(1, Ordering::SeqCst);

                // Reset packet completion flag for the next stream
                // Allows completion detection to work for the new iteration
                shared.slot_data.packet_complete[proc_slot].store(false, Ordering::SeqCst);

                // Reset per-slot packet counter for the next stream
                // This ensures the network node index starts at 0 for the new stream
                shared.slot_data.packet_counters[proc_slot].store(0, Ordering::SeqCst);

                shared.slot_data.pending_tasks[proc_slot].store(shared.graph_cache.total_tasks, Ordering::SeqCst);
                shared.slot_data.pending_cond_tasks[proc_slot].store(shared.graph_cache.total_cond_tasks, Ordering::SeqCst);
                shared.slot_data.needs_check[proc_slot].store(false, Ordering::SeqCst);

                print_debug(|| {
                    format!(
                        "RESET slot {} counters: slot_pending_tasks={}, slot_pending_cond_tasks={}",
                        proc_slot, shared.graph_cache.total_tasks, shared.graph_cache.total_cond_tasks
                    )
                });

                // Unmark slot completion flag so it can complete again for the next stream.
                shared.exec.resolution_state.unmark_slot_completed(proc_slot);

                print_debug(|| {
                    format!(
                        "Cleared all state for slot {} before spawning new stream",
                        proc_slot
                    )
                });

                // Check if we should start a new iteration and release the slot
                let can_restart = process_slot_completion(&shared, proc_slot);
                stream_slot_activity.remove(&proc_slot);
                *slots_dirty = true; // release_slot modified running_streams

                // In slot-priority mode: rotate active slot and activate next buffered slot
                let activated_slot_info = if shared.config.slot_priority_enabled {
                    activate_next_slot(&shared, Some(proc_slot))
                } else {
                    None
                };

                // Track whether we activated a buffering slot (for restart decision below)
                let _buffering_slot_was_activated = activated_slot_info.is_some();

                // Process activated slot: spawn initial nodes AND process buffered packets
                if let Some((activated_slot, mut buffered_batch)) = activated_slot_info {
                    print_debug(|| {
                        format!(
                            "Activated slot {} from Buffering to Active (completing slot: {})",
                            activated_slot, proc_slot
                        )
                    });

                    // Spawn initial compute nodes for the activated slot first
                    let activated_compute_nodes = initial_nodes(&shared, vec![activated_slot]);

                    print_debug(|| {
                        format!(
                            "Spawning {} initial nodes for activated slot {}",
                            activated_compute_nodes.len(),
                            activated_slot
                        )
                    });
                    if !activated_compute_nodes.is_empty() {
                        Self::preparation(
                            &shared,
                            &activated_compute_nodes,
                            thread_core,
                            thread_slot,
                        );
                    }

                    // Then process buffered network packets
                    // These are network packets that arrived while the slot was buffering
                    if !buffered_batch.is_empty() {
                        print_debug(|| {
                            format!(
                                "Processing {} buffered network packets for activated slot {}",
                                buffered_batch.len(),
                                activated_slot
                            )
                        });
                        let start_ns_batch = shared.telemetry.base_instant.elapsed().as_nanos();
                        Self::process_batch_resolution(
                            &shared,
                            &mut buffered_batch,
                            thread_core,
                            thread_id,
                            thread_slot,
                            &cond_indexes,
                            stream_slot_activity,
                            start_ns_batch,
                        );
                    }
                }

                let should_restart_completing_slot = can_restart && !shared.config.slot_priority_enabled;

                if should_restart_completing_slot {
                    // In network mode, do NOT spawn initial nodes or start timing here.
                    // The packet loop will re-activate this slot via
                    // assign_stream_to_available_slot (Inactive → Active), which handles
                    // the gen bump, timing start, and initial node spawning atomically.
                    // Spawning here would race with that path: tasks from this spawn
                    // (gen=G+1) partially execute and decrement counters before the
                    // activation gen bump (G+1→G+2) makes them stale. The activation
                    // path then provides a full set of decrements → underflow → hang.
                    if shared.graph.network_config().is_none() {
                        // Non-network mode: restart in-place (no packet-driven activation).
                        //
                        // Re-register slot in running_streams so check_slots can detect the
                        // new stream's completion. process_slot_completion called release_slot
                        // which removed proc_slot from running_streams and marked it Inactive.
                        // Without re-adding it, cached_slots never includes this slot and the
                        // new stream's completion is never detected → hang (Bug fix for
                        // EXP_STREAMS > SLOTS in non-network mode).
                        //
                        // Lock ordering: running_streams → slot_states (global protocol).
                        {
                            let mut running_streams = shared.slot_data.running_streams.write();
                            let mut slot_states = shared.slot_data.states.write();
                            // Count Active/Buffering slots (proc_slot is Inactive after
                            // release_slot, so it is already excluded from this count).
                            let currently_active = slot_states
                                .iter()
                                .filter(|&&s| {
                                    s == SlotState::Active || s == SlotState::Buffering
                                })
                                .count();
                            let completed =
                                shared.telemetry.stream_complete_counter.load(Ordering::Acquire);
                            // Unique stream ID: completed streams + in-flight streams gives
                            // the next monotonically increasing ID, avoiding conflicts with
                            // IDs already assigned during initialisation (0..slots).
                            let next_stream_id = completed + currently_active;
                            slot_states[proc_slot] = SlotState::Active;
                            shared.slot_data.active_bitmap.fetch_or(1u64 << proc_slot, Ordering::Release);
                            shared.slot_data.stream_id[proc_slot].store(next_stream_id, Ordering::Relaxed);
                            running_streams.push((next_stream_id, proc_slot));
                        }
                        // slots_dirty is already true (set after process_slot_completion)
                        // so the per-thread cached_slots will be refreshed next iteration.

                        if let Some(tb) = &shared.telemetry.time_buffer {
                            tb.start_slot_processing(proc_slot);
                        }

                        let compute_nodes = initial_nodes(&shared, vec![proc_slot]);

                        print_debug(|| {
                            format!(
                                "Spawned {} initial nodes for restarting slot {}",
                                compute_nodes.len(),
                                proc_slot
                            )
                        });

                        if !compute_nodes.is_empty() {
                            Self::preparation(&shared, &compute_nodes, thread_core, thread_slot);
                        }
                    }
                }
            }
        }
    }
}
