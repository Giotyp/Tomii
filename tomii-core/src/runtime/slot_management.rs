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

use super::shared_data::{SharedData, SlotState, FRAME_SLOT_EMPTY};
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

    super::phase_prof::trace_admit(
        6,
        shared.slot_data.frame_id[slot].load(Ordering::Relaxed),
        slot as u64,
        0,
    );
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

/// Lock-free lookup of the slot a `frame` is already assigned to, via the
/// `frame_slot_map` cache. Returns `None` on a vacant cell or a frame-tag
/// mismatch (stale aliased entry) — the caller then takes the write-locked slow
/// path.
#[inline]
fn frame_slot_lookup(shared: &Arc<SharedData>, frame: usize) -> Option<usize> {
    let map = &shared.slot_data.frame_slot_map;
    let packed = map[frame & (map.len() - 1)].load(Ordering::Acquire);
    if packed != FRAME_SLOT_EMPTY && gen_unpack_gen(packed) == frame as u32 {
        Some(gen_unpack_val(packed) as usize)
    } else {
        None
    }
}

/// Publish a `frame -> slot` mapping into the lock-free cache. MUST be called
/// while holding `running_frames.write()`, right after the `running_frames.push`,
/// so the cache cannot diverge from `running_frames`.
#[inline]
pub(super) fn frame_slot_publish(shared: &Arc<SharedData>, frame: usize, slot: usize) {
    let map = &shared.slot_data.frame_slot_map;
    map[frame & (map.len() - 1)].store(gen_pack(frame as u32, slot as u32), Ordering::Release);
}

/// Clear a `frame`'s cache cell when the frame leaves `running_frames`. MUST be
/// called under `running_frames.write()`. Clears only if the cell still holds
/// THIS frame: a later frame aliasing the same index would already have
/// overwritten it (possible only once `frame` is gone, since `len >= 2*slots`
/// keeps all live frames in distinct cells).
#[inline]
fn frame_slot_clear(shared: &Arc<SharedData>, frame: usize) {
    let map = &shared.slot_data.frame_slot_map;
    let idx = frame & (map.len() - 1);
    let packed = map[idx].load(Ordering::Acquire);
    if packed != FRAME_SLOT_EMPTY && gen_unpack_gen(packed) == frame as u32 {
        map[idx].store(FRAME_SLOT_EMPTY, Ordering::Release);
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
    // Lock-free already-assigned fast path. The common case by far: every packet
    // after a frame's first finds the frame already mapped to a slot. Serve it
    // without the per-packet `running_frames.write()` + O(slots) scan this
    // function used to take unconditionally — that write lock was the serial
    // bottleneck throttling admission on a single resolution thread.
    if let Some(slot_id) = frame_slot_lookup(shared, frame) {
        return Some((slot_id, false)); // Already assigned, not newly activated
    }

    // Slow path (cache miss): take the write lock and RE-SCAN `running_frames`.
    // This re-scan is the authoritative first-packet-wins check — two threads
    // racing a frame's FIRST packet both miss the lock-free cache, serialise
    // here, and the loser finds the frame already pushed by the winner, so only
    // one slot is ever activated per frame.
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
        frame_slot_publish(shared, frame, last_slot_assigned);
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
        bump_slot_generation(shared, last_slot_assigned);

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
            if !shared.config.slot_priority_enabled && shared.graph.network_config().is_some() {
                // Parallel mode (slot-priority off — the default) on a NETWORK graph:
                // activate the slot immediately so this frame runs concurrently with the
                // others, exactly like the `last_assigned` fast path above. The Buffering
                // state exists only for slot-priority's sequential promotion machinery
                // (release_and_activate_next), which never runs when slot-priority is off
                // — so a network slot parked as Buffering here would strand its packets
                // forever: admit_packet routes them by the real active_bitmap (which stays
                // clear for a Buffering slot), they land in the slot buffer, and nothing
                // ever drains it. That silently dropped every frame after the first on a
                // multi-slot network run (findings #29).
                //
                // Non-network parallel graphs are deliberately left on the Buffering path
                // below: there the initial nodes are spawned eagerly by the caller and
                // check_slots polls every slot (active_bitmap is treated as all-ones when
                // slot-priority is off), so those slots complete without promotion.
                *state = SlotState::Active;
                shared
                    .slot_data
                    .active_bitmap
                    .fetch_or(1u64 << slot_id, Ordering::Release);
                shared.slot_data.needs_check[slot_id].store(true, Ordering::Release);
                running_frames.push((frame, slot_id));
                frame_slot_publish(shared, frame, slot_id);
                shared.slot_data.frame_id[slot_id].store(frame, Ordering::Relaxed);
                shared
                    .slot_data
                    .last_assigned
                    .store(slot_id, Ordering::SeqCst);
                print_debug(|| {
                    format!(
                        "Assigned frame {} to slot {} (Inactive) -> Active (parallel)",
                        frame, slot_id
                    )
                });
                drop(running_frames); // Release lock before returning

                // Bump generation for the new frame (see the last_assigned branch).
                bump_slot_generation(shared, slot_id);
                shared
                    .telemetry
                    .with_timing(|tb| tb.start_slot_processing(slot_id));
                slot_check_sample(shared);
                // Newly activated: the caller spawns this slot's initial nodes.
                return Some((slot_id, true));
            }

            *state = SlotState::Buffering; // Mark slot as Buffering
            running_frames.push((frame, slot_id));
            frame_slot_publish(shared, frame, slot_id);
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
        frame_slot_clear(shared, frame_id);
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
    bump_slot_generation(shared, slot_id);

    shared
        .telemetry
        .with_timing(|tb| tb.start_slot_processing(slot_id));

    Some((slot_id, buffered))
}

/// Build the task descriptors that seed a slot: every initial (root) node's instances.
///
/// Each root's `factor` instances are CHUNKED into up to `workers` contiguous bulk
/// tasks instead of one task per instance. The resolution thread dispatches these
/// serially, so shrinking a wide root's dispatch from `factor` to `min(factor,
/// workers)` tasks removes the per-frame root-dispatch ceiling (a wide root spent
/// ~1 µs/instance in `send_to_scheduler`), and the chunks execute in parallel across
/// workers. Roots have no `$res` inputs, so bulk execution reads nothing per cell;
/// `execute_bulk_task` still stores per-instance results for successors. A root with
/// `factor <= workers` yields one task per instance (bulk_count 1), i.e. unchanged.
pub(super) fn initial_nodes(graph: &Graph, slots: Vec<usize>, workers: usize) -> Vec<NodeInfo> {
    let mut node_infos = Vec::new();
    for slot in slots {
        for node_id in &graph.initial_nodes {
            let factor = graph.nodes[*node_id as usize].factor;
            if factor == 0 {
                continue;
            }
            let n_chunks = workers.max(1).min(factor);
            let base = factor / n_chunks;
            let extra = factor % n_chunks;
            let mut start = 0usize;
            for c in 0..n_chunks {
                let count = base + usize::from(c < extra);
                if count == 0 {
                    continue;
                }
                let mut ni = NodeInfo::new(*node_id, slot, start, 0);
                ni.bulk_count = count;
                node_infos.push(ni);
                start += count;
            }
        }
    }
    node_infos
}

// ---------------------------------------------------------------------------
// Centralised slot-generation bump helper
// ---------------------------------------------------------------------------
//
// The per-slot generation counter (`shared.slot_data.generation`) packs into the
// upper 32 bits of every generational `AtomicU64` counter (see
// `crate::buffers::{gen_pack, gen_unpack_gen, gen_unpack_val}`), while the counter
// itself is a full `u64`. Reads of the packed tag truncate the generation to its
// low 32 bits, so the effective generation space is 2^32: a per-slot entry that
// goes untouched for a full 2^32-generation span (reachable for conditional/sparse
// graphs over a long-running process) keeps a stale tag that would false-match
// once the low 32 bits wrap back to that value.
//
// `bump_slot_generation` is the single bump site all slot-reset/activation call
// sites route through. It performs the ordinary `fetch_add` on every call and,
// only on the rare low-32-bit wrap, eagerly sweeps every generational entry for
// that slot so none can carry a stale tag into the new epoch. The common
// per-frame path is untouched — it stays 100% lazy.

/// Bump `shared.slot_data.generation[slot]` by one.
///
/// This must be called from the same exclusive slot-reset/activation window as
/// the rest of slot reinitialisation (before any other thread can observe the
/// new frame), exactly as the raw `fetch_add` it replaces.
///
/// On the once-per-2^32 wrap of the low 32 bits, this eagerly reinitialises every
/// generational dependency entry for `slot` to the new (wrapped) generation so
/// that no entry can carry a stale tag from the previous 2^32 epoch forward.
pub(crate) fn bump_slot_generation(shared: &SharedData, slot: usize) {
    let prev = shared.slot_data.generation[slot].fetch_add(1, Ordering::SeqCst);
    if prev as u32 == u32::MAX {
        // Low 32 bits just wrapped: the new low-32 generation is 0.
        let new_gen = prev.wrapping_add(1) as u32;
        sweep_slot_generational_entries(shared, slot, new_gen);
    }
}

/// Eagerly reinitialise every generational per-slot entry to `new_gen`.
///
/// Covers all three gen-tagged per-slot structures: `NodeDepMap` (via
/// `DependencyCounter::reset_slot_epoch`), `cond_instances_to_spawn`, and
/// `fanout_bulk_arrived`. Only reachable from [`bump_slot_generation`] at the
/// 2^32 wrap boundary.
fn sweep_slot_generational_entries(shared: &SharedData, slot: usize, new_gen: u32) {
    shared.exec.resolution_state.reset_slot_epoch(slot, new_gen);

    // `cond_instances_to_spawn[slot]` and `fanout_bulk_arrived[slot]` are aligned
    // 1:1 with `node_cache` (one entry per node, same order; see
    // `runtime::init::build_slot_counters`). Iterate by the shorter of the two to
    // stay in-bounds even if that invariant is ever violated.
    let cond_slot = &shared.slot_data.cond_instances_to_spawn[slot];
    let fanout_slot = &shared.slot_data.fanout_bulk_arrived[slot];
    let node_cache = &shared.graph_cache.node_cache;
    let n = node_cache.len().min(cond_slot.len()).min(fanout_slot.len());

    for (node_id, nc) in node_cache.iter().enumerate().take(n) {
        let cond_init = if nc.is_condition { nc.factor as u32 } else { 0 };
        cond_slot[node_id].store(
            crate::buffers::gen_pack(new_gen, cond_init),
            Ordering::SeqCst,
        );
        fanout_slot[node_id].store(crate::buffers::gen_pack(new_gen, 0), Ordering::SeqCst);
    }
}

#[cfg(test)]
mod generation_tests {
    use super::*;
    use crate::graph_gen::from_json_str;
    use crate::runtime::{RuntimeConfig, TomiiRt, TomiiRtBuilder};
    use crate::scheduler::{create_scheduler, SchedulerConfig, SchedulerType};

    /// a1, a2 (roots, factor 1) -> b (factor 1), b has two non-barrier `$res`
    /// deps so `dependency_count_vec()` gives b a `deps_per_group` of 2 — enough
    /// to distinguish "freshly initialised" (2 remaining) from "one decrement
    /// already applied" (1 remaining) in the assertions below.
    const TWO_PRED_GRAPH: &str = r#"
    {
        "nodes": [
            { "name": "a1", "function": "noop", "args": [] },
            { "name": "a2", "function": "noop", "args": [] },
            {
                "name": "b",
                "function": "noop",
                "args": [
                    { "type": "$res", "predecessor": { "name": "a1", "indexes": "0" } },
                    { "type": "$res", "predecessor": { "name": "a2", "indexes": "0" } }
                ]
            }
        ]
    }
    "#;

    /// Node id of `b` in [`TWO_PRED_GRAPH`] (ids are assigned in declaration order).
    const B_NODE_ID: usize = 2;

    /// Build a single-slot runtime for [`TWO_PRED_GRAPH`] without running it —
    /// tests poke `shared` state directly rather than driving execution.
    fn build_test_rt() -> TomiiRt {
        let spec = from_json_str(TWO_PRED_GRAPH, 2).expect("JSON parse failed");
        let scheduler = create_scheduler(SchedulerConfig {
            scheduler_type: SchedulerType::WorkStealing,
            core_offset: 0,
            num_workers: 2,
            record: false,
            external_recorder: None,
            base_instant: std::time::Instant::now(),
            system_threads: 1,
            receiver_threads: 0,
            target_batch_size: 1,
            batch_timeout_us: 10,
            worker_affinity: None,
            worker_hook: None,
        });
        let compiled = spec.compile(&scheduler);
        let config = RuntimeConfig {
            slots: 1,
            max_frames: 1,
            max_runtime: Some(5),
            system_threads: 1,
            workers: 2,
            ..RuntimeConfig::default()
        };
        TomiiRtBuilder::with_config(compiled, scheduler, config)
            .build()
            .expect("build failed")
    }

    #[test]
    fn test_bump_slot_generation_wrap_sweeps_stale_entries() {
        let rt = build_test_rt();
        let shared = &rt.shared;
        let slot = 0;

        // Simulate an entry last touched one decrement ago, tagged with the
        // (low-32) generation the upcoming wrap will produce (0) — exactly the
        // collision the bug describes. Without the eager sweep this stale tag
        // would false-match the new epoch and the node would need only 1 more
        // decrement instead of a fresh 2.
        let mut ready = Vec::new();
        shared
            .exec
            .resolution_state
            .decrease_and_get_ready_into(slot, B_NODE_ID, 0, None, 1, None, &mut ready);
        assert!(ready.is_empty(), "1 of 2 deps consumed, not ready yet");

        // Poison cond_instances_to_spawn / fanout_bulk_arrived with a stale tag
        // at the same colliding generation (0) and an unmistakably non-init value.
        shared.slot_data.cond_instances_to_spawn[slot][B_NODE_ID]
            .store(crate::buffers::gen_pack(0, 999), Ordering::SeqCst);
        shared.slot_data.fanout_bulk_arrived[slot][B_NODE_ID]
            .store(crate::buffers::gen_pack(0, 888), Ordering::SeqCst);

        // Force the counter to the wrap boundary: the next fetch_add's `prev as
        // u32` is `u32::MAX`, so the post-increment low 32 bits become 0 —
        // colliding with the stale tags above.
        shared.slot_data.generation[slot].store(u64::from(u32::MAX), Ordering::SeqCst);

        bump_slot_generation(shared, slot);

        // cond_instances_to_spawn / fanout_bulk_arrived were eagerly reinitialised
        // to the fresh init value at the new generation, not left at the poisoned
        // stale value.
        let cond_packed =
            shared.slot_data.cond_instances_to_spawn[slot][B_NODE_ID].load(Ordering::SeqCst);
        assert_eq!(crate::buffers::gen_unpack_gen(cond_packed), 0);
        assert_eq!(crate::buffers::gen_unpack_val(cond_packed), 0); // b is not a condition node
        let fanout_packed =
            shared.slot_data.fanout_bulk_arrived[slot][B_NODE_ID].load(Ordering::SeqCst);
        assert_eq!(crate::buffers::gen_unpack_gen(fanout_packed), 0);
        assert_eq!(crate::buffers::gen_unpack_val(fanout_packed), 0);

        // resolution_state: the pre-wrap decrement (1 of 2 deps) must NOT carry
        // forward into the new epoch. A decrement at the new generation must
        // require the FULL 2 deps again, proving the sweep eagerly reinitialised
        // NodeDepMap rather than the stale tag lazily (and wrongly) matching.
        let mut ready2 = Vec::new();
        shared.exec.resolution_state.decrease_and_get_ready_into(
            slot,
            B_NODE_ID,
            0,
            None,
            1,
            None,
            &mut ready2,
        );
        assert!(
            ready2.is_empty(),
            "sweep must reset remaining_deps to the full count at the new generation"
        );
        let mut ready3 = Vec::new();
        shared.exec.resolution_state.decrease_and_get_ready_into(
            slot,
            B_NODE_ID,
            0,
            None,
            1,
            None,
            &mut ready3,
        );
        assert_eq!(
            ready3,
            vec![0],
            "second decrement at the new gen completes the freshly-reset count"
        );
    }

    #[test]
    fn test_bump_slot_generation_non_wrap_does_not_sweep() {
        let rt = build_test_rt();
        let shared = &rt.shared;
        let slot = 0;

        // Non-wrap starting point: low 32 bits != u32::MAX.
        shared.slot_data.generation[slot].store(5, Ordering::SeqCst);

        // Poison the swept-on-wrap structures with sentinel values that would
        // only change if a sweep (incorrectly) ran on this non-wrap bump.
        shared.slot_data.cond_instances_to_spawn[slot][B_NODE_ID]
            .store(crate::buffers::gen_pack(123, 456), Ordering::SeqCst);
        shared.slot_data.fanout_bulk_arrived[slot][B_NODE_ID]
            .store(crate::buffers::gen_pack(123, 456), Ordering::SeqCst);

        // Pre-set resolution_state away from its init value at an arbitrary gen
        // tag (123): 1 of 2 deps consumed.
        let mut ready = Vec::new();
        shared
            .exec
            .resolution_state
            .decrease_and_get_ready_into(slot, B_NODE_ID, 123, None, 1, None, &mut ready);
        assert!(ready.is_empty());

        bump_slot_generation(shared, slot); // 5 -> 6, non-wrap

        assert_eq!(shared.slot_data.generation[slot].load(Ordering::SeqCst), 6);
        assert_eq!(
            shared.slot_data.cond_instances_to_spawn[slot][B_NODE_ID].load(Ordering::SeqCst),
            crate::buffers::gen_pack(123, 456),
            "non-wrap bump must not touch cond_instances_to_spawn"
        );
        assert_eq!(
            shared.slot_data.fanout_bulk_arrived[slot][B_NODE_ID].load(Ordering::SeqCst),
            crate::buffers::gen_pack(123, 456),
            "non-wrap bump must not touch fanout_bulk_arrived"
        );

        // resolution_state: the pending 1-of-2 decrement from before the bump
        // must still need only 1 more decrement (not a fresh 2) — proving the
        // non-wrap bump did not sweep NodeDepMap either.
        let mut ready2 = Vec::new();
        shared.exec.resolution_state.decrease_and_get_ready_into(
            slot,
            B_NODE_ID,
            123,
            None,
            1,
            None,
            &mut ready2,
        );
        assert_eq!(
            ready2,
            vec![0],
            "non-wrap bump must leave the in-flight decrement count untouched"
        );
    }
}

// ---------------------------------------------------------------------------
// Loom model: the `frame_slot_map` lock-free fast path vs. activation (QW2).
// ---------------------------------------------------------------------------
//
// `assign_frame_to_available_slot` serves already-assigned frames from the
// lock-free `frame_slot_map` cell and takes `running_frames.write()` only on a
// cache miss. The invariant QW2 must preserve is FIRST-PACKET-WINS: when a
// frame's first packet is admitted concurrently by two resolution threads, both
// miss the cache, yet exactly ONE slot is ever activated for that frame.
//
// This model reconstructs that path with loom primitives — cell load (Acquire)
// -> miss -> running_frames.write() -> authoritative re-scan -> activate + push
// + cell.store (Release) — mirroring `frame_slot_lookup`/`frame_slot_publish`
// and the slow-path re-scan. loom explores every interleaving, including the
// one where a thread fully activates and publishes before the other loads the
// cell (the latter must then observe the published slot via the fast path and
// NOT re-activate).
#[cfg(loom)]
mod loom_frame_slot {
    use loom::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use loom::sync::{Arc, RwLock};

    const EMPTY: u64 = u64::MAX;
    const FRAME: u64 = 1;
    const SLOT: u64 = 0;

    fn pack(frame: u64, slot: u64) -> u64 {
        (frame << 32) | slot
    }
    fn unpack_frame(p: u64) -> u64 {
        p >> 32
    }
    fn unpack_slot(p: u64) -> u64 {
        p & 0xFFFF_FFFF
    }

    #[test]
    fn frame_slot_fast_path_first_packet_wins() {
        loom::model(|| {
            let cell = Arc::new(AtomicU64::new(EMPTY));
            let running_frames: Arc<RwLock<Vec<(u64, u64)>>> = Arc::new(RwLock::new(Vec::new()));
            let activations = Arc::new(AtomicUsize::new(0));

            let mk = || {
                let (cell, rf, act) = (
                    Arc::clone(&cell),
                    Arc::clone(&running_frames),
                    Arc::clone(&activations),
                );
                loom::thread::spawn(move || -> (u64, bool) {
                    // Lock-free fast path (frame_slot_lookup).
                    let packed = cell.load(Ordering::Acquire);
                    if packed != EMPTY && unpack_frame(packed) == FRAME {
                        return (unpack_slot(packed), false);
                    }
                    // Slow path: authoritative re-scan under the write lock.
                    let mut rf = rf.write().unwrap();
                    if let Some(&(_, slot)) = rf.iter().find(|&&(f, _)| f == FRAME) {
                        return (slot, false);
                    }
                    // Activate: push then publish the cell (frame_slot_publish).
                    act.fetch_add(1, Ordering::Relaxed);
                    rf.push((FRAME, SLOT));
                    cell.store(pack(FRAME, SLOT), Ordering::Release);
                    (SLOT, true)
                })
            };

            let t1 = mk();
            let t2 = mk();
            let (s1, a1) = t1.join().unwrap();
            let (s2, a2) = t2.join().unwrap();

            // First-packet-wins: the frame is activated exactly once.
            assert_eq!(
                activations.load(Ordering::Relaxed),
                1,
                "frame activated more than once (first-packet-wins violated)"
            );
            // Both admitters resolve to the same slot.
            assert_eq!(s1, SLOT);
            assert_eq!(s2, SLOT);
            // Exactly one admitter reports the newly-activated transition.
            assert!(a1 ^ a2, "exactly one admitter must report newly-activated");
        });
    }
}
