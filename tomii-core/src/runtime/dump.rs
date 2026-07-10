//! Runtime state snapshots for offline debugging (`--dump-state`, P7).
//!
//! [`StateDumper`] holds an `Arc<SharedData>` clone so a watcher thread (or
//! the embedding application) can serialize the runtime's observable state at
//! any moment — including while the run is wedged, which is the case that
//! matters.  All reads are atomics or short read-lock acquisitions; a dump
//! never blocks the hot path beyond those.
//!
//! The output is a single JSON object: per-slot state (lifecycle state,
//! frame id, generation, pending counters), global progress counters,
//! scheduler totals, and — on network builds — receiver-side counters
//! including parked out-of-window packets.

use super::shared_data::{SharedData, SlotState};
use serde_json::{json, Value};
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// Cloneable handle for snapshotting a runtime's state.
///
/// Obtain via [`super::TomiiRt::state_dumper`].
#[derive(Clone)]
pub struct StateDumper {
    pub(super) shared: Arc<SharedData>,
}

impl StateDumper {
    /// Serialize the current runtime state to a pretty-printed JSON string.
    pub fn dump(&self) -> String {
        serde_json::to_string_pretty(&self.dump_value()).expect("state JSON")
    }

    /// Serialize the current runtime state as a `serde_json::Value`.
    pub fn dump_value(&self) -> Value {
        let shared = &self.shared;
        let slots = shared.config.slots;

        let states: Vec<String> = {
            let guard = shared.slot_data.states.read();
            guard
                .iter()
                .map(|s| {
                    match s {
                        SlotState::Active => "active",
                        SlotState::Buffering => "buffering",
                        SlotState::Inactive => "inactive",
                    }
                    .to_string()
                })
                .collect()
        };
        let running_frames: Vec<(usize, usize)> = shared.slot_data.running_frames.read().clone();

        let slot_entries: Vec<Value> = (0..slots)
            .map(|slot| {
                let frame_id = shared.slot_data.frame_id[slot].load(Ordering::Relaxed);
                let frame_id_json = if frame_id == usize::MAX {
                    Value::Null
                } else {
                    json!(frame_id)
                };
                json!({
                    "slot": slot,
                    "state": states.get(slot),
                    "frame_id": frame_id_json,
                    "generation": shared.slot_data.generation[slot].load(Ordering::Relaxed),
                    "pending_tasks": shared.slot_data.pending_tasks[slot].load(Ordering::Relaxed),
                    "pending_cond_tasks": shared.slot_data.pending_cond_tasks[slot].load(Ordering::Relaxed),
                    "processing_count": shared.slot_data.processing_count[slot].load(Ordering::Relaxed),
                    "needs_check": shared.slot_data.needs_check[slot].load(Ordering::Relaxed),
                    "packet_count": shared.slot_data.packet_counters[slot].load(Ordering::Relaxed),
                    "packet_complete": shared.slot_data.packet_complete[slot].load(Ordering::Relaxed),
                })
            })
            .collect();

        let mut root = json!({
            "graph": {
                "nodes": shared.graph.nodes.len(),
                "total_tasks_per_frame": shared.graph_cache.total_tasks,
                "total_cond_tasks_per_frame": shared.graph_cache.total_cond_tasks,
            },
            "config": {
                "slots": slots,
                "max_frames": shared.config.max_frames,
                "workers": shared.config.workers,
                "system_threads": shared.config.system_threads,
            },
            "progress": {
                "frames_completed": shared.telemetry.frame_complete_counter.load(Ordering::Relaxed),
                "jobs_recorded": shared.telemetry.job_counter.load(Ordering::Relaxed),
                "shutdown": shared.shutdown_flag.load(Ordering::Relaxed),
            },
            "scheduler": {
                "total_spawned": shared.exec.scheduler.total_jobs_spawned(),
                "total_completed": shared.exec.scheduler.total_jobs_completed(),
                "batch_queue_len": shared.exec.batch_queue_rx.len(),
            },
            "slot_data": {
                "active_bitmap": format!("{:#x}", shared.slot_data.active_bitmap.load(Ordering::Relaxed)),
                "last_assigned": shared.slot_data.last_assigned.load(Ordering::Relaxed),
                "running_frames": running_frames,
                "slots": slot_entries,
            },
        });

        #[cfg(feature = "network")]
        {
            root["network"] = json!({
                "frames_received": shared.net.frames_receive_counter.load(Ordering::Relaxed),
                "dropped_frames": shared.net.dropped_frames.load(Ordering::Relaxed),
                "receive_finished": shared.net.receive_finished.load(Ordering::Relaxed),
                "parked_packets": shared.net.pending_count.load(Ordering::Relaxed),
                "parked_frames": shared
                    .net
                    .pending_frames
                    .lock()
                    .keys()
                    .copied()
                    .collect::<Vec<_>>(),
            });
        }

        root
    }
}
