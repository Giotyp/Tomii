use super::shared_data::{RuntimeConfig, SlotData};
use std::sync::atomic::Ordering;

/// Check if we should record for a given slot based on its current stream ID.
/// Returns true if recording is enabled for all streams (None) or if the slot's
/// current stream matches the target stream.
#[inline(always)]
pub(super) fn should_record_slot(
    config: &RuntimeConfig,
    slot_data: &SlotData,
    slot: usize,
) -> bool {
    match config.record_stream {
        None => true, // Record all streams
        Some(target_stream) => slot_data.stream_id[slot].load(Ordering::Relaxed) == target_stream,
    }
}

impl super::SynRt {
    pub fn print_statistics(
        &self,
        bench_name: &str,
        out_file: Option<&str>,
        exclude_streams: usize,
    ) {
        self.shared
            .telemetry
            .with_timing(|tb| tb.print_stats(bench_name, out_file, exclude_streams));
    }

    pub fn write_json_report(&self, path: &str, exclude_streams: usize) {
        self.shared.telemetry.with_timing(|tb| {
            let graph_edges: Vec<(String, Vec<String>)> = self
                .shared
                .graph
                .nodes
                .iter()
                .map(|node| {
                    let node_id = node.id as usize;
                    let succs: Vec<String> = if node_id < self.shared.graph.successors.len() {
                        self.shared.graph.successors[node_id]
                            .iter()
                            .map(|&sid| self.shared.graph.nodes[sid as usize].name.clone())
                            .collect()
                    } else {
                        Vec::new()
                    };
                    (node.name.clone(), succs)
                })
                .collect();
            tb.write_json_report(&graph_edges, path, exclude_streams);
        });
    }

    pub fn write_record(&self, path: &str) {
        self.shared.exec.scheduler.write_record(path);
        self.write_runtime_record(path);
    }

    pub fn write_runtime_record(&self, _path: &str) {
        if let Some(_rec) = &self.shared.telemetry.async_recorder {
            tracing::debug!("async_recorder records already written via scheduler");
        } else {
            tracing::debug!("recorder not enabled");
        }
    }
}
