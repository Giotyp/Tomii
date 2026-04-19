/// Private helpers for `TimeBuffer::print_stats` and `TimeBuffer::write_json_report`.
///
/// All items are `pub(super)` — visible to sibling `buffer.rs` but not outside the module.
use super::SlotStats;
use std::time::Duration;

// ── Module-level structs used by write_json_report helpers ───────────────────

/// Per-node aggregated timing statistics used in JSON reports.
pub(super) struct NodeStats {
    pub invocations: usize,
    pub mean_exec_us: f64,
    pub p99_exec_us: f64,
    pub total_exec_us: f64,
    pub pct_of_total: f64,
}

/// Critical-path result for a DAG, computed via Kahn's topo-sort + DP.
pub(super) struct ReportCriticalPath {
    pub length_nodes: usize,
    pub estimated_latency_us: f64,
    pub nodes: Vec<String>,
}

// ── Free helpers for print_stats ─────────────────────────────────────────────

/// Collect per-stream total times and per-stream task maps from worker slots,
/// and collect system-thread task times grouped by slot.
///
/// Returns `(global_total_times, per_stream_task_data, system_task_data_by_slot, total_streams)`.
#[allow(clippy::type_complexity)]
pub(super) fn collect_print_stats_data(
    slot_statistics: &[Vec<SlotStats>],
    slots: usize,
    system_slots_start: usize,
) -> (
    Vec<Duration>,
    Vec<std::collections::HashMap<String, Vec<(usize, Duration)>>>,
    std::collections::HashMap<usize, std::collections::HashMap<String, Vec<Duration>>>,
    usize,
) {
    let mut global_total_times: Vec<Duration> = Vec::new();
    let mut per_stream_task_data: Vec<std::collections::HashMap<String, Vec<(usize, Duration)>>> =
        Vec::new();
    let mut system_task_data_by_slot: std::collections::HashMap<
        usize,
        std::collections::HashMap<String, Vec<Duration>>,
    > = std::collections::HashMap::new();
    let mut total_streams = 0;

    for (slot_id, slot_stats) in slot_statistics.iter().enumerate().take(slots) {
        if slot_stats.is_empty() {
            continue;
        }

        if slot_id >= system_slots_start {
            // Collect system thread task data by slot
            let slot_task_data = system_task_data_by_slot.entry(slot_id).or_default();

            for stats in slot_stats {
                for (task_name, times) in &stats.task_times {
                    let task_durations = slot_task_data.entry(task_name.clone()).or_default();
                    for (_, duration) in times {
                        task_durations.push(*duration);
                    }
                }
            }
            continue;
        }

        total_streams += slot_stats.len();

        for stats in slot_stats {
            global_total_times.push(stats.total_time);

            let mut stream_tasks: std::collections::HashMap<String, Vec<(usize, Duration)>> =
                std::collections::HashMap::new();
            for (task_name, times) in &stats.task_times {
                stream_tasks.insert(task_name.clone(), times.clone());
            }
            per_stream_task_data.push(stream_tasks);
        }
    }

    (
        global_total_times,
        per_stream_task_data,
        system_task_data_by_slot,
        total_streams,
    )
}

/// Aggregate per-task durations and per-worker counts/totals across included streams
/// (i.e. after skipping the first `exclude_streams` streams).
///
/// Returns `(global_task_data, per_worker_counts, per_worker_totals)`.
#[allow(clippy::type_complexity)]
pub(super) fn aggregate_task_data(
    per_stream_task_data: &[std::collections::HashMap<String, Vec<(usize, Duration)>>],
    exclude_streams: usize,
) -> (
    std::collections::HashMap<String, Vec<Duration>>,
    std::collections::HashMap<String, std::collections::HashMap<usize, usize>>,
    std::collections::HashMap<String, std::collections::HashMap<usize, Duration>>,
) {
    let excluded_count = exclude_streams.min(per_stream_task_data.len());
    let streams_to_analyze: Vec<_> = if excluded_count > 0 {
        per_stream_task_data.iter().skip(excluded_count).collect()
    } else {
        per_stream_task_data.iter().collect()
    };

    let mut global_task_data: std::collections::HashMap<String, Vec<Duration>> =
        std::collections::HashMap::new();
    let mut global_per_worker_counts: std::collections::HashMap<
        String,
        std::collections::HashMap<usize, usize>,
    > = std::collections::HashMap::new();
    let mut global_per_worker_totals: std::collections::HashMap<
        String,
        std::collections::HashMap<usize, Duration>,
    > = std::collections::HashMap::new();

    for stream_tasks in streams_to_analyze {
        for (task_name, times) in stream_tasks {
            let task_durations = global_task_data.entry(task_name.clone()).or_default();

            for (worker_id, duration) in times {
                task_durations.push(*duration);

                let worker_counts = global_per_worker_counts
                    .entry(task_name.clone())
                    .or_default();
                *worker_counts.entry(*worker_id).or_insert(0) += 1;

                let worker_totals = global_per_worker_totals
                    .entry(task_name.clone())
                    .or_default();
                *worker_totals.entry(*worker_id).or_insert(Duration::ZERO) += *duration;
            }
        }
    }

    (
        global_task_data,
        global_per_worker_counts,
        global_per_worker_totals,
    )
}

/// Format the header and global timing statistics block (stream counts, averages, min/max).
pub(super) fn format_timing_summary(
    global_total_times: &[Duration],
    global_task_data: &std::collections::HashMap<String, Vec<Duration>>,
    total_streams: usize,
    exclude_streams: usize,
    worker_slots_end: usize,
    slot_statistics: &[Vec<SlotStats>],
    filler: &str,
) -> String {
    let mut out = format!("{}\nAggregated Statistics (All Slots):\n", filler);
    out.push_str(&format!("  Total Streams Processed: {}\n", total_streams));

    // Per-slot stream breakdown (worker slots only)
    out.push_str("  Streams per Slot: ");
    let mut slot_stream_items: Vec<String> = Vec::new();
    for (slot_id, slot_stats) in slot_statistics.iter().enumerate().take(worker_slots_end) {
        slot_stream_items.push(format!("Slot {}: {}", slot_id, slot_stats.len()));
    }
    out.push_str(&format!("{}\n", slot_stream_items.join(", ")));

    let excluded_count = exclude_streams.min(total_streams);
    let steady_state_count = total_streams.saturating_sub(excluded_count);

    if !global_total_times.is_empty() {
        let global_total: Duration = global_total_times.iter().sum();

        if excluded_count > 0 {
            out.push_str(&format!(
                "  Excluded Streams (warm-up): {} (Steady-state: {} streams)\n",
                excluded_count, steady_state_count
            ));
        }

        let steady_state_times: Vec<Duration> = if excluded_count > 0 && steady_state_count > 0 {
            global_total_times
                .iter()
                .skip(excluded_count)
                .copied()
                .collect()
        } else {
            global_total_times.to_vec()
        };

        let avg_total_time = if !steady_state_times.is_empty() {
            steady_state_times.iter().sum::<Duration>() / steady_state_times.len() as u32
        } else {
            Duration::ZERO
        };

        let std_dev_stream = if !steady_state_times.is_empty() {
            let mean_ns = avg_total_time.as_nanos() as f64;
            Duration::from_nanos(
                (steady_state_times
                    .iter()
                    .map(|d| {
                        let diff = d.as_nanos() as f64 - mean_ns;
                        diff * diff
                    })
                    .sum::<f64>()
                    / steady_state_times.len() as f64)
                    .sqrt() as u64,
            )
        } else {
            Duration::ZERO
        };

        let min_total_time = if !steady_state_times.is_empty() {
            steady_state_times.iter().min().unwrap()
        } else {
            global_total_times.iter().min().unwrap()
        };

        let max_total_time = if !steady_state_times.is_empty() {
            steady_state_times.iter().max().unwrap()
        } else {
            global_total_times.iter().max().unwrap()
        };

        let total_compute_time_all = global_task_data
            .values()
            .map(|times| times.iter().sum::<Duration>())
            .sum::<Duration>();

        let avg_compute_time = if steady_state_count > 0 {
            total_compute_time_all / steady_state_count as u32
        } else {
            total_compute_time_all / total_streams as u32
        };

        out.push_str(&format!("  Total Runtime: {:.4?}\n", global_total));
        out.push_str(&format!(
            "  Avg Time Per Stream: {:.4?} (std: {:.4?})\n",
            avg_total_time, std_dev_stream
        ));
        out.push_str(&format!(
            "  Min/Max Per Stream: {:.4?} / {:.4?}\n",
            min_total_time, max_total_time
        ));
        out.push_str(&format!(
            "  Total Compute Time: {:.4?}\n",
            total_compute_time_all
        ));
        out.push_str(&format!(
            "  Avg Compute Time Per Stream: {:.4?}\n",
            avg_compute_time
        ));
    }

    out
}

/// Format the per-task analysis section (one entry per task, with worker breakdown).
pub(super) fn format_per_task_analysis(
    global_task_data: &std::collections::HashMap<String, Vec<Duration>>,
    per_worker_counts: &std::collections::HashMap<String, std::collections::HashMap<usize, usize>>,
    per_worker_totals: &std::collections::HashMap<
        String,
        std::collections::HashMap<usize, Duration>,
    >,
    steady_state_count: usize,
    filler: &str,
) -> String {
    let mut out = format!("{}\nPer-Task Analysis (Aggregated):\n", filler);

    let mut sorted_tasks: Vec<_> = global_task_data.keys().cloned().collect();
    sorted_tasks.sort();

    for task_name in sorted_tasks {
        if let Some(task_times) = global_task_data.get(&task_name) {
            if task_times.is_empty() {
                continue;
            }

            out.push_str(&format!("  {}\n", filler));

            let total_executions = task_times.len();
            let total_time: Duration = task_times.iter().sum();

            let avg_time = if steady_state_count > 0 {
                total_time / steady_state_count as u32
            } else {
                Duration::ZERO
            };

            let avg_task = total_time / total_executions as u32;
            let min_time = task_times.iter().min().unwrap();
            let max_time = task_times.iter().max().unwrap();
            let mean_nanos = avg_task.as_nanos() as f64;
            let std_dev_task = Duration::from_nanos(
                (task_times
                    .iter()
                    .map(|d| {
                        let diff = d.as_nanos() as f64 - mean_nanos;
                        diff * diff
                    })
                    .sum::<f64>()
                    / total_executions as f64)
                    .sqrt() as u64,
            );

            let worker_counts = per_worker_counts.get(&task_name).unwrap();
            let worker_totals = per_worker_totals.get(&task_name).unwrap();

            out.push_str(&format!(
                "  Task '{}' - Workers: {}, Total Executions: {}\n",
                task_name,
                worker_counts.len(),
                total_executions
            ));

            out.push_str(&format!(
                "    Timing - Avg/Stream: {:.4?}, Avg/Task: {:.4?}, Std: {:.4?}, Min: {:.4?}, Max: {:.4?}, Total: {:.4?}\n",
                avg_time, avg_task, std_dev_task, min_time, max_time, total_time
            ));

            out.push_str("    Worker Summary: ");
            let mut worker_items: Vec<String> = Vec::new();
            for (worker_id, count) in worker_counts.iter() {
                let pct = (*count as f64) / (total_executions as f64) * 100.0;
                let time_total = worker_totals.get(worker_id).unwrap_or(&Duration::ZERO);
                let label = if *worker_id == usize::MAX {
                    "runtime".to_string()
                } else {
                    format!("W-{}", worker_id)
                };
                worker_items.push(format!(
                    "{}: {} ({:.1}%) - {:.4?}",
                    label, count, pct, time_total
                ));
            }
            out.push_str(&format!("{}\n", worker_items.join(", ")));
        }
    }

    out
}

/// Format the system-thread task statistics section (one sub-section per system slot).
pub(super) fn format_system_thread_stats(
    system_task_data_by_slot: &std::collections::HashMap<
        usize,
        std::collections::HashMap<String, Vec<Duration>>,
    >,
    system_slots_start: usize,
    slots: usize,
    filler: &str,
) -> String {
    if system_task_data_by_slot.is_empty() {
        return String::new();
    }

    let mut out = format!(
        "{}\nSystem Thread Tasks (Slots {}..{}):\n",
        filler, system_slots_start, slots
    );

    for slot_id in system_slots_start..slots {
        let thread_id = slot_id - system_slots_start;

        if let Some(slot_task_data) = system_task_data_by_slot.get(&slot_id) {
            out.push_str(&format!(
                "  Resolution Thread {} (Slot {}):\n",
                thread_id, slot_id
            ));

            let mut sorted_system_tasks: Vec<_> = slot_task_data.keys().cloned().collect();
            sorted_system_tasks.sort();

            for task_name in sorted_system_tasks {
                if let Some(task_times) = slot_task_data.get(&task_name) {
                    if task_times.is_empty() {
                        continue;
                    }

                    let total_executions = task_times.len();
                    let min_time = task_times.iter().min().unwrap();
                    let max_time = task_times.iter().max().unwrap();
                    let total_time: Duration = task_times.iter().sum();
                    let avg_time = total_time / total_executions as u32;
                    let mean_nanos = avg_time.as_nanos() as f64;
                    let std_dev = Duration::from_nanos(
                        (task_times
                            .iter()
                            .map(|d| {
                                let diff = d.as_nanos() as f64 - mean_nanos;
                                diff * diff
                            })
                            .sum::<f64>()
                            / total_executions as f64)
                            .sqrt() as u64,
                    );

                    out.push_str(&format!(
                        "    Task '{}' - Executions: {}, Avg: {:.4?}, Std: {:.4?}, Min: {:.4?}, Max: {:.4?}, Total: {:.4?}\n",
                        task_name, total_executions, avg_time, std_dev, min_time, max_time, total_time
                    ));
                }
            }
        }
    }

    out
}

// ── Free helpers for write_json_report ───────────────────────────────────────

/// Collect per-stream total times and per-stream task maps for included streams
/// (worker slots only, with warm-up exclusion applied).
///
/// Returns `None` if no streams remain after exclusion.
#[allow(clippy::type_complexity)]
pub(super) fn collect_report_stream_data(
    slot_statistics: &[Vec<SlotStats>],
    worker_slots_end: usize,
    exclude_streams: usize,
) -> Option<(
    Vec<Duration>,
    Vec<std::collections::HashMap<String, Vec<(usize, Duration)>>>,
)> {
    let mut stream_total_times: Vec<Duration> = Vec::new();
    let mut per_stream_tasks: Vec<std::collections::HashMap<String, Vec<(usize, Duration)>>> =
        Vec::new();

    for stats_list in slot_statistics.iter().take(worker_slots_end) {
        for stats in stats_list {
            stream_total_times.push(stats.total_time);
            let mut m: std::collections::HashMap<String, Vec<(usize, Duration)>> =
                std::collections::HashMap::new();
            for (name, entries) in &stats.task_times {
                m.insert(name.clone(), entries.clone());
            }
            per_stream_tasks.push(m);
        }
    }

    let total_streams = stream_total_times.len();
    let excluded = exclude_streams.min(total_streams);

    let included_total_times: Vec<Duration> =
        stream_total_times.iter().skip(excluded).copied().collect();
    let included_tasks: Vec<std::collections::HashMap<String, Vec<(usize, Duration)>>> =
        per_stream_tasks.into_iter().skip(excluded).collect();

    if included_total_times.is_empty() {
        return None;
    }

    Some((included_total_times, included_tasks))
}

/// Aggregate per-node execution times across included streams and compute `NodeStats`
/// for each node. Also returns per-worker busy time in microseconds.
///
/// Returns `(node_stats_map, worker_busy_us)`.
pub(super) fn compute_node_stats(
    included_tasks: &[&std::collections::HashMap<String, Vec<(usize, Duration)>>],
    total_wall_us: f64,
) -> (
    std::collections::HashMap<String, NodeStats>,
    std::collections::HashMap<usize, f64>,
) {
    let mut node_entries: std::collections::HashMap<String, Vec<(usize, f64)>> =
        std::collections::HashMap::new();
    let mut worker_busy_us: std::collections::HashMap<usize, f64> =
        std::collections::HashMap::new();

    for stream_map in included_tasks {
        for (name, entries) in *stream_map {
            let bucket = node_entries.entry(name.clone()).or_default();
            for &(wid, dur) in entries {
                let us = dur.as_nanos() as f64 / 1_000.0;
                bucket.push((wid, us));
                if wid != usize::MAX {
                    *worker_busy_us.entry(wid).or_insert(0.0) += us;
                }
            }
        }
    }

    let num_workers = {
        let max_w = worker_busy_us.keys().copied().max().unwrap_or(0);
        max_w + 1
    };
    let denominator_us = total_wall_us * (num_workers as f64).max(1.0);

    let mut node_stats_map: std::collections::HashMap<String, NodeStats> =
        std::collections::HashMap::new();
    for (name, entries) in &node_entries {
        let invocations = entries.len();
        let total_exec_us: f64 = entries.iter().map(|(_, us)| us).sum();
        let mean_exec_us = total_exec_us / invocations as f64;
        let pct_of_total = if denominator_us > 0.0 {
            total_exec_us / denominator_us * 100.0
        } else {
            0.0
        };
        let mut sorted_us: Vec<f64> = entries.iter().map(|(_, us)| *us).collect();
        sorted_us.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p99_idx =
            ((0.99 * (sorted_us.len() as f64 - 1.0)).round() as usize).min(sorted_us.len() - 1);
        let p99_exec_us = sorted_us[p99_idx];
        node_stats_map.insert(
            name.clone(),
            NodeStats {
                invocations,
                mean_exec_us,
                p99_exec_us,
                total_exec_us,
                pct_of_total,
            },
        );
    }

    (node_stats_map, worker_busy_us)
}

/// Compute the critical path through the DAG described by `graph_edges` using
/// Kahn's topological sort + longest-path DP weighted by mean node execution time.
///
/// Returns `None` when `graph_edges` is empty.
pub(super) fn compute_critical_path_report(
    graph_edges: &[(String, Vec<String>)],
    node_stats_map: &std::collections::HashMap<String, NodeStats>,
) -> Option<ReportCriticalPath> {
    if graph_edges.is_empty() {
        return None;
    }

    let mut name_to_idx: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for (i, (name, _)) in graph_edges.iter().enumerate() {
        name_to_idx.insert(name.as_str(), i);
    }
    let n = graph_edges.len();

    let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut in_degree: Vec<usize> = vec![0; n];
    for (i, (_, succs)) in graph_edges.iter().enumerate() {
        for succ_name in succs {
            if let Some(&j) = name_to_idx.get(succ_name.as_str()) {
                adj[i].push(j);
                in_degree[j] += 1;
            }
        }
    }

    let mut queue: std::collections::VecDeque<usize> = in_degree
        .iter()
        .enumerate()
        .filter(|(_, &d)| d == 0)
        .map(|(i, _)| i)
        .collect();
    let mut topo_order: Vec<usize> = Vec::with_capacity(n);
    let mut in_deg = in_degree.clone();
    while let Some(u) = queue.pop_front() {
        topo_order.push(u);
        for &v in &adj[u] {
            in_deg[v] -= 1;
            if in_deg[v] == 0 {
                queue.push_back(v);
            }
        }
    }

    let weights: Vec<f64> = graph_edges
        .iter()
        .map(|(name, _)| {
            node_stats_map
                .get(name.as_str())
                .map_or(0.0, |s| s.mean_exec_us)
        })
        .collect();

    let mut dist: Vec<f64> = weights.clone();
    let mut prev: Vec<Option<usize>> = vec![None; n];

    for &u in &topo_order {
        for &v in &adj[u] {
            let new_dist = dist[u] + weights[v];
            if new_dist > dist[v] {
                dist[v] = new_dist;
                prev[v] = Some(u);
            }
        }
    }

    let (end_node, &max_dist) = dist
        .iter()
        .enumerate()
        .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
        .unwrap_or((0, &0.0));

    let mut path: Vec<usize> = Vec::new();
    let mut cur = end_node;
    loop {
        path.push(cur);
        match prev[cur] {
            Some(p) => cur = p,
            None => break,
        }
    }
    path.reverse();

    let path_names: Vec<String> = path.iter().map(|&i| graph_edges[i].0.clone()).collect();

    Some(ReportCriticalPath {
        length_nodes: path.len(),
        estimated_latency_us: max_dist,
        nodes: path_names,
    })
}

/// Apply the structured optimization suggestion rule engine and return the
/// resulting JSON suggestion objects.
pub(super) fn generate_optimization_suggestions(
    overhead_pct: f64,
    max_cp_factor: usize,
    total_tasks_per_stream: usize,
    critical_path: Option<&ReportCriticalPath>,
    worker_busy_pct: &[f64],
) -> Vec<serde_json::Value> {
    use serde_json::json;
    let mut suggestions: Vec<serde_json::Value> = Vec::new();

    // A. Graph topology coarsening (highest impact when overhead-dominated).
    if overhead_pct > 60.0 && max_cp_factor >= 64 {
        if let Some(cp) = critical_path {
            let suggested_tile = max_cp_factor / 8;
            let speedup_lo = (max_cp_factor / 16).max(1);
            let speedup_hi = max_cp_factor / 8;
            suggestions.push(json!({
                "priority": 1,
                "category": "graph_topology",
                "description": format!(
                    "Critical path has {} nodes; highest-factor critical-path node has \
                     factor {} ({} total tasks/stream). Graph coarsening will cut \
                     scheduling overhead by ~{}x.",
                    cp.length_nodes, max_cp_factor, total_tasks_per_stream,
                    max_cp_factor / 8
                ),
                "action": format!(
                    "In your graph builder, reduce the per-node factor from {} to ~{} \
                     by increasing tile_size from 1 to {}.",
                    max_cp_factor, max_cp_factor / 8, suggested_tile
                ),
                "knob": "tile_size",
                "suggested_value": suggested_tile,
                "estimated_speedup": format!("{}–{}x", speedup_lo, speedup_hi),
                "confidence": "high",
            }));
        }
    }

    // A'. High sequential-node-count, low per-node factor: wrong graph structure.
    if overhead_pct > 60.0 && max_cp_factor < 16 && total_tasks_per_stream > 200 {
        if let Some(cp) = critical_path {
            if cp.length_nodes > 50 {
                suggestions.push(json!({
                    "priority": 1,
                    "category": "graph_topology",
                    "description": format!(
                        "Critical path has {} nodes with max per-node factor {} \
                         ({:.0}% overhead). The graph is too sequential: the critical \
                         path is long but each node does little parallel work. \
                         Restructure so each node covers one parallel work unit \
                         with a large factor (e.g. one node per diagonal, \
                         factor = cells_in_diagonal).",
                        cp.length_nodes, max_cp_factor, overhead_pct
                    ),
                    "action": "Rewrite your graph builder to create one node per parallel \
                               work unit with factor = number_of_parallel_items. \
                               For a wavefront sweep: loop over anti-diagonals \
                               (d in 0..2N-1), one node per diagonal with \
                               factor = min(d+1, N, 2N-1-d), then apply coarsening \
                               as shown in AGENT.md § Graph Coarsening Recipe. \
                               Do NOT simply change tile_size — the graph loop \
                               structure itself needs to change.",
                    "knob": "graph_structure",
                    "suggested_value": null,
                    "estimated_speedup": "3–10x",
                    "confidence": "high",
                }));
            }
        }
    }

    // A''. Mixed overhead zone with over-coarsened graph (small factor, moderate overhead).
    if overhead_pct > 20.0 && overhead_pct < 60.0 && max_cp_factor > 0 && max_cp_factor < 8 {
        if let Some(cp) = critical_path {
            if cp.length_nodes >= 4 {
                let suggested_factor = (max_cp_factor * 2).max(8);
                suggestions.push(json!({
                    "priority": 2,
                    "category": "graph_topology",
                    "description": format!(
                        "Overhead is {:.0}% (mixed profile) with only {} tasks per CP node. \
                         The graph may be over-coarsened: too few parallel tasks per node to \
                         keep all workers busy. Increasing factor to ~{} exposes more \
                         parallel work per node.",
                        overhead_pct, max_cp_factor, suggested_factor
                    ),
                    "action": format!(
                        "Double the factor on critical-path nodes to ~{} (currently {}). \
                         If your graph builder uses a tile_size or group_size to compute \
                         factor, halve that parameter. If you set factor directly in \
                         graph.node(), increase it to ~{}. Then re-benchmark.",
                        suggested_factor, max_cp_factor, suggested_factor
                    ),
                    "knob": "factor",
                    "suggested_value": suggested_factor,
                    "estimated_speedup": "1.3–2x",
                    "confidence": "medium",
                }));
            }
        }
    }

    // B. coalesce_barriers for high-factor barrier fan-outs.
    if overhead_pct > 40.0 && max_cp_factor >= 8 {
        suggestions.push(json!({
            "priority": 2,
            "category": "runtime_flags",
            "description": format!(
                "Barrier fan-out overhead is likely significant with max critical-path \
                 factor {}. coalesce_barriers groups simultaneous completions into bulk tasks.",
                max_cp_factor
            ),
            "action": "Set coalesce_barriers=True in graph.run()",
            "knob": "coalesce_barriers",
            "suggested_value": true,
            "estimated_speedup": "1.2–2x",
            "confidence": "medium",
        }));
    }

    // C. batching_size for high task counts.
    if total_tasks_per_stream > 10_000 && overhead_pct > 40.0 {
        suggestions.push(json!({
            "priority": 3,
            "category": "runtime_flags",
            "description": format!(
                "{} tasks/stream creates high scheduler-submission pressure. \
                 Larger batching_size amortizes per-batch overhead.",
                total_tasks_per_stream
            ),
            "action": "Try batching_size=64 (or 16, 256) in graph.run()",
            "knob": "batching_size",
            "suggested_value": 64,
            "estimated_speedup": "1.1–1.5x",
            "confidence": "medium",
        }));
    }

    // D. Worker underutilization after coarsening.
    if !worker_busy_pct.is_empty() && max_cp_factor > 8 && overhead_pct < 60.0 {
        let max_util = worker_busy_pct
            .iter()
            .cloned()
            .fold(f64::NEG_INFINITY, f64::max);
        if max_util < 50.0 {
            if let Some(cp) = critical_path {
                if cp.length_nodes > 10 {
                    suggestions.push(json!({
                        "priority": 4,
                        "category": "parallelism",
                        "description": format!(
                            "Peak worker utilization is only {:.0}% — workers are mostly idle \
                             because the critical path serialises execution. Τομί workers \
                             are Rayon threads that consume graph tasks; intra-task thread \
                             parallelism (e.g. adding Rayon inside a kernel function) is NOT \
                             the fix and will not compile due to Send/Sync constraints.",
                            max_util
                        ),
                        "action": "Reduce tile_size to create more parallel tasks per diagonal \
                                   (e.g. tile_size = max_node_factor / 4). Do NOT add Rayon \
                                   or threads inside kernel functions.",
                        "knob": "tile_size",
                        "suggested_value": (max_cp_factor / 4).max(1),
                        "estimated_speedup": "1.5–3x",
                        "confidence": "medium",
                    }));
                }
            }
        }
    }

    suggestions
}

/// Assemble the final JSON report value from all pre-computed components.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_json_report_value(
    num_included: usize,
    avg_latency_us: f64,
    p50_latency_us: f64,
    p99_latency_us: f64,
    throughput_streams_per_sec: f64,
    total_tasks_per_stream: usize,
    cp_exec_us: f64,
    overhead_us: f64,
    overhead_pct: f64,
    sched_interpretation: &str,
    node_stats_map: &std::collections::HashMap<String, NodeStats>,
    critical_path: Option<&ReportCriticalPath>,
    max_cp_factor: usize,
    worker_busy_pct: &[f64],
    hints: &[String],
    suggestions: Vec<serde_json::Value>,
    critical_path_node_set: &std::collections::HashSet<&str>,
) -> serde_json::Value {
    use serde_json::json;

    // Build per-node JSON array
    let mut per_node_entries: Vec<serde_json::Value> = node_stats_map
        .iter()
        .map(|(name, stats)| {
            let factor = if num_included > 0 {
                stats.invocations / num_included
            } else {
                0
            };
            json!({
                "name": name,
                "factor": factor,
                "invocations": stats.invocations,
                "mean_exec_us": (stats.mean_exec_us * 100.0).round() / 100.0,
                "p99_exec_us": (stats.p99_exec_us * 100.0).round() / 100.0,
                "total_exec_us": (stats.total_exec_us * 100.0).round() / 100.0,
                "pct_of_total": (stats.pct_of_total * 10.0).round() / 10.0,
                "on_critical_path": critical_path_node_set.contains(name.as_str()),
            })
        })
        .collect();
    per_node_entries.sort_by(|a, b| {
        let ta = a["total_exec_us"].as_f64().unwrap_or(0.0);
        let tb = b["total_exec_us"].as_f64().unwrap_or(0.0);
        tb.partial_cmp(&ta).unwrap()
    });

    // Assemble critical-path JSON
    let critical_path_json = match critical_path {
        Some(cp) => {
            let nodes_sample: Vec<String> = if cp.nodes.len() > 5 {
                let mut s: Vec<String> = cp.nodes[..5].to_vec();
                s.push(format!("... ({} more)", cp.nodes.len() - 5));
                s
            } else {
                cp.nodes.clone()
            };
            json!({
                "length_nodes": cp.length_nodes,
                "max_node_factor": max_cp_factor,
                "estimated_latency_us": (cp.estimated_latency_us * 100.0).round() / 100.0,
                "nodes_sample": nodes_sample,
            })
        }
        None => json!(null),
    };

    let worker_busy_pct_rounded: Vec<f64> = worker_busy_pct
        .iter()
        .map(|&p| (p * 10.0).round() / 10.0)
        .collect();

    json!({
        "summary": {
            "total_streams": num_included,
            "avg_latency_us": (avg_latency_us * 100.0).round() / 100.0,
            "p50_latency_us": (p50_latency_us * 100.0).round() / 100.0,
            "p99_latency_us": (p99_latency_us * 100.0).round() / 100.0,
            "throughput_streams_per_sec": (throughput_streams_per_sec * 10.0).round() / 10.0,
            "total_tasks_per_stream": total_tasks_per_stream,
            "scheduling_overhead_diagnostic": {
                "critical_path_exec_us": cp_exec_us,
                "overhead_us": (overhead_us * 100.0).round() / 100.0,
                "overhead_pct": (overhead_pct * 10.0).round() / 10.0,
                "interpretation": sched_interpretation,
            },
        },
        "per_node": per_node_entries,
        "critical_path": critical_path_json,
        "resource_utilization": {
            "worker_busy_pct": worker_busy_pct_rounded,
        },
        "bottleneck_hints": hints,
        "optimization_suggestions": suggestions,
    })
}
