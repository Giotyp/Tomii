use super::report::{
    aggregate_task_data, build_json_report_value, collect_print_stats_data,
    collect_report_stream_data, compute_critical_path_report, compute_node_stats,
    format_per_task_analysis, format_system_thread_stats, format_timing_summary,
    generate_optimization_suggestions,
};
use super::{SlotStats, TimingMethod, TimingRequest};
use crate::utils_rdtsc::{cycles_to_ns, rdtsc};
use rapidhash::{HashMapExt, RapidHashMap};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Asynchronous TimeBuffer controller
pub struct AsyncTimeBuffer {
    pub(super) request_tx: Sender<TimingRequest>,
    pub(super) controller_handle: Option<JoinHandle<()>>,
    time_buffer: Arc<Mutex<TimeBuffer>>, // Direct access for print_stats
}

impl AsyncTimeBuffer {
    /// Create a new AsyncTimeBuffer with a background controller
    pub fn new(slots: usize, system_threads: usize, use_rdtsc: bool) -> Self {
        let (request_tx, request_rx) = mpsc::channel();
        let time_buffer = Arc::new(Mutex::new(TimeBuffer::new(
            slots,
            system_threads,
            use_rdtsc,
        )));
        let time_buffer_clone = Arc::clone(&time_buffer);

        let controller_handle = thread::spawn(move || {
            let time_buffer = time_buffer_clone;

            while let Ok(request) = request_rx.recv() {
                match request {
                    TimingRequest::AddTaskTime {
                        slot_id,
                        task_name,
                        worker_id,
                        duration,
                    } => {
                        if let Ok(mut buf) = time_buffer.lock() {
                            buf.add_task_time(slot_id, &task_name, worker_id, duration);
                        }
                    }
                    TimingRequest::AddTaskTimeCycles {
                        slot_id,
                        task_name,
                        worker_id,
                        cycles,
                    } => {
                        if let Ok(mut buf) = time_buffer.lock() {
                            buf.add_task_time_cycles(slot_id, &task_name, worker_id, cycles);
                        }
                    }
                    TimingRequest::AddTaskTimeInstant {
                        slot_id,
                        task_name,
                        worker_id,
                        start,
                        end,
                    } => {
                        if let Ok(mut buf) = time_buffer.lock() {
                            buf.add_task_time_instant(slot_id, &task_name, worker_id, start, end);
                        }
                    }
                    TimingRequest::AddTaskTimeRdtsc {
                        slot_id,
                        task_name,
                        worker_id,
                        start_cycles,
                        end_cycles,
                    } => {
                        if let Ok(mut buf) = time_buffer.lock() {
                            buf.add_task_time_rdtsc(
                                slot_id,
                                &task_name,
                                worker_id,
                                start_cycles,
                                end_cycles,
                            );
                        }
                    }
                    TimingRequest::StartSlotProcessing {
                        slot_id,
                        start_time,
                    } => {
                        if let Ok(mut buf) = time_buffer.lock() {
                            buf.start_slot_processing_with_time(slot_id, start_time);
                        }
                    }
                    TimingRequest::FinishSlotProcessing {
                        slot_id,
                        end_time,
                        response_tx,
                    } => {
                        if let Ok(mut buf) = time_buffer.lock() {
                            let stats = buf.finish_slot_processing_with_time(slot_id, end_time);
                            let _ = response_tx.send(stats);
                        }
                    }
                    TimingRequest::GetSlotStatistics {
                        slot_id,
                        response_tx,
                    } => {
                        if let Ok(buf) = time_buffer.lock() {
                            let stats = buf.get_slot_statistics(slot_id).clone();
                            let _ = response_tx.send(stats);
                        }
                    }
                    TimingRequest::PrintStats {
                        bench_name,
                        out_file,
                        exclude_streams,
                        response_tx,
                    } => {
                        if let Ok(buf) = time_buffer.lock() {
                            buf.print_stats(&bench_name, out_file.as_deref(), exclude_streams);
                            let _ = response_tx.send(());
                        }
                    }
                    TimingRequest::Shutdown => {
                        break;
                    }
                }
            }
        });

        Self {
            request_tx,
            controller_handle: Some(controller_handle),
            time_buffer,
        }
    }

    /// Add a task timing measurement asynchronously (non-blocking)
    pub fn add_task_time_async(
        &self,
        slot_id: usize,
        task_name: &str,
        worker_id: usize,
        duration: Duration,
    ) {
        let request = TimingRequest::AddTaskTime {
            slot_id,
            task_name: task_name.to_string(),
            worker_id,
            duration,
        };
        // Send is non-blocking if the channel has capacity
        if let Err(_) = self.request_tx.send(request) {
            eprintln!("Warning: Failed to send timing request - controller may have shut down");
        }
    }

    /// Add a task timing measurement using rdtsc cycles asynchronously
    pub fn add_task_time_cycles_async(
        &self,
        slot_id: usize,
        task_name: &str,
        worker_id: usize,
        cycles: u64,
    ) {
        let request = TimingRequest::AddTaskTimeCycles {
            slot_id,
            task_name: task_name.to_string(),
            worker_id,
            cycles,
        };
        if let Err(_) = self.request_tx.send(request) {
            eprintln!("Warning: Failed to send timing request - controller may have shut down");
        }
    }

    /// Add a task timing measurement using start/end Instant asynchronously
    pub fn add_task_time_instant_async(
        &self,
        slot_id: usize,
        task_name: &str,
        worker_id: usize,
        start: Instant,
        end: Instant,
    ) {
        let request = TimingRequest::AddTaskTimeInstant {
            slot_id,
            task_name: task_name.to_string(),
            worker_id,
            start,
            end,
        };
        if let Err(_) = self.request_tx.send(request) {
            eprintln!("Warning: Failed to send timing request - controller may have shut down");
        }
    }

    /// Add a task timing measurement using start/end rdtsc cycles asynchronously
    pub fn add_task_time_rdtsc_async(
        &self,
        slot_id: usize,
        task_name: &str,
        worker_id: usize,
        start_cycles: u64,
        end_cycles: u64,
    ) {
        let request = TimingRequest::AddTaskTimeRdtsc {
            slot_id,
            task_name: task_name.to_string(),
            worker_id,
            start_cycles,
            end_cycles,
        };
        if let Err(_) = self.request_tx.send(request) {
            eprintln!("Warning: Failed to send timing request - controller may have shut down");
        }
    }

    /// Start slot processing asynchronously with timestamp captured at call site
    /// This ensures precise timing regardless of channel/queue latency
    pub fn start_slot_processing_async(&self, slot_id: usize, use_rdtsc: bool) {
        // Capture timestamp at call site for precise timing
        let start_time = if use_rdtsc {
            TimingMethod::Rdtsc(rdtsc())
        } else {
            TimingMethod::Instant(Instant::now())
        };
        let request = TimingRequest::StartSlotProcessing {
            slot_id,
            start_time,
        };
        if let Err(_) = self.request_tx.send(request) {
            eprintln!("Warning: Failed to send timing request - controller may have shut down");
        }
    }

    /// Finish slot processing and get stats (blocking - returns result)
    /// End timestamp is captured at call site for precise timing (matches start_slot_processing_async)
    pub fn finish_slot_processing(
        &self,
        slot_id: usize,
        use_rdtsc: bool,
    ) -> Result<SlotStats, &'static str> {
        // Capture end timestamp at call site, before channel send
        let end_time = if use_rdtsc {
            TimingMethod::Rdtsc(rdtsc())
        } else {
            TimingMethod::Instant(Instant::now())
        };
        let (response_tx, response_rx) = mpsc::channel();
        let request = TimingRequest::FinishSlotProcessing {
            slot_id,
            end_time,
            response_tx,
        };

        if let Err(_) = self.request_tx.send(request) {
            return Err("Failed to send request - controller may have shut down");
        }

        response_rx
            .recv()
            .map_err(|_| "Failed to receive response from controller")
    }

    /// Get slot statistics (blocking - returns result)
    pub fn get_slot_statistics(&self, slot_id: usize) -> Result<Vec<SlotStats>, &'static str> {
        let (response_tx, response_rx) = mpsc::channel();
        let request = TimingRequest::GetSlotStatistics {
            slot_id,
            response_tx,
        };

        if let Err(_) = self.request_tx.send(request) {
            return Err("Failed to send request - controller may have shut down");
        }

        response_rx
            .recv()
            .map_err(|_| "Failed to receive response from controller")
    }

    /// Print statistics directly (synchronous, bypasses async channel)
    pub fn print_stats(
        &self,
        bench_name: &str,
        out_file: Option<&str>,
        exclude_streams: usize,
    ) -> Result<(), &'static str> {
        // Direct access to TimeBuffer - no channel needed
        if let Ok(buf) = self.time_buffer.lock() {
            buf.print_stats(bench_name, out_file, exclude_streams);
            Ok(())
        } else {
            Err("Failed to acquire lock on time buffer")
        }
    }

    /// Write a JSON performance report (synchronous, bypasses async channel).
    pub fn write_json_report(
        &self,
        graph_edges: &[(String, Vec<String>)],
        path: &str,
        exclude_streams: usize,
    ) -> Result<(), &'static str> {
        if let Ok(buf) = self.time_buffer.lock() {
            buf.write_json_report(graph_edges, path, exclude_streams);
            Ok(())
        } else {
            Err("Failed to acquire lock on time buffer")
        }
    }

    /// Shutdown the controller and wait for it to finish
    pub fn shutdown(mut self) {
        if let Err(_) = self.request_tx.send(TimingRequest::Shutdown) {
            eprintln!("Warning: Failed to send shutdown request");
        }

        if let Some(handle) = self.controller_handle.take() {
            if let Err(_) = handle.join() {
                eprintln!("Warning: Controller thread panicked during shutdown");
            }
        }
    }
}

pub struct TimeBuffer {
    slots: usize,
    system_threads: usize,
    // Current timing state per slot
    slot_start_times: Vec<Option<TimingMethod>>,
    // Current task times for each slot (accumulated during processing)
    current_slot_tasks: Vec<RapidHashMap<String, Vec<(usize, Duration)>>>,
    // Completed slot statistics
    slot_statistics: Vec<Vec<SlotStats>>, // [slot][stream]
    // Use rdtsc timing instead of Instant
    use_rdtsc: bool,
}

impl TimeBuffer {
    /// Create a new TimeBuffer for the specified number of slots
    /// Use rdtsc_timing=true for high-precision timing, false for Instant-based timing
    pub fn new(slots: usize, system_threads: usize, use_rdtsc: bool) -> Self {
        TimeBuffer {
            slots,
            system_threads,
            slot_start_times: vec![None; slots],
            current_slot_tasks: vec![RapidHashMap::new(); slots],
            slot_statistics: vec![Vec::new(); slots],
            use_rdtsc,
        }
    }

    /// Mark the start of processing for a specific slot
    pub fn start_slot_processing(&mut self, slot_id: usize) {
        if slot_id >= self.slots {
            panic!(
                "Slot ID {} out of bounds (max: {})",
                slot_id,
                self.slots - 1
            );
        }

        let start_time = if self.use_rdtsc {
            TimingMethod::Rdtsc(rdtsc())
        } else {
            TimingMethod::Instant(Instant::now())
        };

        self.slot_start_times[slot_id] = Some(start_time);
        // Preserves any pre-slot-start timings
    }

    /// Mark the start of processing for a specific slot with pre-captured time
    /// Used by async mode to ensure precise timing regardless of channel latency
    pub fn start_slot_processing_with_time(&mut self, slot_id: usize, start_time: TimingMethod) {
        if slot_id >= self.slots {
            panic!(
                "Slot ID {} out of bounds (max: {})",
                slot_id,
                self.slots - 1
            );
        }

        self.slot_start_times[slot_id] = Some(start_time);
        // Preserves any pre-slot-start timings
    }

    /// Add a task timing measurement to a specific slot
    pub fn add_task_time(
        &mut self,
        slot_id: usize,
        task_name: &str,
        worker_id: usize,
        duration: Duration,
    ) {
        if slot_id >= self.slots {
            panic!(
                "Slot ID {} out of bounds (max: {})",
                slot_id,
                self.slots - 1
            );
        }

        self.current_slot_tasks[slot_id]
            .entry(task_name.to_string())
            .or_insert_with(Vec::new)
            .push((worker_id, duration));
    }

    /// Add a task timing measurement using rdtsc cycles
    pub fn add_task_time_cycles(
        &mut self,
        slot_id: usize,
        task_name: &str,
        worker_id: usize,
        cycles: u64,
    ) {
        let duration = Duration::from_nanos(cycles_to_ns(cycles) as u64);
        self.add_task_time(slot_id, task_name, worker_id, duration);
    }

    /// Add a task timing measurement using start/end Instant
    pub fn add_task_time_instant(
        &mut self,
        slot_id: usize,
        task_name: &str,
        worker_id: usize,
        start: Instant,
        end: Instant,
    ) {
        let duration = end.duration_since(start);
        self.add_task_time(slot_id, task_name, worker_id, duration);
    }

    /// Add a task timing measurement using start/end rdtsc cycles
    pub fn add_task_time_rdtsc(
        &mut self,
        slot_id: usize,
        task_name: &str,
        worker_id: usize,
        start_cycles: u64,
        end_cycles: u64,
    ) {
        let cycles = end_cycles.saturating_sub(start_cycles);
        self.add_task_time_cycles(slot_id, task_name, worker_id, cycles);
    }

    /// Finish processing for a slot and calculate total time
    /// Returns the SlotStats for this processing stream
    /// NOTE: End timestamp is captured at call site. Use finish_slot_processing_with_time
    /// for async mode where the end time is pre-captured before channel transit.
    pub fn finish_slot_processing(&mut self, slot_id: usize) -> SlotStats {
        let end_time = if self.use_rdtsc {
            TimingMethod::Rdtsc(rdtsc())
        } else {
            TimingMethod::Instant(Instant::now())
        };
        self.finish_slot_processing_with_time(slot_id, end_time)
    }

    /// Finish processing for a slot using a pre-captured end time
    /// Used by async mode to ensure precise timing regardless of channel latency
    pub fn finish_slot_processing_with_time(
        &mut self,
        slot_id: usize,
        end_time: TimingMethod,
    ) -> SlotStats {
        if slot_id >= self.slots {
            panic!(
                "Slot ID {} out of bounds (max: {})",
                slot_id,
                self.slots - 1
            );
        }

        let start_time = self.slot_start_times[slot_id].take().unwrap_or_else(|| {
            panic!(
                "Slot {} processing was never started or was already finished",
                slot_id
            )
        });

        let total_time = match (start_time, end_time) {
            (TimingMethod::Instant(start), TimingMethod::Instant(end)) => end.duration_since(start),
            (TimingMethod::Rdtsc(start_cycles), TimingMethod::Rdtsc(end_cycles)) => {
                let cycles = end_cycles.saturating_sub(start_cycles);
                Duration::from_nanos(cycles_to_ns(cycles) as u64)
            }
            _ => panic!("Cannot mix Instant and Rdtsc timing methods"),
        };

        let stream_count = self.slot_statistics[slot_id].len();
        let mut slot_stats = SlotStats::new(slot_id, stream_count);
        slot_stats.total_time = total_time;

        // Copy task times from current slot to slot stats
        for (task_name, times) in &self.current_slot_tasks[slot_id] {
            for &(worker_id, duration) in times {
                slot_stats.add_task_time(task_name, worker_id, duration);
            }
        }

        // Store the completed slot stats
        self.slot_statistics[slot_id].push(slot_stats.clone());

        // Clear task times for this slot after finishing (for next stream on this slot)
        self.current_slot_tasks[slot_id].clear();

        slot_stats
    }

    pub fn measure_time(&self) -> TimingMethod {
        if self.use_rdtsc {
            TimingMethod::Rdtsc(rdtsc())
        } else {
            TimingMethod::Instant(Instant::now())
        }
    }

    pub fn measure_duration(&self, start_time: TimingMethod, end_time: TimingMethod) -> Duration {
        match (start_time, end_time) {
            (TimingMethod::Instant(start), TimingMethod::Instant(end)) => end.duration_since(start),
            (TimingMethod::Rdtsc(start_cycles), TimingMethod::Rdtsc(end_cycles)) => {
                let cycles = end_cycles.saturating_sub(start_cycles);
                Duration::from_nanos(cycles_to_ns(cycles) as u64)
            }
            _ => panic!("Cannot mix Instant and Rdtsc timing methods"),
        }
    }

    /// Get all statistics for a specific slot
    pub fn get_slot_statistics(&self, slot_id: usize) -> &Vec<SlotStats> {
        if slot_id >= self.slots {
            panic!(
                "Slot ID {} out of bounds (max: {})",
                slot_id,
                self.slots - 1
            );
        }
        &self.slot_statistics[slot_id]
    }

    /// Get the latest statistics for a specific slot
    pub fn get_latest_slot_stats(&self, slot_id: usize) -> Option<&SlotStats> {
        if slot_id >= self.slots {
            panic!(
                "Slot ID {} out of bounds (max: {})",
                slot_id,
                self.slots - 1
            );
        }
        self.slot_statistics[slot_id].last()
    }

    /// Get the number of completed cycles for a slot
    pub fn get_slot_stream_count(&self, slot_id: usize) -> usize {
        if slot_id >= self.slots {
            panic!(
                "Slot ID {} out of bounds (max: {})",
                slot_id,
                self.slots - 1
            );
        }
        self.slot_statistics[slot_id].len()
    }

    /// Print comprehensive statistics for all slots with aggregated per-task analysis
    /// `exclude_streams` - Number of initial streams to exclude from average calculations (for steady-state measurement)
    pub fn print_stats(&self, bench_name: &str, out_file: Option<&str>, exclude_streams: usize) {
        let filler = "****************";
        let mut output_buffer = format!("Time Statistics for {}\n", bench_name);
        output_buffer.push_str(&format!("Total Slots: {}\n", self.slots));
        output_buffer.push_str(&format!(
            "Timing Method: {}\n",
            if self.use_rdtsc { "RDTSC" } else { "Instant" }
        ));

        // Calculate worker and system slot ranges
        let worker_slots_end = self.slots.saturating_sub(self.system_threads);
        let system_slots_start = worker_slots_end;

        output_buffer.push_str(&format!(
            "Worker Slots: 0..{}, System Thread Slots: {}..{}\n",
            worker_slots_end, system_slots_start, self.slots
        ));

        let (global_total_times, per_stream_task_data, system_task_data_by_slot, total_streams) =
            collect_print_stats_data(&self.slot_statistics, self.slots, system_slots_start);

        let (global_task_data, per_worker_counts, per_worker_totals) =
            aggregate_task_data(&per_stream_task_data, exclude_streams);

        output_buffer.push_str(&format_timing_summary(
            &global_total_times,
            &global_task_data,
            total_streams,
            exclude_streams,
            worker_slots_end,
            &self.slot_statistics,
            filler,
        ));

        let excluded_count = exclude_streams.min(total_streams);
        let steady_state_count = total_streams.saturating_sub(excluded_count);

        output_buffer.push_str(&format_per_task_analysis(
            &global_task_data,
            &per_worker_counts,
            &per_worker_totals,
            steady_state_count,
            filler,
        ));

        output_buffer.push_str(&format_system_thread_stats(
            &system_task_data_by_slot,
            system_slots_start,
            self.slots,
            filler,
        ));

        if let Some(out_file) = out_file {
            std::fs::write(out_file, &output_buffer).expect("Unable to write file");
        } else {
            print!("{}", output_buffer);
        }
    }

    /// Write a JSON performance report to the given path.
    ///
    /// `graph_edges` – `(node_name, Vec<successor_names>)` pairs describing the DAG.
    /// `exclude_streams` – number of leading streams to skip (warm-up exclusion, mirrors `print_stats`).
    pub fn write_json_report(
        &self,
        graph_edges: &[(String, Vec<String>)],
        path: &str,
        exclude_streams: usize,
    ) {
        // ── 1. Determine slot ranges (same split as print_stats) ──────────────────
        let worker_slots_end = self.slots.saturating_sub(self.system_threads);

        // ── 2+3. Collect and apply exclusion ──────────────────────────────────────
        let (included_total_times, per_stream_tasks) = match collect_report_stream_data(
            &self.slot_statistics,
            worker_slots_end,
            exclude_streams,
        ) {
            Some(data) => data,
            None => {
                eprintln!("[SynStream] No streams to report after exclusion.");
                return;
            }
        };

        let num_included = included_total_times.len();
        let included_tasks: Vec<&std::collections::HashMap<String, Vec<(usize, Duration)>>> =
            per_stream_tasks.iter().collect();

        // ── 4. Stream-level latency statistics ────────────────────────────────────
        let mut sorted_latencies: Vec<f64> = included_total_times
            .iter()
            .map(|d| d.as_nanos() as f64 / 1_000.0)
            .collect();
        sorted_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let avg_latency_us: f64 = sorted_latencies.iter().sum::<f64>() / num_included as f64;

        let percentile = |pct: f64| -> f64 {
            let idx = ((pct / 100.0) * (num_included as f64 - 1.0)).round() as usize;
            sorted_latencies[idx.min(num_included - 1)]
        };
        let p50_latency_us = percentile(50.0);
        let p99_latency_us = percentile(99.0);

        let total_wall_us: f64 = included_total_times
            .iter()
            .map(|d| d.as_nanos() as f64 / 1_000.0)
            .sum();
        let throughput_streams_per_sec = if total_wall_us > 0.0 {
            (num_included as f64) / (total_wall_us / 1_000_000.0)
        } else {
            0.0
        };

        // ── 5+6. Aggregate per-node timing and compute stats ──────────────────────
        let (node_stats_map, worker_busy_us) = compute_node_stats(&included_tasks, total_wall_us);

        // ── 7. Critical path via Kahn's topo-sort + DP ────────────────────────────
        let critical_path = compute_critical_path_report(graph_edges, &node_stats_map);

        // ── 8. Worker utilization ─────────────────────────────────────────────────
        let worker_denom = avg_latency_us * num_included as f64;
        let max_worker_id = worker_busy_us.keys().copied().max().unwrap_or(0);
        let worker_busy_pct: Vec<f64> = (0..=max_worker_id)
            .map(|wid| {
                let busy = worker_busy_us.get(&wid).copied().unwrap_or(0.0);
                if worker_denom > 0.0 {
                    busy / worker_denom * 100.0
                } else {
                    0.0
                }
            })
            .collect();

        // ── 9. Bottleneck hints ───────────────────────────────────────────────────
        let mut hints: Vec<String> = Vec::new();

        let critical_path_node_set: std::collections::HashSet<&str> = critical_path
            .as_ref()
            .map(|cp| cp.nodes.iter().map(String::as_str).collect())
            .unwrap_or_default();

        for (name, stats) in &node_stats_map {
            if critical_path_node_set.contains(name.as_str()) && stats.pct_of_total > 20.0 {
                hints.push(format!(
                    "node '{}' is on the critical path and accounts for {:.1}% of total compute time",
                    name, stats.pct_of_total
                ));
            }
        }

        if !worker_busy_pct.is_empty() {
            let max_pct = worker_busy_pct
                .iter()
                .cloned()
                .fold(f64::NEG_INFINITY, f64::max);
            let avg_pct = worker_busy_pct.iter().sum::<f64>() / worker_busy_pct.len() as f64;
            for (wid, &pct) in worker_busy_pct.iter().enumerate() {
                let diff = max_pct - pct;
                if diff > 10.0 {
                    hints.push(format!(
                        "worker {} utilization ({:.1}%) is {:.1}% below average — possible load imbalance",
                        wid, pct, avg_pct - pct
                    ));
                }
            }
        }

        if let Some(ref cp) = critical_path {
            if avg_latency_us > 0.0 {
                let ratio = cp.estimated_latency_us / avg_latency_us;
                if ratio > 0.8 {
                    hints.push(format!(
                        "critical path accounts for {:.1}% of average stream latency — limited parallelism gains from adding workers",
                        ratio * 100.0
                    ));
                }
            }
        }

        // ── 9b. Agent-native derived metrics ─────────────────────────────────────
        // Total tasks spawned per stream (sum of all node factors).
        let total_tasks_per_stream: usize = node_stats_map
            .values()
            .map(|s| {
                if num_included > 0 {
                    s.invocations / num_included
                } else {
                    0
                }
            })
            .sum();

        // Max factor among critical-path nodes (key input for tile-size suggestion).
        let max_cp_factor: usize = critical_path_node_set
            .iter()
            .filter_map(|name| node_stats_map.get(*name))
            .map(|s| {
                if num_included > 0 {
                    s.invocations / num_included
                } else {
                    0
                }
            })
            .max()
            .unwrap_or(0);

        // Scheduling overhead: gap between measured latency and critical-path compute.
        let cp_exec_us = critical_path
            .as_ref()
            .map_or(0.0, |cp| (cp.estimated_latency_us * 100.0).round() / 100.0);
        let overhead_us = (avg_latency_us
            - critical_path
                .as_ref()
                .map_or(0.0, |cp| cp.estimated_latency_us))
        .max(0.0);
        let overhead_pct = if avg_latency_us > 0.0 {
            overhead_us / avg_latency_us * 100.0
        } else {
            0.0
        };
        let sched_interpretation = if critical_path.is_none() {
            "critical path unavailable — pass graph edges to enable diagnostics".to_string()
        } else if overhead_pct >= 80.0 {
            format!(
                "{:.0}% of latency is scheduling overhead — reducing task count via graph \
                 coarsening will have high impact; try larger tile_size or group_size in \
                 your graph builder, or enable coalesce_barriers=True",
                overhead_pct
            )
        } else if overhead_pct >= 50.0 {
            format!(
                "{:.0}% of latency is scheduling overhead — consider coalesce_barriers=True, \
                 batching_size tuning, or inline_continuation=True",
                overhead_pct
            )
        } else if overhead_pct < 20.0 {
            format!(
                "{:.0}% of latency is scheduling overhead — compute-bound; focus on \
                 kernel optimization rather than graph structure",
                overhead_pct
            )
        } else {
            format!(
                "{:.0}% of latency is scheduling overhead — mixed profile; try both \
                 kernel optimization and scheduling knobs",
                overhead_pct
            )
        };

        // ── 9c. Structured optimization suggestions ───────────────────────────────
        let suggestions = generate_optimization_suggestions(
            overhead_pct,
            max_cp_factor,
            total_tasks_per_stream,
            critical_path.as_ref(),
            &worker_busy_pct,
        );

        // ── 10–11. Build JSON and write ───────────────────────────────────────────
        let report = build_json_report_value(
            num_included,
            avg_latency_us,
            p50_latency_us,
            p99_latency_us,
            throughput_streams_per_sec,
            total_tasks_per_stream,
            cp_exec_us,
            overhead_us,
            overhead_pct,
            &sched_interpretation,
            &node_stats_map,
            critical_path.as_ref(),
            max_cp_factor,
            &worker_busy_pct,
            &hints,
            suggestions,
            &critical_path_node_set,
        );

        // ── 12. Write to file ─────────────────────────────────────────────────────
        match std::fs::File::create(path) {
            Ok(file) => {
                if let Err(e) = serde_json::to_writer_pretty(file, &report) {
                    eprintln!("[SynStream] Failed to write JSON report: {}", e);
                } else {
                    println!("[SynStream] Report written to {}", path);
                }
            }
            Err(e) => {
                eprintln!("[SynStream] Failed to create report file '{}': {}", path, e);
            }
        }
    }

    /// Clear all statistics and reset the buffer
    pub fn reset(&mut self) {
        self.slot_start_times = vec![None; self.slots];
        self.current_slot_tasks = vec![RapidHashMap::new(); self.slots];
        self.slot_statistics = vec![Vec::new(); self.slots];
        // Note: system_threads is preserved as it's a constant configuration
    }

    /// Get a summary of all slots
    pub fn get_summary(&self) -> RapidHashMap<usize, (usize, Duration, Duration)> {
        let mut summary = RapidHashMap::new();

        for slot_id in 0..self.slots {
            let slot_stats = &self.slot_statistics[slot_id];
            if !slot_stats.is_empty() {
                let total_cycles = slot_stats.len();
                let total_times: Vec<Duration> = slot_stats.iter().map(|s| s.total_time).collect();
                let avg_time = total_times.iter().sum::<Duration>() / total_times.len() as u32;
                let total_time: Duration = total_times.iter().sum();

                summary.insert(slot_id, (total_cycles, avg_time, total_time));
            }
        }

        summary
    }
}
