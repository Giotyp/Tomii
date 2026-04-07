// Several TimingRequest variants and TimeBuffer/AsyncTimeBuffer methods are
// planned for future use (RDTSC-based timing, slot statistics, async paths)
// but not yet wired into the runtime. Suppress dead-code warnings for this
// internal (pub(crate)) module.
#![allow(dead_code)]

use crate::utils_rdtsc::{cycles_to_ns, rdtsc};
use rapidhash::{HashMapExt, RapidHashMap};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct SlotStats {
    /// Maps task name to (worker_id, Duration) pairs for per-worker tracking
    pub task_times: RapidHashMap<String, Vec<(usize, Duration)>>,
    pub total_time: Duration,
    pub slot_id: usize,
    pub stream_count: usize,
}

impl SlotStats {
    pub fn new(slot_id: usize, stream_count: usize) -> Self {
        Self {
            task_times: RapidHashMap::new(),
            total_time: Duration::ZERO,
            slot_id,
            stream_count,
        }
    }

    /// Add a task timing measurement with worker ID tracking
    pub fn add_task_time(&mut self, task_name: &str, worker_id: usize, duration: Duration) {
        self.task_times
            .entry(task_name.to_string())
            .or_insert_with(Vec::new)
            .push((worker_id, duration));
    }

    pub fn get_task_total_time(&self, task_name: &str) -> Duration {
        self.task_times
            .get(task_name)
            .map(|times| times.iter().map(|(_, d)| d).sum())
            .unwrap_or(Duration::ZERO)
    }

    pub fn get_task_avg_time(&self, task_name: &str) -> Duration {
        if let Some(times) = self.task_times.get(task_name) {
            if !times.is_empty() {
                return times.iter().map(|(_, d)| d).sum::<Duration>() / times.len() as u32;
            }
        }
        Duration::ZERO
    }
}

#[derive(Debug, Clone)]
pub enum TimingMethod {
    Instant(Instant),
    Rdtsc(u64),
}

/// Asynchronous task timing request
#[derive(Debug)]
pub enum TimingRequest {
    AddTaskTime {
        slot_id: usize,
        task_name: String,
        worker_id: usize,
        duration: Duration,
    },
    AddTaskTimeCycles {
        slot_id: usize,
        task_name: String,
        worker_id: usize,
        cycles: u64,
    },
    AddTaskTimeInstant {
        slot_id: usize,
        task_name: String,
        worker_id: usize,
        start: Instant,
        end: Instant,
    },
    AddTaskTimeRdtsc {
        slot_id: usize,
        task_name: String,
        worker_id: usize,
        start_cycles: u64,
        end_cycles: u64,
    },
    StartSlotProcessing {
        slot_id: usize,
        /// Pre-captured start time at call site for precise timing
        start_time: TimingMethod,
    },
    FinishSlotProcessing {
        slot_id: usize,
        /// Pre-captured end time at call site for precise timing
        end_time: TimingMethod,
        response_tx: mpsc::Sender<SlotStats>,
    },
    GetSlotStatistics {
        slot_id: usize,
        response_tx: mpsc::Sender<Vec<SlotStats>>,
    },
    PrintStats {
        bench_name: String,
        out_file: Option<String>,
        exclude_streams: usize,
        response_tx: mpsc::Sender<()>,
    },
    Shutdown,
}

/// Asynchronous TimeBuffer controller
pub struct AsyncTimeBuffer {
    request_tx: Sender<TimingRequest>,
    controller_handle: Option<JoinHandle<()>>,
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
    pub fn finish_slot_processing(&self, slot_id: usize, use_rdtsc: bool) -> Result<SlotStats, &'static str> {
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
    pub fn finish_slot_processing_with_time(&mut self, slot_id: usize, end_time: TimingMethod) -> SlotStats {
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

        let avg_latency_us: f64 =
            sorted_latencies.iter().sum::<f64>() / num_included as f64;

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
        let (node_stats_map, worker_busy_us) =
            compute_node_stats(&included_tasks, total_wall_us);

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
            .map(|s| if num_included > 0 { s.invocations / num_included } else { 0 })
            .sum();

        // Max factor among critical-path nodes (key input for tile-size suggestion).
        let max_cp_factor: usize = critical_path_node_set
            .iter()
            .filter_map(|name| node_stats_map.get(*name))
            .map(|s| if num_included > 0 { s.invocations / num_included } else { 0 })
            .max()
            .unwrap_or(0);

        // Scheduling overhead: gap between measured latency and critical-path compute.
        let cp_exec_us = critical_path
            .as_ref()
            .map_or(0.0, |cp| (cp.estimated_latency_us * 100.0).round() / 100.0);
        let overhead_us = (avg_latency_us - critical_path.as_ref().map_or(0.0, |cp| cp.estimated_latency_us)).max(0.0);
        let overhead_pct = if avg_latency_us > 0.0 { overhead_us / avg_latency_us * 100.0 } else { 0.0 };
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

// ── Module-level structs used by write_json_report helpers ───────────────────

/// Per-node aggregated timing statistics used in JSON reports.
struct NodeStats {
    invocations: usize,
    mean_exec_us: f64,
    p99_exec_us: f64,
    total_exec_us: f64,
    pct_of_total: f64,
}

/// Critical-path result for a DAG, computed via Kahn's topo-sort + DP.
struct ReportCriticalPath {
    length_nodes: usize,
    estimated_latency_us: f64,
    nodes: Vec<String>,
}

// ── Free helpers for print_stats ─────────────────────────────────────────────

/// Collect per-stream total times and per-stream task maps from worker slots,
/// and collect system-thread task times grouped by slot.
///
/// Returns `(global_total_times, per_stream_task_data, system_task_data_by_slot, total_streams)`.
fn collect_print_stats_data(
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

    for slot_id in 0..slots {
        let slot_stats = &slot_statistics[slot_id];
        if slot_stats.is_empty() {
            continue;
        }

        if slot_id >= system_slots_start {
            // Collect system thread task data by slot
            let slot_task_data = system_task_data_by_slot
                .entry(slot_id)
                .or_insert_with(std::collections::HashMap::new);

            for stats in slot_stats {
                for (task_name, times) in &stats.task_times {
                    let task_durations = slot_task_data
                        .entry(task_name.clone())
                        .or_insert_with(Vec::new);
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
fn aggregate_task_data(
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
            let task_durations = global_task_data
                .entry(task_name.clone())
                .or_insert_with(Vec::new);

            for (worker_id, duration) in times {
                task_durations.push(*duration);

                let worker_counts = global_per_worker_counts
                    .entry(task_name.clone())
                    .or_insert_with(std::collections::HashMap::new);
                *worker_counts.entry(*worker_id).or_insert(0) += 1;

                let worker_totals = global_per_worker_totals
                    .entry(task_name.clone())
                    .or_insert_with(std::collections::HashMap::new);
                *worker_totals.entry(*worker_id).or_insert(Duration::ZERO) += *duration;
            }
        }
    }

    (global_task_data, global_per_worker_counts, global_per_worker_totals)
}

/// Format the header and global timing statistics block (stream counts, averages, min/max).
fn format_timing_summary(
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
    for slot_id in 0..worker_slots_end {
        let stream_count = slot_statistics[slot_id].len();
        slot_stream_items.push(format!("Slot {}: {}", slot_id, stream_count));
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
            global_total_times.iter().skip(excluded_count).copied().collect()
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
fn format_per_task_analysis(
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
fn format_system_thread_stats(
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
fn collect_report_stream_data(
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

    for slot_id in 0..worker_slots_end {
        for stats in &slot_statistics[slot_id] {
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
fn compute_node_stats(
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
        let p99_idx = ((0.99 * (sorted_us.len() as f64 - 1.0)).round() as usize)
            .min(sorted_us.len() - 1);
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
fn compute_critical_path_report(
    graph_edges: &[(String, Vec<String>)],
    node_stats_map: &std::collections::HashMap<String, NodeStats>,
) -> Option<ReportCriticalPath> {
    if graph_edges.is_empty() {
        return None;
    }

    let mut name_to_idx: std::collections::HashMap<&str, usize> =
        std::collections::HashMap::new();
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
fn generate_optimization_suggestions(
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
        let max_util = worker_busy_pct.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        if max_util < 50.0 {
            if let Some(cp) = critical_path {
                if cp.length_nodes > 10 {
                    suggestions.push(json!({
                        "priority": 4,
                        "category": "parallelism",
                        "description": format!(
                            "Peak worker utilization is only {:.0}% — workers are mostly idle \
                             because the critical path serialises execution. SynStream workers \
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
fn build_json_report_value(
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

/// Wrapper that provides both synchronous and asynchronous TimeBuffer interfaces
pub struct TimeBufferManager {
    async_buffer: Option<AsyncTimeBuffer>,
    sync_buffer: Option<Arc<Mutex<TimeBuffer>>>,
    is_async: bool,
    use_rdtsc: bool, // Store timing method for async mode
}

impl TimeBufferManager {
    /// Create a new synchronous TimeBuffer manager
    pub fn new_sync(slots: usize, system_threads: usize, use_rdtsc: bool) -> Self {
        Self {
            async_buffer: None,
            sync_buffer: Some(Arc::new(Mutex::new(TimeBuffer::new(
                slots,
                system_threads,
                use_rdtsc,
            )))),
            is_async: false,
            use_rdtsc,
        }
    }

    /// Create a new asynchronous TimeBuffer manager
    pub fn new_async(slots: usize, system_threads: usize, use_rdtsc: bool) -> Self {
        Self {
            async_buffer: Some(AsyncTimeBuffer::new(slots, system_threads, use_rdtsc)),
            sync_buffer: None,
            is_async: true,
            use_rdtsc,
        }
    }

    /// Add task time - async if manager is async, sync if manager is sync
    pub fn add_task_time(
        &self,
        slot_id: usize,
        task_name: &str,
        worker_id: usize,
        duration: Duration,
    ) {
        if self.is_async {
            if let Some(ref async_buf) = self.async_buffer {
                async_buf.add_task_time_async(slot_id, task_name, worker_id, duration);
            }
        } else {
            if let Some(ref sync_buf) = self.sync_buffer {
                if let Ok(mut buf) = sync_buf.lock() {
                    buf.add_task_time(slot_id, task_name, worker_id, duration);
                }
            }
        }
    }

    /// Start slot processing
    /// In async mode, timestamp is captured at call site for precise timing
    pub fn start_slot_processing(&self, slot_id: usize) {
        if self.is_async {
            if let Some(ref async_buf) = self.async_buffer {
                async_buf.start_slot_processing_async(slot_id, self.use_rdtsc);
            }
        } else {
            if let Some(ref sync_buf) = self.sync_buffer {
                if let Ok(mut buf) = sync_buf.lock() {
                    buf.start_slot_processing(slot_id);
                }
            }
        }
    }

    /// Finish slot processing (always blocking to return result)
    /// In async mode, end timestamp is captured at call site for precise timing
    pub fn finish_slot_processing(&self, slot_id: usize) -> Result<SlotStats, &'static str> {
        if self.is_async {
            if let Some(ref async_buf) = self.async_buffer {
                async_buf.finish_slot_processing(slot_id, self.use_rdtsc)
            } else {
                Err("Async buffer not initialized")
            }
        } else {
            if let Some(ref sync_buf) = self.sync_buffer {
                if let Ok(mut buf) = sync_buf.lock() {
                    Ok(buf.finish_slot_processing(slot_id))
                } else {
                    Err("Failed to acquire sync buffer lock")
                }
            } else {
                Err("Sync buffer not initialized")
            }
        }
    }

    /// Get timing method for measurements (synchronous for both async and sync modes)
    pub fn measure_time(&self) -> TimingMethod {
        if self.use_rdtsc {
            TimingMethod::Rdtsc(rdtsc())
        } else {
            TimingMethod::Instant(Instant::now())
        }
    }

    /// Measure duration between two timing methods (synchronous for both async and sync modes)
    pub fn measure_duration(&self, start_time: TimingMethod, end_time: TimingMethod) -> Duration {
        // Duration calculation is fast and stateless, so we can do it directly
        // for both async and sync modes without going through the controller
        match (start_time, end_time) {
            (TimingMethod::Instant(start), TimingMethod::Instant(end)) => end.duration_since(start),
            (TimingMethod::Rdtsc(start_cycles), TimingMethod::Rdtsc(end_cycles)) => {
                let cycles = end_cycles.saturating_sub(start_cycles);
                Duration::from_nanos(cycles_to_ns(cycles) as u64)
            }
            _ => panic!("Cannot mix Instant and Rdtsc timing methods"),
        }
    }

    /// Print stats with worker accounting - blocking in both async and sync modes
    pub fn print_stats(&self, bench_name: &str, out_file: Option<&str>, exclude_streams: usize) {
        if self.is_async {
            if let Some(ref async_buf) = self.async_buffer {
                if let Err(e) = async_buf.print_stats(bench_name, out_file, exclude_streams) {
                    eprintln!("Failed to print stats: {}", e);
                }
            }
        } else {
            if let Some(ref sync_buf) = self.sync_buffer {
                if let Ok(buf) = sync_buf.lock() {
                    buf.print_stats(bench_name, out_file, exclude_streams);
                }
            }
        }
    }

    /// Write a JSON performance report — blocking in both async and sync modes.
    pub fn write_json_report(
        &self,
        graph_edges: &[(String, Vec<String>)],
        path: &str,
        exclude_streams: usize,
    ) {
        if self.is_async {
            if let Some(ref async_buf) = self.async_buffer {
                if let Err(e) = async_buf.write_json_report(graph_edges, path, exclude_streams) {
                    eprintln!("Failed to write JSON report: {}", e);
                }
            }
        } else if let Some(ref sync_buf) = self.sync_buffer {
            if let Ok(buf) = sync_buf.lock() {
                buf.write_json_report(graph_edges, path, exclude_streams);
            }
        }
    }

    /// Shutdown async controller if using async mode
    pub fn shutdown(mut self) {
        if let Some(async_buf) = self.async_buffer.take() {
            async_buf.shutdown();
        }
    }
}

impl Drop for TimeBufferManager {
    fn drop(&mut self) {
        // If we have an async buffer, shut it down explicitly and wait
        if let Some(async_buf) = self.async_buffer.take() {
            // Send shutdown and immediately wait for thread to finish
            let _ = async_buf.request_tx.send(TimingRequest::Shutdown);

            // Join the controller thread to ensure clean shutdown
            if let Some(handle) = async_buf.controller_handle {
                let _ = handle.join();
            }
            // time_buffer (Arc<Mutex>) will be dropped automatically
        }
    }
}
