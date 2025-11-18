use crate::utils_rdtsc::{cycles_to_ns, rdtsc};
use rapidhash::{HashMapExt, RapidHashMap};
use std::fs::File;
use std::io::Write;
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
    },
    FinishSlotProcessing {
        slot_id: usize,
        response_tx: mpsc::Sender<SlotStats>,
    },
    GetSlotStatistics {
        slot_id: usize,
        response_tx: mpsc::Sender<Vec<SlotStats>>,
    },
    PrintStats {
        bench_name: String,
        out_file: Option<String>,
    },
    Shutdown,
}

/// Asynchronous TimeBuffer controller
pub struct AsyncTimeBuffer {
    request_tx: Sender<TimingRequest>,
    controller_handle: Option<JoinHandle<()>>,
}

impl AsyncTimeBuffer {
    /// Create a new AsyncTimeBuffer with a background controller
    pub fn new(slots: usize, use_rdtsc: bool) -> Self {
        let (request_tx, request_rx) = mpsc::channel();

        let controller_handle = thread::spawn(move || {
            let mut time_buffer = TimeBuffer::new(slots, use_rdtsc);

            while let Ok(request) = request_rx.recv() {
                match request {
                    TimingRequest::AddTaskTime {
                        slot_id,
                        task_name,
                        worker_id,
                        duration,
                    } => {
                        time_buffer.add_task_time(slot_id, &task_name, worker_id, duration);
                    }
                    TimingRequest::AddTaskTimeCycles {
                        slot_id,
                        task_name,
                        worker_id,
                        cycles,
                    } => {
                        time_buffer.add_task_time_cycles(slot_id, &task_name, worker_id, cycles);
                    }
                    TimingRequest::AddTaskTimeInstant {
                        slot_id,
                        task_name,
                        worker_id,
                        start,
                        end,
                    } => {
                        time_buffer
                            .add_task_time_instant(slot_id, &task_name, worker_id, start, end);
                    }
                    TimingRequest::AddTaskTimeRdtsc {
                        slot_id,
                        task_name,
                        worker_id,
                        start_cycles,
                        end_cycles,
                    } => {
                        time_buffer.add_task_time_rdtsc(
                            slot_id,
                            &task_name,
                            worker_id,
                            start_cycles,
                            end_cycles,
                        );
                    }
                    TimingRequest::StartSlotProcessing { slot_id } => {
                        time_buffer.start_slot_processing(slot_id);
                    }
                    TimingRequest::FinishSlotProcessing {
                        slot_id,
                        response_tx,
                    } => {
                        let stats = time_buffer.finish_slot_processing(slot_id);
                        let _ = response_tx.send(stats); // Ignore send errors if receiver dropped
                    }
                    TimingRequest::GetSlotStatistics {
                        slot_id,
                        response_tx,
                    } => {
                        let stats = time_buffer.get_slot_statistics(slot_id).clone();
                        let _ = response_tx.send(stats);
                    }
                    TimingRequest::PrintStats {
                        bench_name,
                        out_file,
                    } => {
                        time_buffer.print_stats(&bench_name, out_file.as_deref());
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

    /// Start slot processing asynchronously
    pub fn start_slot_processing_async(&self, slot_id: usize) {
        let request = TimingRequest::StartSlotProcessing { slot_id };
        if let Err(_) = self.request_tx.send(request) {
            eprintln!("Warning: Failed to send timing request - controller may have shut down");
        }
    }

    /// Finish slot processing and get stats (blocking - returns result)
    pub fn finish_slot_processing(&self, slot_id: usize) -> Result<SlotStats, &'static str> {
        let (response_tx, response_rx) = mpsc::channel();
        let request = TimingRequest::FinishSlotProcessing {
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

    /// Print statistics asynchronously
    pub fn print_stats_async(&self, bench_name: &str, out_file: Option<&str>) {
        let request = TimingRequest::PrintStats {
            bench_name: bench_name.to_string(),
            out_file: out_file.map(|s| s.to_string()),
        };
        if let Err(_) = self.request_tx.send(request) {
            eprintln!("Warning: Failed to send timing request - controller may have shut down");
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
    pub fn new(slots: usize, use_rdtsc: bool) -> Self {
        TimeBuffer {
            slots,
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
    pub fn finish_slot_processing(&mut self, slot_id: usize) -> SlotStats {
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

        let total_time = match start_time {
            TimingMethod::Instant(start) => Instant::now().duration_since(start),
            TimingMethod::Rdtsc(start_cycles) => {
                let end_cycles = rdtsc();
                let cycles = end_cycles.saturating_sub(start_cycles);
                Duration::from_nanos(cycles_to_ns(cycles) as u64)
            }
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
    pub fn print_stats(&self, bench_name: &str, out_file: Option<&str>) {
        let filler = "****************";
        let mut output_buffer = format!("Time Statistics for {}\n", bench_name);
        output_buffer.push_str(&format!("Total Slots: {}\n", self.slots));
        output_buffer.push_str(&format!(
            "Timing Method: {}\n",
            if self.use_rdtsc { "RDTSC" } else { "Instant" }
        ));

        // Statistics from all slots (excluding runtime slot)
        let mut global_total_times: Vec<Duration> = Vec::new();
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

        // Separate storage for runtime task data
        let mut runtime_task_data: std::collections::HashMap<String, Vec<Duration>> =
            std::collections::HashMap::new();

        let mut total_streams = 0;
        let runtime_slot_id = if self.slots > 0 { self.slots - 1 } else { 0 };

        for slot_id in 0..self.slots {
            let slot_stats = &self.slot_statistics[slot_id];
            if slot_stats.is_empty() {
                continue;
            }

            // Skip the runtime slot
            if slot_id == runtime_slot_id {
                // Collect runtime task data separately
                for stats in slot_stats {
                    for (task_name, times) in &stats.task_times {
                        let task_durations = runtime_task_data
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

            // Collect total times from all streams in this slot
            for stats in slot_stats {
                global_total_times.push(stats.total_time);

                // Collect task data from all streams in this slot
                for (task_name, times) in &stats.task_times {
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
        }

        // Print aggregated statistics header
        output_buffer.push_str(&format!("{}\nAggregated Statistics (All Slots):\n", filler));
        output_buffer.push_str(&format!("  Total Streams Processed: {}\n", total_streams));

        // Print per-slot stream breakdown
        output_buffer.push_str("  Streams per Slot: ");
        let mut slot_stream_items: Vec<String> = Vec::new();
        for slot_id in 0..self.slots {
            let slot_stats = &self.slot_statistics[slot_id];
            if slot_id == self.slots - 1 && !slot_stats.is_empty() {
                // Skip runtime slot
                continue;
            }
            let stream_count = slot_stats.len();
            slot_stream_items.push(format!("Slot {}: {}", slot_id, stream_count));
        }
        output_buffer.push_str(&format!("{}\n", slot_stream_items.join(", ")));

        // Calculate statistics for total times
        if !global_total_times.is_empty() {
            let global_total: Duration = global_total_times.iter().sum();
            let avg_total_time = global_total / global_total_times.len() as u32;
            let min_total_time = global_total_times.iter().min().unwrap();
            let max_total_time = global_total_times.iter().max().unwrap();

            output_buffer.push_str(&format!("  Total Runtime: {:.4?}\n", global_total));
            output_buffer.push_str(&format!("  Avg Time Per Stream: {:.4?}\n", avg_total_time));
            output_buffer.push_str(&format!(
                "  Min/Max Per Stream: {:.4?} / {:.4?}\n",
                min_total_time, max_total_time
            ));
        }

        // Print per-task analysis for all slots combined
        output_buffer.push_str(&format!("{}\nPer-Task Analysis (Aggregated):\n", filler));

        let mut sorted_tasks: Vec<_> = global_task_data.keys().cloned().collect();
        sorted_tasks.sort();

        for task_name in sorted_tasks {
            if let Some(task_times) = global_task_data.get(&task_name) {
                if task_times.is_empty() {
                    continue;
                }

                output_buffer.push_str(&format!("  {}\n", filler));

                let total_executions = task_times.len();
                let total_time: Duration = task_times.iter().sum();

                let avg_time = if total_streams > 0 {
                    total_time / total_streams as u32
                } else {
                    Duration::ZERO
                };

                let avg_task = total_time / total_executions as u32;
                let min_time = task_times.iter().min().unwrap();
                let max_time = task_times.iter().max().unwrap();

                let worker_counts = global_per_worker_counts.get(&task_name).unwrap();
                let worker_totals = global_per_worker_totals.get(&task_name).unwrap();

                output_buffer.push_str(&format!(
                    "  Task '{}' - Workers: {}, Total Executions: {}\n",
                    task_name,
                    worker_counts.len(),
                    total_executions
                ));

                output_buffer.push_str(&format!(
                    "    Timing - Avg/Stream: {:.4?}, Avg/Task: {:.4?}, Min: {:.4?}, Max: {:.4?}, Total: {:.4?}\n",
                    avg_time, avg_task, min_time, max_time, total_time
                ));

                // Tasks and time per worker combined
                output_buffer.push_str("    Worker Summary: ");
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
                output_buffer.push_str(&format!("{}\n", worker_items.join(", ")));
            }
        }

        // Print runtime task analysis (separate section)
        if !runtime_task_data.is_empty() {
            output_buffer.push_str(&format!("{}\nRuntime Tasks (Final Slot):\n", filler));

            let mut sorted_runtime_tasks: Vec<_> = runtime_task_data.keys().cloned().collect();
            sorted_runtime_tasks.sort();

            for task_name in sorted_runtime_tasks {
                if let Some(task_times) = runtime_task_data.get(&task_name) {
                    if task_times.is_empty() {
                        continue;
                    }

                    let total_executions = task_times.len();
                    let min_time = task_times.iter().min().unwrap();
                    let max_time = task_times.iter().max().unwrap();
                    let total_time: Duration = task_times.iter().sum();
                    let avg_time = total_time / total_executions as u32;

                    output_buffer.push_str(&format!(
                        "  Task '{}' - Total Executions: {}, Avg: {:.4?}, Min: {:.4?}, Max: {:.4?}, Total: {:.4?}\n",
                        task_name, total_executions, avg_time, min_time, max_time, total_time
                    ));
                }
            }
        }

        // Print per-slot summary
        output_buffer.push_str(&format!("{}\nPer-Slot Summary:\n", filler));
        for slot_id in 0..self.slots {
            let slot_stats = &self.slot_statistics[slot_id];
            if slot_stats.is_empty() {
                output_buffer.push_str(&format!("  Slot {}: No data\n", slot_id));
                continue;
            }

            let slot_header = if slot_id == self.slots - 1 {
                "SynStream Runtime".to_string()
            } else {
                format!("Slot {}", slot_id)
            };

            output_buffer.push_str(&format!(
                "  {} - Streams: {}\n",
                slot_header,
                slot_stats.len()
            ));
        }

        if let Some(out_file) = out_file {
            std::fs::write(out_file, &output_buffer).expect("Unable to write file");
        } else {
            print!("{}", output_buffer);
        }
    }

    /// Export detailed statistics to a file
    pub fn export_detailed_stats(&self, bench_name: &str, out_file: &str) {
        let filler = "****************";
        let mut output_buffer = format!("Detailed Time Statistics for {}\n", bench_name);
        output_buffer.push_str(&format!("Total Slots: {}\n", self.slots));
        output_buffer.push_str(&format!(
            "Timing Method: {}\n\n",
            if self.use_rdtsc { "RDTSC" } else { "Instant" }
        ));

        for slot_id in 0..self.slots {
            let slot_stats = &self.slot_statistics[slot_id];
            if slot_stats.is_empty() {
                continue;
            }

            output_buffer.push_str(&format!("{}\nSlot {}\n", filler, slot_id));

            for (cycle_idx, stats) in slot_stats.iter().enumerate() {
                output_buffer.push_str(&format!(
                    "  Stream {}: Total Time: {:.4?}\n",
                    cycle_idx, stats.total_time
                ));

                for (task_name, times) in &stats.task_times {
                    output_buffer.push_str(&format!("    Task '{}': [", task_name));
                    for (i, (worker_id, time)) in times.iter().enumerate() {
                        let label = if *worker_id == usize::MAX {
                            "runtime".to_string()
                        } else {
                            format!("W{}", worker_id)
                        };
                        if i == times.len() - 1 {
                            output_buffer.push_str(&format!("({}: {:.4?})", label, time));
                        } else {
                            output_buffer.push_str(&format!("({}: {:.4?}), ", label, time));
                        }
                    }
                    output_buffer.push_str("]\n");
                }
            }
        }

        let mut file = File::create(out_file).expect("Unable to create file");
        file.write_all(output_buffer.as_bytes())
            .expect("Unable to write to file");
    }

    /// Clear all statistics and reset the buffer
    pub fn reset(&mut self) {
        self.slot_start_times = vec![None; self.slots];
        self.current_slot_tasks = vec![RapidHashMap::new(); self.slots];
        self.slot_statistics = vec![Vec::new(); self.slots];
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

/// Wrapper that provides both synchronous and asynchronous TimeBuffer interfaces
pub struct TimeBufferManager {
    async_buffer: Option<AsyncTimeBuffer>,
    sync_buffer: Option<Arc<Mutex<TimeBuffer>>>,
    is_async: bool,
    use_rdtsc: bool, // Store timing method for async mode
}

impl TimeBufferManager {
    /// Create a new synchronous TimeBuffer manager
    pub fn new_sync(slots: usize, use_rdtsc: bool) -> Self {
        Self {
            async_buffer: None,
            sync_buffer: Some(Arc::new(Mutex::new(TimeBuffer::new(slots, use_rdtsc)))),
            is_async: false,
            use_rdtsc,
        }
    }

    /// Create a new asynchronous TimeBuffer manager
    pub fn new_async(slots: usize, use_rdtsc: bool) -> Self {
        Self {
            async_buffer: Some(AsyncTimeBuffer::new(slots, use_rdtsc)),
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
    pub fn start_slot_processing(&self, slot_id: usize) {
        if self.is_async {
            if let Some(ref async_buf) = self.async_buffer {
                async_buf.start_slot_processing_async(slot_id);
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
    pub fn finish_slot_processing(&self, slot_id: usize) -> Result<SlotStats, &'static str> {
        if self.is_async {
            if let Some(ref async_buf) = self.async_buffer {
                async_buf.finish_slot_processing(slot_id)
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
        if self.is_async {
            // For async mode, we can measure time directly without going through the controller
            // since timing measurement is fast and doesn't require buffer access
            if self.use_rdtsc {
                TimingMethod::Rdtsc(rdtsc())
            } else {
                TimingMethod::Instant(Instant::now())
            }
        } else {
            // For sync mode, use the buffer's measure_time method
            if let Some(ref sync_buf) = self.sync_buffer {
                if let Ok(buf) = sync_buf.lock() {
                    buf.measure_time()
                } else {
                    // Fallback if lock fails
                    if self.use_rdtsc {
                        TimingMethod::Rdtsc(rdtsc())
                    } else {
                        TimingMethod::Instant(Instant::now())
                    }
                }
            } else {
                // Fallback if buffer not initialized
                if self.use_rdtsc {
                    TimingMethod::Rdtsc(rdtsc())
                } else {
                    TimingMethod::Instant(Instant::now())
                }
            }
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

    /// Print stats with worker accounting - async if possible, sync otherwise
    pub fn print_stats(&self, bench_name: &str, out_file: Option<&str>) {
        if self.is_async {
            if let Some(ref async_buf) = self.async_buffer {
                async_buf.print_stats_async(bench_name, out_file);
            }
        } else {
            if let Some(ref sync_buf) = self.sync_buffer {
                if let Ok(buf) = sync_buf.lock() {
                    buf.print_stats(bench_name, out_file);
                }
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
        // If we have an async buffer, we need to shut it down
        // We can't call shutdown() here because it consumes self, but we need to clean up
        if let Some(ref async_buf) = self.async_buffer {
            let _ = async_buf.request_tx.send(TimingRequest::Shutdown);
        }
    }
}

/*
Example Usage:

=== SYNCHRONOUS (Original) ===
// Create a synchronous TimeBuffer for 4 slots using rdtsc timing
let mut time_buffer = TimeBuffer::new(4, true);

// Start processing for slot 0
time_buffer.start_slot_processing(0);

// Add task timings during processing (BLOCKING)
time_buffer.add_task_time(0, "fft", Duration::from_micros(100));
time_buffer.add_task_time(0, "beam", Duration::from_micros(50));

// Finish processing and get stats
let slot_stats = time_buffer.finish_slot_processing(0);
println!("Slot 0 total time: {:?}", slot_stats.total_time);

=== ASYNCHRONOUS (New) ===
// Create an asynchronous TimeBuffer for 4 slots using rdtsc timing
let async_buffer = AsyncTimeBuffer::new(4, true);

// Start processing for slot 0 (NON-BLOCKING)
async_buffer.start_slot_processing_async(0);

// Add task timings during processing (NON-BLOCKING)
async_buffer.add_task_time_async(0, "fft", Duration::from_micros(100));
async_buffer.add_task_time_async(0, "beam", Duration::from_micros(50));

// Using rdtsc cycles (NON-BLOCKING)
let start_cycles = rdtsc();
// ... do some work ...
let end_cycles = rdtsc();
async_buffer.add_task_time_rdtsc_async(0, "decode", start_cycles, end_cycles);

// Using Instant (NON-BLOCKING)
let start = Instant::now();
// ... do some work ...
let end = Instant::now();
async_buffer.add_task_time_instant_async(0, "process", start, end);

// Finish processing and get stats (BLOCKING - returns result)
let slot_stats = async_buffer.finish_slot_processing(0).unwrap();
println!("Slot 0 total time: {:?}", slot_stats.total_time);

// Print stats asynchronously (NON-BLOCKING)
async_buffer.print_stats_async("My Benchmark", Some("stats.txt"));

// Shutdown the controller when done
async_buffer.shutdown();

=== UNIFIED MANAGER (Recommended) ===
// Create manager for async mode
let manager = TimeBufferManager::new_async(4, true);

// Or create manager for sync mode
// let manager = TimeBufferManager::new_sync(4, true);

// Use the same interface regardless of sync/async mode
manager.start_slot_processing(0);
manager.add_task_time(0, "fft", Duration::from_micros(100)); // Non-blocking if async

// Fast timing measurements are synchronous for both modes
let start_time = manager.measure_time(); // Always synchronous and fast
// ... do some work ...
let end_time = manager.measure_time();
let duration = manager.measure_duration(start_time, end_time); // Always synchronous
manager.add_task_time(0, "measured_task", duration);

// This is always blocking to return the result
let slot_stats = manager.finish_slot_processing(0).unwrap();
println!("Slot 0 total time: {:?}", slot_stats.total_time);

// Print stats (non-blocking if async)
manager.print_stats("My Benchmark", Some("stats.txt"));

// Shutdown (only needed for async mode, but safe to call for sync too)
manager.shutdown();
*/
