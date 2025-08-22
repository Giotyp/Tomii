use crate::utils_rdtsc::{cycles_to_ns, rdtsc};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct SlotStats {
    pub task_times: HashMap<String, Vec<Duration>>,
    pub total_time: Duration,
    pub slot_id: usize,
    pub stream_count: usize,
}

impl SlotStats {
    pub fn new(slot_id: usize, stream_count: usize) -> Self {
        Self {
            task_times: HashMap::new(),
            total_time: Duration::ZERO,
            slot_id,
            stream_count,
        }
    }

    pub fn add_task_time(&mut self, task_name: &str, duration: Duration) {
        self.task_times
            .entry(task_name.to_string())
            .or_insert_with(Vec::new)
            .push(duration);
    }

    pub fn get_task_total_time(&self, task_name: &str) -> Duration {
        self.task_times
            .get(task_name)
            .map(|times| times.iter().sum())
            .unwrap_or(Duration::ZERO)
    }

    pub fn get_task_avg_time(&self, task_name: &str) -> Duration {
        if let Some(times) = self.task_times.get(task_name) {
            if !times.is_empty() {
                return times.iter().sum::<Duration>() / times.len() as u32;
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

pub struct TimeBuffer {
    slots: usize,
    // Current timing state per slot
    slot_start_times: Vec<Option<TimingMethod>>,
    // Current task times for each slot (accumulated during processing)
    current_slot_tasks: Vec<HashMap<String, Vec<Duration>>>,
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
            current_slot_tasks: vec![HashMap::new(); slots],
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
        // Clear any previous task times for this slot
        self.current_slot_tasks[slot_id].clear();
    }

    /// Add a task timing measurement to a specific slot
    pub fn add_task_time(&mut self, slot_id: usize, task_name: &str, duration: Duration) {
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
            .push(duration);
    }

    /// Add a task timing measurement using rdtsc cycles
    pub fn add_task_time_cycles(&mut self, slot_id: usize, task_name: &str, cycles: u64) {
        let duration = Duration::from_nanos(cycles_to_ns(cycles) as u64);
        self.add_task_time(slot_id, task_name, duration);
    }

    /// Add a task timing measurement using start/end Instant
    pub fn add_task_time_instant(
        &mut self,
        slot_id: usize,
        task_name: &str,
        start: Instant,
        end: Instant,
    ) {
        let duration = end.duration_since(start);
        self.add_task_time(slot_id, task_name, duration);
    }

    /// Add a task timing measurement using start/end rdtsc cycles
    pub fn add_task_time_rdtsc(
        &mut self,
        slot_id: usize,
        task_name: &str,
        start_cycles: u64,
        end_cycles: u64,
    ) {
        let cycles = end_cycles.saturating_sub(start_cycles);
        self.add_task_time_cycles(slot_id, task_name, cycles);
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

        let start_time = self.slot_start_times[slot_id]
            .take()
            .expect("Slot processing was never started or already finished");

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
            for &time in times {
                slot_stats.add_task_time(task_name, time);
            }
        }

        // Store the completed slot stats
        self.slot_statistics[slot_id].push(slot_stats.clone());

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

    /// Print comprehensive statistics for all slots
    pub fn print_stats(&self, bench_name: &str, out_file: Option<&str>) {
        let filler = "****************";
        let mut output_buffer = format!("Time Statistics for {}\n", bench_name);
        output_buffer.push_str(&format!("Total Slots: {}\n", self.slots));
        output_buffer.push_str(&format!(
            "Timing Method: {}\n",
            if self.use_rdtsc { "RDTSC" } else { "Instant" }
        ));

        for slot_id in 0..self.slots {
            let slot_stats = &self.slot_statistics[slot_id];
            if slot_stats.is_empty() {
                continue;
            }

            output_buffer.push_str(&format!("{}\nSlot {} Statistics:\n", filler, slot_id));
            output_buffer.push_str(&format!("  Completed Streams: {}\n", slot_stats.len()));

            // Calculate average total time per stream
            let total_times: Vec<Duration> = slot_stats.iter().map(|s| s.total_time).collect();
            let avg_total_time = if !total_times.is_empty() {
                total_times.iter().sum::<Duration>() / total_times.len() as u32
            } else {
                Duration::ZERO
            };

            let min_total_time = total_times.iter().min().unwrap_or(&Duration::ZERO);
            let max_total_time = total_times.iter().max().unwrap_or(&Duration::ZERO);

            output_buffer.push_str(&format!(
                "  Total Time - Avg: {:.4?}, Min: {:.4?}, Max: {:.4?}\n",
                avg_total_time, min_total_time, max_total_time
            ));

            // Collect all unique task names for this slot
            let mut all_tasks: std::collections::HashSet<String> = std::collections::HashSet::new();
            for stats in slot_stats {
                all_tasks.extend(stats.task_times.keys().cloned());
            }

            // Print task statistics
            for task_name in all_tasks.iter() {
                let mut all_task_times = Vec::new();
                for stats in slot_stats {
                    if let Some(times) = stats.task_times.get(task_name) {
                        all_task_times.extend(times.iter().cloned());
                    }
                }

                if !all_task_times.is_empty() {
                    let total_time = all_task_times.iter().sum::<Duration>();
                    let total_executions = all_task_times.len();
                    let avg_task_time = total_time / total_executions as u32;
                    let min_task_time = all_task_times.iter().min().unwrap();
                    let max_task_time = all_task_times.iter().max().unwrap();

                    output_buffer.push_str(&format!(
                        "  Task '{}' - Executions: {}, Avg: {:.4?}, Min: {:.4?}, Max: {:.4?}, Total: {:.4?}\n",
                        task_name, total_executions, avg_task_time, min_task_time, max_task_time, total_time
                    ));
                }
            }
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
                    for (i, time) in times.iter().enumerate() {
                        if i == times.len() - 1 {
                            output_buffer.push_str(&format!("{:.4?}", time));
                        } else {
                            output_buffer.push_str(&format!("{:.4?}, ", time));
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
        self.current_slot_tasks = vec![HashMap::new(); self.slots];
        self.slot_statistics = vec![Vec::new(); self.slots];
    }

    /// Get a summary of all slots
    pub fn get_summary(&self) -> HashMap<usize, (usize, Duration, Duration)> {
        let mut summary = HashMap::new();

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

/*
Example Usage:

// Create a TimeBuffer for 4 slots using rdtsc timing
let mut time_buffer = TimeBuffer::new(4, true);

// Start processing for slot 0
time_buffer.start_slot_processing(0);

// Add task timings during processing
time_buffer.add_task_time(0, "fft", Duration::from_micros(100));
time_buffer.add_task_time(0, "beam", Duration::from_micros(50));

// Or using rdtsc cycles
let start_cycles = rdtsc();
// ... do some work ...
let end_cycles = rdtsc();
time_buffer.add_task_time_rdtsc(0, "decode", start_cycles, end_cycles);

// Or using Instant
let start = Instant::now();
// ... do some work ...
let end = Instant::now();
time_buffer.add_task_time_instant(0, "process", start, end);

// Finish processing and get stats
let slot_stats = time_buffer.finish_slot_processing(0);
println!("Slot 0 total time: {:?}", slot_stats.total_time);

// Later, start a new stream for the same slot
time_buffer.start_slot_processing(0);
// ... repeat the process ...

// Print comprehensive statistics
time_buffer.print_stats("My Benchmark", Some("stats.txt"));

// Export detailed stats
time_buffer.export_detailed_stats("My Benchmark", "detailed_stats.txt");

// Get summary
let summary = time_buffer.get_summary();
for (slot_id, (cycles, avg_time, total_time)) in summary {
    println!("Slot {}: {} cycles, avg: {:?}, total: {:?}",
             slot_id, cycles, avg_time, total_time);
}
*/
