// Several TimingRequest variants and TimeBuffer/AsyncTimeBuffer methods are
// planned for future use (RDTSC-based timing, slot statistics, async paths)
// but not yet wired into the runtime. Suppress dead-code warnings for this
// internal (pub(crate)) module.
#![allow(dead_code)]

mod buffer;
mod report;

pub use buffer::{AsyncTimeBuffer, TimeBuffer};

use crate::utils_rdtsc::{cycles_to_ns, rdtsc};
use rapidhash::{HashMapExt, RapidHashMap};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
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
        } else if let Some(ref sync_buf) = self.sync_buffer {
            if let Ok(mut buf) = sync_buf.lock() {
                buf.add_task_time(slot_id, task_name, worker_id, duration);
            }
        }
    }

    /// Start slot processing with precise timestamp
    pub fn start_slot_processing(&self, slot_id: usize) {
        if self.is_async {
            if let Some(ref async_buf) = self.async_buffer {
                async_buf.start_slot_processing_async(slot_id, self.use_rdtsc);
            }
        } else if let Some(ref sync_buf) = self.sync_buffer {
            if let Ok(mut buf) = sync_buf.lock() {
                buf.start_slot_processing(slot_id);
            }
        }
    }

    /// Finish slot processing (blocking in async mode)
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
        match (start_time, end_time) {
            (TimingMethod::Instant(start), TimingMethod::Instant(end)) => end.duration_since(start),
            (TimingMethod::Rdtsc(start_cycles), TimingMethod::Rdtsc(end_cycles)) => {
                let cycles = end_cycles.saturating_sub(start_cycles);
                Duration::from_nanos(cycles_to_ns(cycles) as u64)
            }
            _ => panic!("Cannot mix Instant and Rdtsc timing methods"),
        }
    }

    /// Print statistics to stdout or file
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
        if let Some(async_buf) = self.async_buffer.take() {
            async_buf.shutdown();
        }
    }
}
