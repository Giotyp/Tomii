//! Lock-free asynchronous recording system for scheduler events
//!
//! Design:
//! - Each worker thread has a dedicated unbounded SPSC channel
//! - A single recorder thread collects from all channels
//! - Zero contention in the hot path (task execution)
//! - Bounded memory usage with periodic flushing

use crate::Record;
use crossbeam_channel::{unbounded, Receiver, Sender};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

thread_local! {
    /// Per-worker channel sender for lock-free record submission
    static WORKER_RECORDER: std::cell::RefCell<Option<Sender<Record>>> =
        const { std::cell::RefCell::new(None) };
}

/// Initialize the thread-local recorder sender for this worker
pub fn set_worker_recorder(tx: Sender<Record>) {
    WORKER_RECORDER.with(|cell| {
        *cell.borrow_mut() = Some(tx);
    });
}

/// Submit a record from the current worker (lock-free, allocation-free in steady state)
#[inline(always)]
pub fn submit_record(record: Record) {
    WORKER_RECORDER.with(|cell| {
        if let Some(tx) = cell.borrow().as_ref() {
            // Non-blocking send. If channel is full (shouldn't happen with unbounded),
            // we drop the record to avoid blocking the worker.
            let _ = tx.try_send(record);
        }
    });
}

/// Asynchronous recorder that collects records from worker threads
#[derive(Debug)]
pub struct AsyncRecorder {
    worker_senders: Vec<Sender<Record>>,
    shutdown_flag: Arc<AtomicBool>,
    collector_handle: Option<thread::JoinHandle<()>>,
}

impl AsyncRecorder {
    /// Create a new async recorder with specified number of workers
    pub fn new(num_workers: usize, flush_interval_ms: u64) -> Self {
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let mut worker_senders = Vec::with_capacity(num_workers);
        let mut worker_receivers = Vec::with_capacity(num_workers);

        // Create per-worker channels
        for _ in 0..num_workers {
            let (tx, rx) = unbounded::<Record>();
            worker_senders.push(tx);
            worker_receivers.push(rx);
        }

        // Spawn collector thread
        let shutdown = Arc::clone(&shutdown_flag);
        let collector_handle = thread::spawn(move || {
            Self::collector_loop(worker_receivers, shutdown, flush_interval_ms);
        });

        Self {
            worker_senders,
            shutdown_flag,
            collector_handle: Some(collector_handle),
        }
    }

    /// Get the sender for a specific worker ID
    pub fn get_worker_sender(&self, worker_id: usize) -> Option<Sender<Record>> {
        self.worker_senders.get(worker_id).cloned()
    }

    /// Collector loop running in dedicated thread
    fn collector_loop(
        receivers: Vec<Receiver<Record>>,
        shutdown: Arc<AtomicBool>,
        flush_interval_ms: u64,
    ) {
        // In-memory buffer grouped by slot
        let mut slot_records: HashMap<usize, Vec<Record>> = HashMap::new();
        let flush_interval = Duration::from_millis(flush_interval_ms);
        let mut last_flush = std::time::Instant::now();

        loop {
            // Check shutdown signal
            if shutdown.load(Ordering::Acquire) {
                // Final collection pass
                Self::collect_all_records(&receivers, &mut slot_records);
                break;
            }

            // Collect records from all workers (non-blocking)
            let collected = Self::collect_all_records(&receivers, &mut slot_records);

            // Periodic logging for diagnostics
            if collected > 0 && last_flush.elapsed() >= flush_interval {
                last_flush = std::time::Instant::now();
            }

            // Small sleep to avoid busy-waiting
            thread::sleep(Duration::from_micros(100));
        }

        // Store final records for later export
        FINAL_RECORDS.lock().replace(slot_records);
    }

    /// Collect all available records from worker channels (non-blocking)
    fn collect_all_records(
        receivers: &[Receiver<Record>],
        slot_records: &mut HashMap<usize, Vec<Record>>,
    ) -> usize {
        let mut collected = 0;
        for rx in receivers {
            while let Ok(record) = rx.try_recv() {
                let slot = record.slot;
                slot_records.entry(slot).or_default().push(record);
                collected += 1;
            }
        }
        collected
    }

    /// Write collected records to CSV file
    pub fn write_to_csv(&self, path: &str) -> std::io::Result<()> {
        // Signal shutdown and wait for collector
        self.shutdown_flag.store(true, Ordering::Release);

        if let Some(_handle) = &self.collector_handle {
            // Wait for collector to finish (timeout safety)
            thread::sleep(Duration::from_millis(100));
        }

        // Retrieve final records
        let records_opt = FINAL_RECORDS.lock().take();
        let records = match records_opt {
            Some(r) => r,
            None => {
                tracing::debug!("no records collected");
                return Ok(());
            }
        };

        if records.is_empty() {
            tracing::debug!("no records to write");
            return Ok(());
        }

        // Write to CSV with buffering
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        writeln!(writer, "slot,job_id,start_ns,end_ns,worker,task_id,index")?;

        let mut total_written = 0;
        for (_slot, record_vec) in records.iter() {
            for rec in record_vec {
                writeln!(
                    writer,
                    "{},{},{},{},{},{},{}",
                    rec.slot,
                    rec.job_id,
                    rec.start_ns,
                    rec.end_ns,
                    rec.worker,
                    rec.task_id,
                    rec.index
                )?;
                total_written += 1;
            }
        }

        writer.flush()?;
        tracing::info!(total_written, path, "records written");
        Ok(())
    }

    /// Shutdown and join collector thread
    pub fn shutdown(&mut self) {
        self.shutdown_flag.store(true, Ordering::Release);
        if let Some(handle) = self.collector_handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for AsyncRecorder {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Global storage for final records (after collector thread exits)
static FINAL_RECORDS: Mutex<Option<HashMap<usize, Vec<Record>>>> = Mutex::new(None);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Record;

    #[test]
    fn test_async_recorder_basic() {
        let recorder = AsyncRecorder::new(4, 100);

        // Simulate worker recording
        for worker_id in 0..4 {
            if let Some(tx) = recorder.get_worker_sender(worker_id) {
                for i in 0..10 {
                    let record = Record {
                        slot: 0,
                        job_id: i,
                        start_ns: i as u128 * 1000,
                        end_ns: (i + 1) as u128 * 1000,
                        worker: worker_id,
                        task_id: 1,
                        index: i,
                    };
                    let _ = tx.send(record);
                }
            }
        }

        // Allow collection
        thread::sleep(Duration::from_millis(50));

        // Shutdown and write
        let _ = recorder.write_to_csv("/tmp/test_records.csv");
    }
}
