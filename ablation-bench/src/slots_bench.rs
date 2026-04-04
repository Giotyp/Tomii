/// Static Topology Sharing micro-benchmark.
///
/// Validates that running S concurrent streams over ONE shared graph achieves
/// near-S× throughput without duplicating graph topology.
///
/// Design
/// ──────
/// - Graph: linear chain of N nodes (zero intra-stream parallelism).
///   With S=1, only one worker is ever active.  With S=k, k workers each drive
///   a different stream's current task — throughput should scale linearly.
/// - Shared topology: ONE Arc<FlatGraph> is shared across all S slots.
///   Each slot has its own GenerationalResolution (per-slot dep counters) and
///   a remaining counter.  Topology is never duplicated.
/// - Stream handoff: when a slot's stream completes, the slot atomically claims
///   the next stream from a counter, bumps its generation (O(1) reset), and
///   enqueues its roots.  No allocation, no topology copy.
/// - Measurement: wall time to complete K total streams; yields throughput
///   (streams/sec) and per-stream mean latency (ms).
use crossbeam_channel::{Receiver, Sender};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::{executor::GraphFrontend, generational::GenerationalResolution};

// ── Task encoding ────────────────────────────────────────────────────────────
// Pack (slot_id: u32, task_id: u32) into u64 for the shared ready channel.
#[inline(always)]
fn pack(slot_id: u32, task_id: u32) -> u64 {
    ((slot_id as u64) << 32) | (task_id as u64)
}
#[inline(always)]
fn unpack(v: u64) -> (usize, usize) {
    ((v >> 32) as usize, (v as u32) as usize)
}
const POISON: u64 = u64::MAX;

// ── Per-slot state ───────────────────────────────────────────────────────────
struct SlotState {
    /// Per-slot dependency counters (generational — O(1) inter-stream reset).
    resolution: GenerationalResolution,
    /// Tasks still pending in the current stream for this slot.
    remaining: AtomicUsize,
}

// SAFETY: SlotState contains only atomics; it is Sync.
unsafe impl Sync for SlotState {}
unsafe impl Send for SlotState {}

// ── Worker body ──────────────────────────────────────────────────────────────

fn worker<G: GraphFrontend>(
    ready_rx: Receiver<u64>,
    ready_tx: Sender<u64>,
    graph: Arc<G>,
    slots: Arc<Vec<SlotState>>,
    next_stream: Arc<AtomicUsize>,
    streams_done: Arc<AtomicUsize>,
    k_streams: usize,
    all_done_tx: Sender<()>,
    spin_ns: u64,
) {
    loop {
        let encoded = match ready_rx.recv() {
            Ok(POISON) | Err(_) => break,
            Ok(v) => v,
        };
        let (slot_id, task_id) = unpack(encoded);
        let slot = &slots[slot_id];

        // Load generation once per task (same pattern as executor.rs worker_generational).
        let current_gen = slot.resolution.generation();

        // Execute kernel: configurable spin to simulate real work.
        if spin_ns > 0 {
            let deadline = Instant::now() + Duration::from_nanos(spin_ns);
            while Instant::now() < deadline {
                std::hint::spin_loop();
            }
        }

        // Inline generational resolution.
        for &succ in graph.successors(task_id) {
            if slot.resolution.decrement(succ as usize, current_gen) {
                ready_tx.send(pack(slot_id as u32, succ)).unwrap();
            }
        }

        // Stream completion check.
        if slot.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            let done = streams_done.fetch_add(1, Ordering::AcqRel) + 1;

            // Claim the next stream for this slot.
            let next = next_stream.fetch_add(1, Ordering::SeqCst);
            if next < k_streams {
                // O(1) slot reset: single fetch_add on generation counter.
                slot.resolution.bump_generation();
                slot.remaining.store(graph.n_nodes(), Ordering::SeqCst);
                for &root in graph.roots() {
                    ready_tx.send(pack(slot_id as u32, root)).unwrap();
                }
            }

            if done == k_streams {
                let _ = all_done_tx.send(());
            }
        }
    }
}

// ── Public entry point ───────────────────────────────────────────────────────

/// Run the slots benchmark for a fixed slot count.
///
/// Returns `(throughput_streams_per_sec, mean_stream_latency_ms)`.
///
/// `warmup` full K-stream batches are discarded; `iterations` batches are timed.
pub fn bench_one_slot_count<G: GraphFrontend + 'static>(
    graph: Arc<G>,
    n_slots: usize,
    n_workers: usize,
    k_streams: usize,
    warmup: usize,
    iterations: usize,
    spin_ns: u64,
) -> (f64, f64) {
    let n_nodes = graph.n_nodes();
    let total_batches = warmup + iterations;
    let mut throughputs = Vec::with_capacity(total_batches);
    let mut latencies = Vec::with_capacity(total_batches);

    for _batch in 0..total_batches {
        // Allocate fresh per-slot state (topology is shared, not duplicated).
        let slots: Vec<SlotState> = (0..n_slots)
            .map(|_| SlotState {
                resolution: GenerationalResolution::new(graph.pred_counts()),
                remaining: AtomicUsize::new(0),
            })
            .collect();
        let slots = Arc::new(slots);

        let (ready_tx, ready_rx) = crossbeam_channel::unbounded::<u64>();
        let next_stream = Arc::new(AtomicUsize::new(n_slots));
        let streams_done = Arc::new(AtomicUsize::new(0));
        let (all_done_tx, all_done_rx) = crossbeam_channel::bounded::<()>(1);

        let mut handles = Vec::with_capacity(n_workers);
        for _ in 0..n_workers {
            let rx = ready_rx.clone();
            let tx = ready_tx.clone();
            let g = Arc::clone(&graph);
            let s = Arc::clone(&slots);
            let ns = Arc::clone(&next_stream);
            let sd = Arc::clone(&streams_done);
            let adt = all_done_tx.clone();
            handles.push(thread::spawn(move || {
                worker(rx, tx, g, s, ns, sd, k_streams, adt, spin_ns);
            }));
        }
        drop(all_done_tx);

        // Start the initial min(n_slots, k_streams) streams.
        let initial = n_slots.min(k_streams);

        let t0 = Instant::now();
        for slot_id in 0..initial {
            slots[slot_id].resolution.bump_generation();
            slots[slot_id].remaining.store(n_nodes, Ordering::SeqCst);
            for &root in graph.roots() {
                ready_tx.send(pack(slot_id as u32, root)).unwrap();
            }
        }
        // If fewer streams than slots were submitted, keep next_stream correct.
        next_stream.store(initial, Ordering::SeqCst);

        all_done_rx.recv().unwrap();
        let elapsed = t0.elapsed();

        // Stop workers.
        for _ in 0..n_workers {
            ready_tx.send(POISON).unwrap();
        }
        for h in handles {
            h.join().unwrap();
        }

        let elapsed_s = elapsed.as_secs_f64();
        throughputs.push(k_streams as f64 / elapsed_s);
        latencies.push(elapsed_s * 1000.0 / k_streams as f64);
    }

    let tps = &throughputs[warmup..];
    let lats = &latencies[warmup..];
    let mean_tp = tps.iter().sum::<f64>() / tps.len() as f64;
    let mean_lat = lats.iter().sum::<f64>() / lats.len() as f64;
    (mean_tp, mean_lat)
}

/// Sweep slots ∈ {1, 2, 4, 8} and print CSV.
pub fn run_slots_bench<G: GraphFrontend + 'static>(
    graph: Arc<G>,
    n_workers: usize,
    k_streams: usize,
    warmup: usize,
    iterations: usize,
    spin_ns: u64,
) {
    println!("slots,throughput_streams_per_sec,mean_stream_latency_ms,relative_throughput");
    let slot_counts: &[usize] = &[1, 2, 4, 8];
    let mut baseline_tp: Option<f64> = None;

    for &s in slot_counts {
        if s > n_workers {
            // No point running more slots than workers — we'd just queue.
            continue;
        }
        let (tp, lat) = bench_one_slot_count(
            Arc::clone(&graph),
            s,
            n_workers,
            k_streams,
            warmup,
            iterations,
            spin_ns,
        );
        let baseline = *baseline_tp.get_or_insert(tp);
        println!("{s},{tp:.1},{lat:.3},{:.2}", tp / baseline);
    }
}
