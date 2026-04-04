use crossbeam_channel::{Receiver, Sender};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use crate::{
    centralized::CentralizedResolution,
    distributed::DistributedResolution,
    eager::EagerResolution,
    generational::GenerationalResolution,
    wavefront,
};

/// Implemented by both FlatGraph and PointerGraph.
pub trait GraphFrontend: Send + Sync {
    fn n_nodes(&self) -> usize;
    fn successors(&self, id: usize) -> &[u32];
    fn roots(&self) -> &[u32];
    fn pred_counts(&self) -> &[u32];
}

// ── Poison pill value used to stop worker threads ─────────────────────────
const POISON: u32 = u32::MAX;

// ── Shared grid pointer (workers write to non-overlapping cells) ──────────
#[derive(Clone, Copy)]
struct GridPtr(*mut f64);
unsafe impl Send for GridPtr {}
unsafe impl Sync for GridPtr {}

// ── Common worker body ────────────────────────────────────────────────────

fn worker_distributed<G: GraphFrontend>(
    ready_rx: Receiver<u32>,
    ready_tx: Sender<u32>,
    grid: GridPtr,
    n: usize,
    graph: Arc<G>,
    resolution: Arc<DistributedResolution>,
    remaining: Arc<AtomicUsize>,
    done_tx: Sender<()>,
) {
    loop {
        let task_id = match ready_rx.recv() {
            Ok(POISON) | Err(_) => break,
            Ok(id) => id as usize,
        };
        // Execute kernel.
        unsafe { wavefront::compute_cell(grid.0, n, task_id) };

        // Inline resolution: completing thread decrements successors directly.
        for &succ in graph.successors(task_id) {
            if resolution.decrement(succ as usize) {
                ready_tx.send(succ).unwrap();
            }
        }

        // Signal sweep completion when this was the last task.
        if remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            done_tx.send(()).unwrap();
        }
    }
}

fn worker_centralized(
    ready_rx: Receiver<u32>,
    grid: GridPtr,
    n: usize,
    completion_tx: Sender<u32>,
) {
    loop {
        let task_id = match ready_rx.recv() {
            Ok(POISON) | Err(_) => break,
            Ok(id) => id,
        };
        // Execute kernel.
        unsafe { wavefront::compute_cell(grid.0, n, task_id as usize) };

        // Submit to coordinator — no inline resolution.
        completion_tx.send(task_id).unwrap();
    }
}

// ── Sweep runner — distributed resolution ────────────────────────────────

pub fn run_sweeps_distributed<G: GraphFrontend + 'static>(
    graph: Arc<G>,
    n: usize,
    n_workers: usize,
    warmup: usize,
    iterations: usize,
) -> (Vec<Duration>, Vec<f64>) {
    let n_nodes = graph.n_nodes();
    let resolution = Arc::new(DistributedResolution::new(graph.pred_counts()));
    let remaining = Arc::new(AtomicUsize::new(n_nodes));
    let (done_tx, done_rx) = crossbeam_channel::bounded::<()>(1);

    // Allocate grid (main thread owns it; workers access via raw pointer).
    let mut grid_vec = wavefront::init_grid(n);
    let grid = GridPtr(grid_vec.as_mut_ptr());

    // Shared MPMC ready channel.
    let (ready_tx, ready_rx) = crossbeam_channel::unbounded::<u32>();

    // Spawn workers.
    let mut handles = Vec::with_capacity(n_workers);
    for _ in 0..n_workers {
        let rx = ready_rx.clone();
        let tx = ready_tx.clone();
        let g = Arc::clone(&graph);
        let res = Arc::clone(&resolution);
        let rem = Arc::clone(&remaining);
        let dtx = done_tx.clone();
        handles.push(thread::spawn(move || {
            worker_distributed(rx, tx, grid, n, g, res, rem, dtx);
        }));
    }
    drop(done_tx); // main thread doesn't hold a sender

    let mut times = Vec::with_capacity(iterations);

    for sweep in 0..warmup + iterations {
        // --- Outside timing: reset state ---
        for j in 0..n { grid_vec[j] = (j + 1) as f64; }
        for i in 1..n { grid_vec[i * n] = (i + 1) as f64; }
        for i in 1..n { for j in 1..n { grid_vec[i * n + j] = 0.0; } }
        resolution.reset();
        remaining.store(n_nodes, Ordering::SeqCst);

        // --- Timed section ---
        let t0 = Instant::now();
        for &root in graph.roots() {
            ready_tx.send(root).unwrap();
        }
        done_rx.recv().unwrap();
        let elapsed = t0.elapsed();

        if sweep >= warmup {
            times.push(elapsed);
        }
    }

    // Stop workers.
    for _ in 0..n_workers {
        ready_tx.send(POISON).unwrap();
    }
    for h in handles {
        h.join().unwrap();
    }

    (times, grid_vec)
}

// ── Sweep runner — centralized resolution ────────────────────────────────

pub fn run_sweeps_centralized<G: GraphFrontend + 'static>(
    graph: Arc<G>,
    n: usize,
    n_workers: usize,
    warmup: usize,
    iterations: usize,
) -> (Vec<Duration>, Vec<f64>) {
    let n_nodes = graph.n_nodes();
    let remaining = Arc::new(AtomicUsize::new(n_nodes));
    let (done_tx, done_rx) = crossbeam_channel::bounded::<()>(1);

    // Allocate grid.
    let mut grid_vec = wavefront::init_grid(n);
    let grid = GridPtr(grid_vec.as_mut_ptr());

    // Shared MPMC ready channel (coordinator enqueues; workers dequeue).
    let (ready_tx, ready_rx) = crossbeam_channel::unbounded::<u32>();

    // Build successor topology snapshot for the coordinator (flat graph representation).
    let successors: Vec<Vec<u32>> = (0..n_nodes)
        .map(|id| graph.successors(id).to_vec())
        .collect();

    // Start coordinator thread.
    let resolution = CentralizedResolution::start(
        graph.pred_counts().to_vec(),
        successors,
        ready_tx.clone(),
        Arc::clone(&remaining),
        done_tx,
    );

    // Spawn workers (they only execute + submit completions; no resolution).
    let mut handles = Vec::with_capacity(n_workers);
    for _ in 0..n_workers {
        let rx = ready_rx.clone();
        let tx = resolution.completion_tx.clone();
        handles.push(thread::spawn(move || {
            worker_centralized(rx, grid, n, tx);
        }));
    }

    let mut times = Vec::with_capacity(iterations);

    for sweep in 0..warmup + iterations {
        // --- Outside timing: reset state ---
        for j in 0..n { grid_vec[j] = (j + 1) as f64; }
        for i in 1..n { grid_vec[i * n] = (i + 1) as f64; }
        for i in 1..n { for j in 1..n { grid_vec[i * n + j] = 0.0; } }
        remaining.store(n_nodes, Ordering::SeqCst);
        resolution.reset(); // synchronous: coordinator resets dep counters, sends ack

        // --- Timed section ---
        let t0 = Instant::now();
        for &root in graph.roots() {
            ready_tx.send(root).unwrap();
        }
        done_rx.recv().unwrap();
        let elapsed = t0.elapsed();

        if sweep >= warmup {
            times.push(elapsed);
        }
    }

    // Stop workers and coordinator.
    for _ in 0..n_workers {
        ready_tx.send(POISON).unwrap();
    }
    for h in handles {
        h.join().unwrap();
    }
    resolution.stop();

    (times, grid_vec)
}

// ── Sweep runner — generational resolution (SynStream O(1) reset) ────────

/// Worker for `GenerationalResolution`.
///
/// The generation is read directly from the shared `AtomicU32` inside
/// `resolution` at the start of each task.  This is safe because:
/// - `bump_generation` uses `SeqCst` ordering.
/// - Workers dequeue their first task only after the main thread enqueues
///   roots (which happens after `bump_generation`).
/// - Therefore all workers see the current generation on their first load
///   from the shared atomic.
///
/// Workers cache the generation in a local variable and refresh it with
/// `Acquire` on each task dequeue.  This keeps the fast path to one load
/// per task rather than one per decrement call.
fn worker_generational<G: GraphFrontend>(
    ready_rx: Receiver<u32>,
    ready_tx: Sender<u32>,
    grid: GridPtr,
    n: usize,
    graph: Arc<G>,
    resolution: Arc<GenerationalResolution>,
    remaining: Arc<AtomicUsize>,
    done_tx: Sender<()>,
) {
    loop {
        let task_id = match ready_rx.recv() {
            Ok(POISON) | Err(_) => break,
            Ok(id) => id as usize,
        };

        // Load the current generation once per task.  The SeqCst bump in the
        // main thread and the Acquire load here form a synchronisation point.
        let current_gen = resolution.generation();

        // Execute kernel.
        unsafe { wavefront::compute_cell(grid.0, n, task_id) };

        // Inline generational resolution.
        for &succ in graph.successors(task_id) {
            if resolution.decrement(succ as usize, current_gen) {
                ready_tx.send(succ).unwrap();
            }
        }

        if remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            done_tx.send(()).unwrap();
        }
    }
}

pub fn run_sweeps_generational<G: GraphFrontend + 'static>(
    graph: Arc<G>,
    n: usize,
    n_workers: usize,
    warmup: usize,
    iterations: usize,
) -> (Vec<Duration>, Vec<f64>) {
    let n_nodes = graph.n_nodes();
    let resolution = Arc::new(GenerationalResolution::new(graph.pred_counts()));
    let remaining = Arc::new(AtomicUsize::new(n_nodes));
    let (done_tx, done_rx) = crossbeam_channel::bounded::<()>(1);

    let mut grid_vec = wavefront::init_grid(n);
    let grid = GridPtr(grid_vec.as_mut_ptr());

    let (ready_tx, ready_rx) = crossbeam_channel::unbounded::<u32>();

    let mut handles = Vec::with_capacity(n_workers);
    for _ in 0..n_workers {
        let rx = ready_rx.clone();
        let tx = ready_tx.clone();
        let g = Arc::clone(&graph);
        let res = Arc::clone(&resolution);
        let rem = Arc::clone(&remaining);
        let dtx = done_tx.clone();
        handles.push(thread::spawn(move || {
            worker_generational(rx, tx, grid, n, g, res, rem, dtx);
        }));
    }
    drop(done_tx);

    let mut times = Vec::with_capacity(iterations);

    for sweep in 0..warmup + iterations {
        // --- Outside timing: reset grid state ---
        for j in 0..n { grid_vec[j] = (j + 1) as f64; }
        for i in 1..n { grid_vec[i * n] = (i + 1) as f64; }
        for i in 1..n { for j in 1..n { grid_vec[i * n + j] = 0.0; } }
        remaining.store(n_nodes, Ordering::SeqCst);

        // O(1) inter-sweep reset: single fetch_add on the generation counter.
        // No per-node counter reset.  Workers lazily reinitialise stale slots
        // on first access via the CAS loop in `GenerationalResolution::decrement`.
        resolution.bump_generation();

        // --- Timed section ---
        let t0 = Instant::now();
        for &root in graph.roots() {
            ready_tx.send(root).unwrap();
        }
        done_rx.recv().unwrap();
        let elapsed = t0.elapsed();

        if sweep >= warmup {
            times.push(elapsed);
        }
    }

    for _ in 0..n_workers {
        ready_tx.send(POISON).unwrap();
    }
    for h in handles {
        h.join().unwrap();
    }

    (times, grid_vec)
}

// ── Sweep runner — eager resolution (TaskFlow O(N) reset) ────────────────

fn worker_eager<G: GraphFrontend>(
    ready_rx: Receiver<u32>,
    ready_tx: Sender<u32>,
    grid: GridPtr,
    n: usize,
    graph: Arc<G>,
    resolution: Arc<EagerResolution>,
    remaining: Arc<AtomicUsize>,
    done_tx: Sender<()>,
) {
    loop {
        let task_id = match ready_rx.recv() {
            Ok(POISON) | Err(_) => break,
            Ok(id) => id as usize,
        };
        unsafe { wavefront::compute_cell(grid.0, n, task_id) };

        for &succ in graph.successors(task_id) {
            if resolution.decrement(succ as usize) {
                ready_tx.send(succ).unwrap();
            }
        }

        if remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            done_tx.send(()).unwrap();
        }
    }
}

pub fn run_sweeps_eager<G: GraphFrontend + 'static>(
    graph: Arc<G>,
    n: usize,
    n_workers: usize,
    warmup: usize,
    iterations: usize,
) -> (Vec<Duration>, Vec<f64>) {
    let n_nodes = graph.n_nodes();
    let resolution = Arc::new(EagerResolution::new(graph.pred_counts()));
    let remaining = Arc::new(AtomicUsize::new(n_nodes));
    let (done_tx, done_rx) = crossbeam_channel::bounded::<()>(1);

    let mut grid_vec = wavefront::init_grid(n);
    let grid = GridPtr(grid_vec.as_mut_ptr());

    let (ready_tx, ready_rx) = crossbeam_channel::unbounded::<u32>();

    let mut handles = Vec::with_capacity(n_workers);
    for _ in 0..n_workers {
        let rx = ready_rx.clone();
        let tx = ready_tx.clone();
        let g = Arc::clone(&graph);
        let res = Arc::clone(&resolution);
        let rem = Arc::clone(&remaining);
        let dtx = done_tx.clone();
        handles.push(thread::spawn(move || {
            worker_eager(rx, tx, grid, n, g, res, rem, dtx);
        }));
    }
    drop(done_tx);

    let mut times = Vec::with_capacity(iterations);

    for sweep in 0..warmup + iterations {
        // --- Outside timing: O(N) eager reset + grid reset ---
        for j in 0..n { grid_vec[j] = (j + 1) as f64; }
        for i in 1..n { grid_vec[i * n] = (i + 1) as f64; }
        for i in 1..n { for j in 1..n { grid_vec[i * n + j] = 0.0; } }
        // Explicit O(N) reset — required before every sweep.
        resolution.reset();
        remaining.store(n_nodes, Ordering::SeqCst);

        // --- Timed section ---
        let t0 = Instant::now();
        for &root in graph.roots() {
            ready_tx.send(root).unwrap();
        }
        done_rx.recv().unwrap();
        let elapsed = t0.elapsed();

        if sweep >= warmup {
            times.push(elapsed);
        }
    }

    for _ in 0..n_workers {
        ready_tx.send(POISON).unwrap();
    }
    for h in handles {
        h.join().unwrap();
    }

    (times, grid_vec)
}
