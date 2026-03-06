//! Timely Dataflow Wavefront benchmark.
//!
//! Computes iterative anti-diagonal wavefront on an N×N grid:
//!   grid[i][j] = 0.5 * (grid[i-1][j] + grid[i][j-1])
//!
//! Boundary conditions (pre-initialised, never overwritten):
//!   grid[0][j] = (j+1) as f64   (top row)
//!   grid[i][0] = (i+1) as f64   (left column)
//!
//! Parallelism: worker wi processes cells on diagonal d where position p
//! satisfies p % num_workers == wi.  A Barrier synchronises workers between
//! consecutive diagonals (matching the SynStream $barrier semantics).
//!
//! CPU pinning: if --core-offset N is given, worker wi is pinned to core N+wi,
//! matching SynStream's --core-offset behaviour.
//!
//! Usage:
//!   cargo run -r -p timely-bench --bin wavefront -- \
//!     --n 512 --iterations 20 --workers 4 --core-offset 1 \
//!     --output timely_wavefront.csv

use clap::Parser;
use std::sync::{Arc, Barrier};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use timely_bench::append_wavefront_csv;

#[derive(Parser, Debug)]
#[command(name = "wavefront", about = "Timely Dataflow Wavefront benchmark")]
struct Cli {
    #[arg(long, default_value_t = 512usize)]
    n: usize,
    #[arg(long, default_value_t = 20usize)]
    iterations: usize,
    #[arg(long, default_value_t = 3usize)]
    warmup: usize,
    #[arg(long, default_value_t = 1usize)]
    workers: usize,
    /// Pin worker wi to CPU core (core_offset + wi).  Mirrors SynStream's --core-offset.
    #[arg(long)]
    core_offset: Option<usize>,
    #[arg(long, default_value = "timely_wavefront.csv")]
    output: String,
}

/// Pin the calling thread to a specific CPU core using sched_setaffinity.
fn pin_to_core(core_id: usize) {
    unsafe {
        let mut cpuset: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(core_id, &mut cpuset);
        let ret = libc::sched_setaffinity(
            0,
            std::mem::size_of::<libc::cpu_set_t>(),
            &cpuset,
        );
        if ret != 0 {
            eprintln!("Warning: sched_setaffinity failed for core {}", core_id);
        }
    }
}

fn main() {
    let cli = Cli::parse();
    let n           = cli.n;
    let iterations  = cli.iterations;
    let warmup      = cli.warmup;
    let num_workers = cli.workers;
    let core_offset = cli.core_offset;
    let output      = cli.output.clone();

    println!("Timely Wavefront N={} workers={} core_offset={:?}", n, num_workers, core_offset);

    // Pre-allocate the shared N×N grid with boundary values.
    let mut grid_init = vec![0.0f64; n * n];
    for j in 0..n { grid_init[j]     = (j + 1) as f64; }  // row 0
    for i in 1..n { grid_init[i * n] = (i + 1) as f64; }  // col 0

    // Wrap in Arc<Vec<f64>>; workers write non-overlapping cells via raw ptr.
    let grid: Arc<Vec<f64>> = Arc::new(grid_init);

    // Barrier: all workers finish diagonal d before any starts d+1.
    let bar_diag = Arc::new(Barrier::new(num_workers));

    // Accumulate timed-sweep nanoseconds from worker 0 only.
    let timed_ns_out: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    let timed_ns = timed_ns_out.clone();

    let total_sweeps = warmup + iterations;

    timely::execute_from_args(
        vec![String::from("-w"), num_workers.to_string()].into_iter(),
        move |worker| {
            let wi     = worker.index();
            let nw     = worker.peers();
            let g      = grid.clone();
            let bar    = bar_diag.clone();
            let acc_ns = timed_ns.clone();

            // Pin this worker thread to its assigned core.
            if let Some(offset) = core_offset {
                pin_to_core(offset + wi);
            }

            let data_ptr = g.as_ptr() as *mut f64;

            for sweep in 0..total_sweeps {
                let t0 = if wi == 0 { Some(Instant::now()) } else { None };

                // d=0 is cell (0,0) — boundary only; start from d=1.
                for d in 1..(2 * n - 1) {
                    let i_start = d.min(n - 1);
                    let width   = (d + 1).min(n).min(2 * n - 1 - d);

                    // Worker wi processes positions p = wi, wi+nw, wi+2nw, ...
                    let mut p = wi;
                    while p < width {
                        let i = i_start - p;
                        let j = d - i;
                        if i > 0 && j > 0 {
                            // SAFETY:
                            // 1. Workers write non-overlapping cells (unique p per worker).
                            // 2. Barrier below ensures diagonal d-1 writes are visible
                            //    before any worker reads them for diagonal d.
                            // 3. Vec allocation is stable (no reallocation during run).
                            unsafe {
                                let left = *data_ptr.add(i * n + (j - 1));
                                let top  = *data_ptr.add((i - 1) * n + j);
                                *data_ptr.add(i * n + j) = 0.5 * (left + top);
                            }
                        }
                        p += nw;
                    }

                    // All workers must finish diagonal d before any starts d+1.
                    bar.wait();
                }

                if wi == 0 {
                    let elapsed_ns = t0.unwrap().elapsed().as_nanos() as u64;
                    if sweep >= warmup {
                        acc_ns.fetch_add(elapsed_ns, Ordering::Relaxed);
                        println!(
                            "  sweep {:2}: {:.4}s",
                            sweep - warmup + 1,
                            elapsed_ns as f64 / 1e9
                        );
                    }
                }
            }
        },
    )
    .expect("Timely execute failed");

    let total_s    = timed_ns_out.load(Ordering::Relaxed) as f64 / 1e9;
    let s_per_iter = total_s / iterations as f64;

    println!(
        "Timely Wavefront | n={} | workers={} | {} timed sweeps | total={:.3}s | {:.4}s/iter",
        n, num_workers, iterations, total_s, s_per_iter
    );

    append_wavefront_csv(&output, "timely", n, num_workers, iterations, total_s, s_per_iter);
}
