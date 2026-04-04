mod centralized;
mod distributed;
mod eager;
mod executor;
mod flat_graph;
mod generational;
mod pointer_graph;
mod reset_bench;
mod slots_bench;
mod wavefront;

use executor::GraphFrontend;
use flat_graph::FlatGraph;
use pointer_graph::PointerGraph;
use std::{env, sync::Arc, time::Duration};

// ── GraphFrontend impls ───────────────────────────────────────────────────

impl GraphFrontend for FlatGraph {
    fn n_nodes(&self) -> usize { self.n_nodes }
    fn successors(&self, id: usize) -> &[u32] { self.successors(id) }
    fn roots(&self) -> &[u32] { &self.roots }
    fn pred_counts(&self) -> &[u32] { self.pred_counts() }
}

impl GraphFrontend for PointerGraph {
    fn n_nodes(&self) -> usize { self.n_nodes }
    fn successors(&self, id: usize) -> &[u32] { self.successors(id) }
    fn roots(&self) -> &[u32] { &self.roots }
    fn pred_counts(&self) -> &[u32] { self.pred_counts() }
}

// ── CLI ───────────────────────────────────────────────────────────────────

struct Args {
    mode: String,
    n: usize,
    workers: usize,
    iterations: usize,
    warmup: usize,
    verify: bool,
    streams: usize,
    spin_ns: u64,
}

fn parse_args() -> Args {
    let args: Vec<String> = env::args().collect();
    let mut mode = "flat-distributed".to_string();
    let mut n = 256usize;
    let mut workers = 4usize;
    let mut iterations = 20usize;
    let mut warmup = 3usize;
    let mut verify = false;
    let mut streams = 64usize;
    let mut spin_ns = 1_000u64; // 1µs default task duration

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--mode" => { mode = args[i + 1].clone(); i += 2; }
            "--n" => { n = args[i + 1].parse().expect("--n must be integer"); i += 2; }
            "--workers" => { workers = args[i + 1].parse().expect("--workers must be integer"); i += 2; }
            "--iterations" => { iterations = args[i + 1].parse().expect("--iterations must be integer"); i += 2; }
            "--warmup" => { warmup = args[i + 1].parse().expect("--warmup must be integer"); i += 2; }
            "--verify" => { verify = true; i += 1; }
            "--streams" => { streams = args[i + 1].parse().expect("--streams must be integer"); i += 2; }
            "--spin" => { spin_ns = args[i + 1].parse().expect("--spin must be integer (ns)"); i += 2; }
            "--help" | "-h" => {
                eprintln!("Usage: wavefront-ablation [--mode flat-distributed|flat-centralized|pointer-distributed|pointer-centralized|flat-generational|flat-eager|reset-bench|slots-bench] [--n N] [--workers W] [--iterations I] [--warmup W] [--streams K] [--spin NS] [--verify]");
                std::process::exit(0);
            }
            other => { eprintln!("Unknown argument: {other}"); i += 1; }
        }
    }
    Args { mode, n, workers, iterations, warmup, verify, streams, spin_ns }
}

// ── Main ──────────────────────────────────────────────────────────────────

fn main() {
    let args = parse_args();
    let n = args.n;
    let workers = args.workers;
    let iters = args.iterations;
    let warmup = args.warmup;

    // ── reset-bench mode: measures reset cost only, no task execution ────────
    if args.mode == "reset-bench" {
        let node_sizes: &[usize] = &[64, 256, 1024, 4096, 16384];
        println!("mode,n_nodes,mean_ns_gen,mean_ns_eager");
        for &n_nodes in node_sizes {
            let (mean_gen, mean_eager) =
                reset_bench::bench_reset_cost(n_nodes, iters);
            println!(
                "reset-bench,{n_nodes},{mean_gen:.2},{mean_eager:.2}",
            );
        }
        return;
    }

    // ── slots-bench mode: measures concurrent-slot throughput scaling ─────────
    // Uses a linear chain graph (zero intra-stream parallelism) to show that
    // S concurrent slots give near-S× throughput when sharing a single topology.
    if args.mode == "slots-bench" {
        let graph = Arc::new(FlatGraph::from_chain(n));
        slots_bench::run_slots_bench(
            graph,
            workers,
            args.streams,
            warmup,
            iters,
            args.spin_ns,
        );
        return;
    }

    let (times, final_grid): (Vec<Duration>, Vec<f64>) = match args.mode.as_str() {
        "flat-distributed" => {
            let graph = Arc::new(FlatGraph::from_wavefront(n));
            executor::run_sweeps_distributed(graph, n, workers, warmup, iters)
        }
        "flat-centralized" => {
            let graph = Arc::new(FlatGraph::from_wavefront(n));
            executor::run_sweeps_centralized(graph, n, workers, warmup, iters)
        }
        "pointer-distributed" => {
            let graph = Arc::new(PointerGraph::from_wavefront(n));
            executor::run_sweeps_distributed(graph, n, workers, warmup, iters)
        }
        "pointer-centralized" => {
            let graph = Arc::new(PointerGraph::from_wavefront(n));
            executor::run_sweeps_centralized(graph, n, workers, warmup, iters)
        }
        // ── New corrected ablation baselines ─────────────────────────────
        "flat-generational" => {
            // FlatGraph + GenerationalResolution: the corrected SynStream
            // baseline that uses O(1) inter-sweep reset.
            let graph = Arc::new(FlatGraph::from_wavefront(n));
            executor::run_sweeps_generational(graph, n, workers, warmup, iters)
        }
        "flat-eager" => {
            // FlatGraph + EagerResolution: the TaskFlow-equivalent baseline
            // with O(N) per-sweep reset (same decrement hot path as
            // flat-distributed, but reset is explicit and measurable).
            let graph = Arc::new(FlatGraph::from_wavefront(n));
            executor::run_sweeps_eager(graph, n, workers, warmup, iters)
        }
        other => {
            eprintln!(
                "Unknown mode: {other}. Use flat-distributed, flat-centralized, \
                 pointer-distributed, pointer-centralized, flat-generational, \
                 flat-eager, or reset-bench."
            );
            std::process::exit(1);
        }
    };

    if args.verify {
        if wavefront::verify_grid(&final_grid, n) {
            eprintln!("verify: PASS (mode={}, n={n})", args.mode);
        } else {
            eprintln!("verify: FAIL (mode={}, n={n})", args.mode);
            std::process::exit(2);
        }
    }

    let total_s: f64 = times.iter().map(|d| d.as_secs_f64()).sum();
    let mean_ms: f64 = total_s * 1000.0 / iters as f64;
    let variance: f64 = times.iter()
        .map(|d| { let x = d.as_secs_f64() * 1000.0 - mean_ms; x * x })
        .sum::<f64>() / iters as f64;
    let stddev_ms = variance.sqrt();

    // Output: CSV row compatible with existing benchmark schema + mean_ms + stddev_ms
    // system,n,workers,iterations,total_s,s_per_iter,mean_ms,stddev_ms
    println!(
        "{},{},{},{},{:.6},{:.9},{:.4},{:.4}",
        args.mode, n, workers, iters,
        total_s, total_s / iters as f64,
        mean_ms, stddev_ms
    );
}
