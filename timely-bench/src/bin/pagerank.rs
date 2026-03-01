//! Timely Dataflow PageRank benchmark following the COST paper approach.
//!
//! McSherry, "Scalability! But at what COST?", HotOS 2015.
//!
//! Workers share the rank vector via Arc<RwLock<>>.  Each worker holds a
//! per-worker contribution buffer.  Scatter reads ranks (concurrent reads),
//! then each worker writes its own contribution buffer (no shared write
//! contention).  Worker 0 gathers all buffers and updates ranks.
//!
//! Usage:
//!   cargo run -r -p timely-bench --bin pagerank -- \
//!     --graph-file /data/snap/soc-LiveJournal1.txt \
//!     --iterations 20 --workers 4 --output timely_pagerank.csv

use clap::Parser;
use std::sync::{Arc, Barrier, RwLock};
use std::time::Instant;
use timely_bench::{append_graph_csv, parse_snap};

#[derive(Parser, Debug)]
#[command(name = "pagerank", about = "Timely Dataflow PageRank benchmark")]
struct Cli {
    #[arg(long)]
    graph_file: String,
    #[arg(long, default_value_t = 20usize)]
    iterations: usize,
    #[arg(long, default_value_t = 0.85f64)]
    damping: f64,
    #[arg(long, default_value_t = 1usize)]
    workers: usize,
    #[arg(long, default_value = "timely_pagerank.csv")]
    output: String,
    #[arg(long, default_value = "unknown")]
    dataset: String,
}

fn main() {
    let cli = Cli::parse();
    let graph_file = cli.graph_file.clone();
    let iterations = cli.iterations;
    let damping = cli.damping;
    let num_workers = cli.workers;
    let output = cli.output.clone();
    let dataset = cli.dataset.clone();

    println!("Loading graph from {}...", graph_file);
    let (num_nodes, all_edges) = parse_snap(&graph_file);
    println!("  {} nodes, {} edges", num_nodes, all_edges.len());

    // Global out-degrees
    let mut out_degrees = vec![0u32; num_nodes];
    for &(src, _) in &all_edges {
        out_degrees[src as usize] += 1;
    }
    let out_degrees = Arc::new(out_degrees);

    // Partition edges by source into per-worker CSR tables (Arc-wrapped for cloning)
    // worker i owns sources where src % num_workers == i
    let all_edges_arc = Arc::new(all_edges);

    // Shared rank vector (updated once per iteration, after scatter)
    let ranks: Arc<RwLock<Vec<f64>>> = Arc::new(RwLock::new(
        vec![1.0f64 / num_nodes as f64; num_nodes]
    ));

    // Per-worker contribution buffers (worker i writes contribs_list[i])
    let contribs_list: Arc<Vec<RwLock<Vec<f64>>>> = Arc::new(
        (0..num_workers).map(|_| RwLock::new(vec![0.0f64; num_nodes])).collect()
    );

    // Barrier for synchronising scatter → gather phases
    let scatter_done = Arc::new(Barrier::new(num_workers));
    let gather_done  = Arc::new(Barrier::new(num_workers));

    let t0 = Instant::now();

    timely::execute_from_args(
        vec![String::from("-w"), num_workers.to_string()].into_iter(),
        move |worker| {
            let wi = worker.index();
            let nw = worker.peers();

            // Build local CSR (only edges where src % nw == wi)
            let edges = &*all_edges_arc;
            let od    = &*out_degrees;

            // Local edge list
            let local_edges: Vec<(u32, u32)> = edges.iter()
                .filter(|&&(src, _)| (src as usize % nw) == wi)
                .cloned()
                .collect();

            // Build CSR over local edges
            let mut offsets = vec![0usize; num_nodes + 1];
            for &(src, _) in &local_edges {
                offsets[src as usize + 1] += 1;
            }
            for i in 0..num_nodes { offsets[i + 1] += offsets[i]; }
            let mut targets = vec![0u32; local_edges.len()];
            let mut pos = offsets[..num_nodes].to_vec();
            for &(src, dst) in &local_edges {
                targets[pos[src as usize]] = dst;
                pos[src as usize] += 1;
            }

            let ranks_ref   = ranks.clone();
            let clist       = contribs_list.clone();
            let bar_scatter = scatter_done.clone();
            let bar_gather  = gather_done.clone();

            for _iter in 0..iterations {
                // --- Scatter: read current ranks, write own contribution buffer ---
                {
                    let r = ranks_ref.read().unwrap();
                    let mut my_contribs = clist[wi].write().unwrap();
                    // Zero out
                    for v in my_contribs.iter_mut() { *v = 0.0; }

                    // Iterate over local sources
                    let mut src_node = wi;
                    while src_node < num_nodes {
                        let od_n = od[src_node];
                        if od_n > 0 {
                            let contrib = r[src_node] / od_n as f64;
                            for &dst in &targets[offsets[src_node]..offsets[src_node + 1]] {
                                my_contribs[dst as usize] += contrib;
                            }
                        }
                        src_node += nw;
                    }
                } // read lock on ranks dropped, write lock on my_contribs dropped

                // Wait for all workers to finish their scatter
                bar_scatter.wait();

                // Worker 0 gathers all contribution buffers and updates ranks
                if wi == 0 {
                    let base = (1.0 - damping) / num_nodes as f64;
                    let mut r = ranks_ref.write().unwrap();
                    for i in 0..num_nodes {
                        let total: f64 = clist.iter()
                            .map(|cl| cl.read().unwrap()[i])
                            .sum();
                        r[i] = base + damping * total;
                    }
                }

                // Wait for worker 0 to finish the gather before next iteration
                bar_gather.wait();
            }
        },
    )
    .expect("Timely execute failed");

    let total_s = t0.elapsed().as_secs_f64();
    let s_per_iter = total_s / iterations as f64;

    println!(
        "Timely PageRank | dataset={} | workers={} | {} iters | total={:.3}s | {:.3}s/iter",
        dataset, num_workers, iterations, total_s, s_per_iter
    );

    append_graph_csv(&output, "timely", &dataset, num_workers, iterations, total_s, s_per_iter);
}
