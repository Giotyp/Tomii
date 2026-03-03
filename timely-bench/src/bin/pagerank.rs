//! Timely Dataflow PageRank benchmark following the COST paper approach.
//!
//! McSherry, "Scalability! But at what COST?", HotOS 2015.
//!
//! Ranks are split into per-worker chunks so that both scatter and gather are
//! fully parallel — no serial single-worker bottleneck:
//!
//!   scatter: all workers read ALL rank chunks (concurrent read locks),
//!            each writes its own contribution buffer.
//!   gather:  all workers concurrently reduce their own rank chunk
//!            (write lock on chunk i, read locks on all contrib buffers).
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
    #[arg(long, default_value_t = 0.85f32)]
    damping: f32,
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

    let all_edges_arc = Arc::new(all_edges);

    // Chunk size for rank partitioning: worker i owns ranks_slices[i]
    // covering nodes [i*chunk, min((i+1)*chunk, num_nodes)).
    let chunk = (num_nodes + num_workers - 1) / num_workers;

    // Per-worker rank slices (replaces single shared Arc<RwLock<Vec<f32>>>).
    // Scatter reads all slices concurrently; gather writes own slice in parallel.
    let ranks_slices: Arc<Vec<RwLock<Vec<f32>>>> = Arc::new(
        (0..num_workers)
            .map(|i| {
                let start = i * chunk;
                let end = (start + chunk).min(num_nodes);
                RwLock::new(vec![1.0f32 / num_nodes as f32; end - start])
            })
            .collect(),
    );

    // Per-worker contribution buffers (worker i writes contribs_list[i])
    let contribs_list: Arc<Vec<RwLock<Vec<f32>>>> = Arc::new(
        (0..num_workers)
            .map(|_| RwLock::new(vec![0.0f32; num_nodes]))
            .collect(),
    );

    // Barrier for synchronising scatter → gather phases
    let scatter_done = Arc::new(Barrier::new(num_workers));
    let gather_done = Arc::new(Barrier::new(num_workers));

    let t0 = Instant::now();

    timely::execute_from_args(
        vec![String::from("-w"), num_workers.to_string()].into_iter(),
        move |worker| {
            let wi = worker.index();
            let nw = worker.peers();

            // Build local CSR (only edges where src % nw == wi)
            let edges = &*all_edges_arc;
            let od = &*out_degrees;

            let local_edges: Vec<(u32, u32)> = edges
                .iter()
                .filter(|&&(src, _)| (src as usize % nw) == wi)
                .cloned()
                .collect();

            let mut offsets = vec![0usize; num_nodes + 1];
            for &(src, _) in &local_edges {
                offsets[src as usize + 1] += 1;
            }
            for i in 0..num_nodes {
                offsets[i + 1] += offsets[i];
            }
            let mut targets = vec![0u32; local_edges.len()];
            let mut pos = offsets[..num_nodes].to_vec();
            for &(src, dst) in &local_edges {
                targets[pos[src as usize]] = dst;
                pos[src as usize] += 1;
            }

            let rs_ref = ranks_slices.clone();
            let clist = contribs_list.clone();
            let bar_scatter = scatter_done.clone();
            let bar_gather = gather_done.clone();

            for _iter in 0..iterations {
                // --- Scatter: read current ranks, write own contribution buffer ---
                {
                    // All workers take read locks on ALL rank slices concurrently.
                    // Read-read is compatible: no blocking between workers.
                    let rank_reads: Vec<_> = rs_ref.iter().map(|r| r.read().unwrap()).collect();
                    let get_rank = |node: usize| -> f32 {
                        let si = node / chunk;
                        rank_reads[si][node - si * chunk]
                    };

                    let mut my_contribs = clist[wi].write().unwrap();
                    for v in my_contribs.iter_mut() {
                        *v = 0.0;
                    }

                    let mut src_node = wi;
                    while src_node < num_nodes {
                        let od_n = od[src_node];
                        if od_n > 0 {
                            let contrib = get_rank(src_node) / od_n as f32;
                            for &dst in &targets[offsets[src_node]..offsets[src_node + 1]] {
                                my_contribs[dst as usize] += contrib;
                            }
                        }
                        src_node += nw;
                    }
                } // rank read locks + my_contribs write lock dropped

                // Wait for all workers to finish scatter
                bar_scatter.wait();

                // --- Gather: each worker reduces its own rank chunk in parallel ---
                // Worker i writes only ranks_slices[i] covering [i*chunk, (i+1)*chunk).
                // All workers read all contrib buffers concurrently (read-read compatible).
                {
                    let clist_reads: Vec<_> =
                        clist.iter().map(|c| c.read().unwrap()).collect();
                    let start = wi * chunk;
                    let end = (start + chunk).min(num_nodes);
                    let base = (1.0 - damping) / num_nodes as f32;
                    let mut my_ranks = rs_ref[wi].write().unwrap();
                    for (j, v) in (start..end).enumerate() {
                        let total: f32 = clist_reads.iter().map(|c| c[v]).sum();
                        my_ranks[j] = base + damping * total;
                    }
                } // clist read locks + my_ranks write lock dropped

                // Wait for all workers to finish gather before next scatter
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

    append_graph_csv(
        &output,
        "timely",
        &dataset,
        num_workers,
        iterations,
        total_s,
        s_per_iter,
    );
}
