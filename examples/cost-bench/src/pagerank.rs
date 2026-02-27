use crate::graph_io::{CsrGraph, PartitionedEdges};

/// Scatter phase: compute this worker's contribution to each node's new rank.
///
/// For every edge (src → dst) in `partition`, accumulates `rank[src] / out_degree[src]`
/// into `contributions[dst]`. Returns a Vec<f32> of length `num_nodes`.
pub fn pr_scatter(partition: &PartitionedEdges, graph: &CsrGraph, ranks: &[f32]) -> Vec<f32> {
    let n = partition.num_nodes;
    let mut contributions = vec![0.0f32; n];
    for &(src, dst) in &partition.edges {
        let out_deg = graph.out_degrees[src as usize];
        if out_deg > 0 {
            contributions[dst as usize] += ranks[src as usize] / out_deg as f32;
        }
    }
    contributions
}

/// Gather phase: aggregate all worker contributions and write new ranks in-place.
///
/// `new_rank[v] = (1 - damping) / N + damping * sum_contributions[v]`
///
/// `all_contribs[k]` is the contribution vector from worker k.
/// `ranks` is updated in place.
pub fn pr_gather(ranks: &mut Vec<f32>, damping: f32, all_contribs: &[&Vec<f32>]) {
    let n = ranks.len();
    let base = (1.0 - damping) / n as f32;

    // Sum contributions across all workers
    let mut total = vec![0.0f32; n];
    for worker_contribs in all_contribs {
        for (i, &v) in worker_contribs.iter().enumerate() {
            total[i] += v;
        }
    }

    // Write new ranks
    for i in 0..n {
        ranks[i] = base + damping * total[i];
    }
}
