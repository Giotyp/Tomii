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

/// Partial gather: sum ALL scatter contributions for the node range owned by instance `idx`.
///
/// Returns a `Vec<f32>` of length `end - start` (≤ chunk) where:
///   `start = idx * chunk`, `end = min(start + chunk, n_nodes)`.
pub fn pr_partial_gather(
    idx: usize,
    n_workers: usize,
    all_contribs: &[&Vec<f32>],
) -> Vec<f32> {
    let n_nodes = all_contribs[0].len();
    let chunk = (n_nodes + n_workers - 1) / n_workers;
    let start = idx * chunk;
    let end = (start + chunk).min(n_nodes);
    let mut partial = vec![0.0f32; end - start];
    for contrib in all_contribs {
        for (j, &v) in contrib[start..end].iter().enumerate() {
            partial[j] += v;
        }
    }
    partial
}

/// Reduce: apply the PageRank formula and write new ranks from N partial sums.
///
/// `partial_sums[i]` covers nodes `[i*chunk, (i+1)*chunk)`.
/// `ranks` is updated in place.
pub fn pr_reduce(ranks: &mut Vec<f32>, damping: f32, partial_sums: &[&Vec<f32>]) {
    let n = ranks.len();
    let base = (1.0 - damping) / n as f32;
    let n_workers = partial_sums.len();
    let chunk = (n + n_workers - 1) / n_workers;
    for (i, ps) in partial_sums.iter().enumerate() {
        let start = i * chunk;
        for (j, &v) in ps.iter().enumerate() {
            ranks[start + j] = base + damping * v;
        }
    }
}

/// Parallel reduce: apply the PageRank formula for one node-range chunk.
///
/// Instance `idx` writes `ranks_ptr[start..end]` where `start = idx * chunk`.
/// All instances write to non-overlapping ranges → safe for concurrent execution.
///
/// # Safety
/// - All instances must write to non-overlapping ranges `[idx*chunk, (idx+1)*chunk)`.
/// - No other code may concurrently read or write `ranks_ptr` while this executes.
/// - The pointer must remain valid (Vec not reallocated) for the call duration.
pub unsafe fn pr_reduce_partial(
    idx: usize,
    n_workers: usize,
    ranks_ptr: *mut f32,
    n_nodes: usize,
    damping: f32,
    partial_sum: &[f32],
) {
    let chunk = (n_nodes + n_workers - 1) / n_workers;
    let start = idx * chunk;
    let base = (1.0 - damping) / n_nodes as f32;
    // Use partial_sum.len() as the authoritative write length.
    // With 1:1 factor mapping, partial_sum covers exactly [idx*chunk, (idx+1)*chunk)
    // and its length equals end-start (handling the shorter last chunk correctly).
    for (j, &v) in partial_sum.iter().enumerate() {
        *ranks_ptr.add(start + j) = base + damping * v;
    }
}
