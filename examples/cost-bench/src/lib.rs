pub mod graph_io;
pub mod pagerank;

use graph_io::{CsrGraph, PartitionedEdges};
use pagerank::{pr_gather, pr_scatter};
use synstream_types::CmTypes;

// ---------------------------------------------------------------------------
// Initialization functions
// ---------------------------------------------------------------------------

/// Load a SNAP-format graph from a file path.
///
/// The CmTypes::String argument may be either:
///   - A bare file path, e.g. "/data/snap/twitter.txt"
///   - An environment-variable name whose value holds the path
#[no_mangle]
pub fn load_graph_cm(path_cm: &CmTypes) -> CmTypes {
    let path = match path_cm {
        CmTypes::String(s) => std::env::var(s.as_ref()).unwrap_or_else(|_| s.to_string()),
        _ => panic!("load_graph_cm: expected String argument"),
    };
    CmTypes::from_any(CsrGraph::from_snap(&path))
}

/// Create an initial uniform rank vector (1/N for all nodes).
#[no_mangle]
pub fn create_ranks_cm(graph: &CmTypes) -> CmTypes {
    graph
        .with_any(|g: &CsrGraph| {
            let initial = 1.0f32 / g.num_nodes as f32;
            CmTypes::from_any(vec![initial; g.num_nodes])
        })
        .expect("create_ranks_cm: expected CsrGraph")
}

/// Extract partition `idx` of `n_parts` from the graph's edge list.
#[no_mangle]
pub fn get_partition_cm(graph: &CmTypes, idx: usize, n_parts: usize) -> CmTypes {
    graph
        .with_any(|g: &CsrGraph| CmTypes::from_any(PartitionedEdges::from_graph(g, idx, n_parts)))
        .expect("get_partition_cm: expected CsrGraph")
}

// ---------------------------------------------------------------------------
// Compute functions
// ---------------------------------------------------------------------------

/// Scatter: compute per-edge contributions for this partition.
/// Returns a Vec<f32> of length `num_nodes`.
#[no_mangle]
pub fn pr_scatter_cm(partition: &CmTypes, graph: &CmTypes, ranks: &CmTypes) -> CmTypes {
    partition
        .with_any(|part: &PartitionedEdges| {
            graph
                .with_any(|g: &CsrGraph| {
                    ranks
                        .with_any(|r: &Vec<f32>| CmTypes::from_any(pr_scatter(part, g, r)))
                        .expect("pr_scatter_cm: ranks must be Vec<f32>")
                })
                .expect("pr_scatter_cm: graph must be CsrGraph")
        })
        .expect("pr_scatter_cm: partition must be PartitionedEdges")
}

/// Gather: aggregate all scatter contributions and update ranks in-place.
///
/// Wrapper passes arguments as:
///   args[0]   = ranks   (CmTypes::Any containing Vec<f32>, updated in place)
///   args[1]   = damping (CmTypes::F64)
///   args[2..] = per-worker contribution vecs (CmTypes::Any containing Vec<f32>)
#[no_mangle]
pub fn pr_gather_cm(ranks: &CmTypes, damping: f64, contribs: &[CmTypes]) -> CmTypes {
    let d = damping as f32;

    // Clone each worker's contribution Vec<f32> before acquiring the mutable
    // rank lock, so we hold at most one write lock at a time.
    let cloned: Vec<Vec<f32>> = contribs
        .iter()
        .map(|c| {
            c.with_any(|v: &Vec<f32>| v.clone())
                .expect("pr_gather_cm: contribution must be Vec<f32>")
        })
        .collect();

    let borrowed: Vec<&Vec<f32>> = cloned.iter().collect();

    ranks
        .with_any_mut(|rank_vec: &mut Vec<f32>| {
            pr_gather(rank_vec, d, &borrowed);
        })
        .expect("pr_gather_cm: ranks must be Vec<f32>");

    CmTypes::None
}
