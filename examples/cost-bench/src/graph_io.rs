use std::io::{BufRead, BufReader};
use std::fs::File;

/// Compressed Sparse Row (CSR) representation of a directed graph.
#[derive(Clone)]
pub struct CsrGraph {
    pub num_nodes: usize,
    pub num_edges: usize,
    /// offsets[i]..offsets[i+1] is the range of out-edges for node i
    pub offsets: Vec<u32>,
    /// destination node for each edge
    pub targets: Vec<u32>,
    /// out-degree[i] = offsets[i+1] - offsets[i]
    pub out_degrees: Vec<u32>,
}

impl CsrGraph {
    /// Parse a SNAP edge-list file into CSR form.
    ///
    /// Lines starting with `#` are treated as comments.
    /// Each data line must be `"src<whitespace>dst"`.
    pub fn from_snap(path: &str) -> Self {
        let file = File::open(path)
            .unwrap_or_else(|e| panic!("load_graph: cannot open '{}': {}", path, e));
        let reader = BufReader::new(file);

        let mut raw_edges: Vec<(u32, u32)> = Vec::new();
        let mut max_node: u32 = 0;

        for line in reader.lines() {
            let line = line.expect("I/O error reading edge list");
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let mut parts = trimmed.split_whitespace();
            let src: u32 = parts
                .next()
                .expect("missing src")
                .parse()
                .unwrap_or_else(|_| panic!("non-integer src in: {}", trimmed));
            let dst: u32 = parts
                .next()
                .expect("missing dst")
                .parse()
                .unwrap_or_else(|_| panic!("non-integer dst in: {}", trimmed));
            max_node = max_node.max(src).max(dst);
            raw_edges.push((src, dst));
        }

        let num_nodes = (max_node + 1) as usize;
        let num_edges = raw_edges.len();

        // Count out-degrees
        let mut out_degrees = vec![0u32; num_nodes];
        for &(src, _) in &raw_edges {
            out_degrees[src as usize] += 1;
        }

        // Build prefix-sum offset array
        let mut offsets = vec![0u32; num_nodes + 1];
        for i in 0..num_nodes {
            offsets[i + 1] = offsets[i] + out_degrees[i];
        }

        // Fill CSR target array
        let mut targets = vec![0u32; num_edges];
        let mut write_pos = offsets[..num_nodes].to_vec(); // per-node write cursor
        for &(src, dst) in &raw_edges {
            let p = write_pos[src as usize] as usize;
            targets[p] = dst;
            write_pos[src as usize] += 1;
        }

        CsrGraph { num_nodes, num_edges, offsets, targets, out_degrees }
    }
}

/// A contiguous slice of the graph's edges assigned to one worker.
#[derive(Clone)]
pub struct PartitionedEdges {
    /// The (src, dst) pairs owned by this partition
    pub edges: Vec<(u32, u32)>,
    /// Total number of nodes in the graph (needed for scatter output sizing)
    pub num_nodes: usize,
}

impl PartitionedEdges {
    /// Extract partition `idx` out of `n_parts` from the graph's flat edge list.
    pub fn from_graph(graph: &CsrGraph, idx: usize, n_parts: usize) -> Self {
        let n = graph.num_edges;
        let chunk = (n + n_parts - 1) / n_parts;
        let start = idx * chunk;
        let end = (start + chunk).min(n);

        let mut edges = Vec::with_capacity(end.saturating_sub(start));
        let mut edge_cursor = 0usize;
        'outer: for src in 0..graph.num_nodes {
            let e_start = graph.offsets[src] as usize;
            let e_end = graph.offsets[src + 1] as usize;
            for tgt_pos in e_start..e_end {
                if edge_cursor >= start && edge_cursor < end {
                    edges.push((src as u32, graph.targets[tgt_pos]));
                }
                edge_cursor += 1;
                if edge_cursor >= end {
                    break 'outer;
                }
            }
        }

        PartitionedEdges { edges, num_nodes: graph.num_nodes }
    }
}
