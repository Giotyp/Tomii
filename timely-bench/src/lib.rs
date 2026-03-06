//! Shared utilities for timely-bench: SNAP graph parser and CSV timing writer.

use std::fs::File;
use std::io::{BufRead, BufReader, Write};

/// Parse a SNAP-format edge list into (num_nodes, edges: Vec<(u32, u32)>).
///
/// Lines starting with '#' are treated as comments.
pub fn parse_snap(path: &str) -> (usize, Vec<(u32, u32)>) {
    let file = File::open(path).unwrap_or_else(|e| panic!("Cannot open '{}': {}", path, e));
    let reader = BufReader::new(file);

    let mut edges = Vec::new();
    let mut max_node: u32 = 0;

    for line in reader.lines() {
        let line = line.expect("I/O error");
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let mut parts = trimmed.split_whitespace();
        let src: u32 = parts.next().expect("missing src").parse()
            .unwrap_or_else(|_| panic!("non-integer src: {}", trimmed));
        let dst: u32 = parts.next().expect("missing dst").parse()
            .unwrap_or_else(|_| panic!("non-integer dst: {}", trimmed));
        max_node = max_node.max(src).max(dst);
        edges.push((src, dst));
    }

    ((max_node + 1) as usize, edges)
}

/// Append one CSV row to `path`, creating the file with a header if needed.
pub fn append_csv(
    path: &str,
    system: &str,
    kernel: &str,
    array_size: usize,
    workers: usize,
    elapsed_s: f64,
    gb_s: f64,
) {
    let needs_header = !std::path::Path::new(path).exists();
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap_or_else(|e| panic!("Cannot open CSV '{}': {}", path, e));

    if needs_header {
        writeln!(file, "system,kernel,array_size,workers,elapsed_s,gb_s")
            .expect("write header");
    }
    writeln!(file, "{},{},{},{},{:.6},{:.3}", system, kernel, array_size, workers, elapsed_s, gb_s)
        .expect("write row");
}

/// Append one CSV row for wavefront benchmark results.
pub fn append_wavefront_csv(
    path: &str,
    system: &str,
    n: usize,
    workers: usize,
    iterations: usize,
    total_s: f64,
    s_per_iter: f64,
) {
    let needs_header = !std::path::Path::new(path).exists();
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap_or_else(|e| panic!("Cannot open CSV '{}': {}", path, e));

    if needs_header {
        writeln!(file, "system,n,workers,iterations,total_s,s_per_iter")
            .expect("write header");
    }
    writeln!(
        file, "{},{},{},{},{:.6},{:.6}",
        system, n, workers, iterations, total_s, s_per_iter
    )
    .expect("write row");
}

/// Append one CSV row for graph benchmark results.
pub fn append_graph_csv(
    path: &str,
    system: &str,
    dataset: &str,
    workers: usize,
    iterations: usize,
    total_s: f64,
    s_per_iter: f64,
) {
    let needs_header = !std::path::Path::new(path).exists();
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap_or_else(|e| panic!("Cannot open CSV '{}': {}", path, e));

    if needs_header {
        writeln!(file, "system,dataset,workers,iterations,total_s,s_per_iter")
            .expect("write header");
    }
    writeln!(
        file, "{},{},{},{},{:.6},{:.6}",
        system, dataset, workers, iterations, total_s, s_per_iter
    )
    .expect("write row");
}
