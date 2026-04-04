#![allow(clippy::doc_markdown)] // "TaskFlow" is a proper noun, not a code identifier

use std::hint::black_box;
use std::time::Instant;

use crate::eager::EagerResolution;
use crate::generational::GenerationalResolution;

/// Build a flat predecessor-count array for `n_nodes` nodes.
///
/// The exact topology does not matter for reset-cost measurement.  We use a
/// simple "all interior nodes have 2 predecessors, boundary nodes have 1 or 0"
/// pattern derived from a `√n × √n` wavefront.  If `n_nodes` is not a perfect
/// square we fall back to a linear chain (each node except the first has 1
/// predecessor).
fn build_pred_counts(n_nodes: usize) -> Vec<u32> {
    // Integer approximation of √n_nodes.  The truncation and sign-loss are
    // intentional: sqrt of a positive f64 is non-negative and the value fits
    // in usize for all reasonable n_nodes values.
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let side = (n_nodes as f64).sqrt() as usize;

    if side * side == n_nodes {
        // Wavefront topology: node (i,j) has up to 2 predecessors.
        let mut pred_counts = vec![0u32; n_nodes];
        for i in 0..side {
            for j in 0..side {
                let id = i * side + j;
                if i > 0 {
                    pred_counts[id] += 1;
                }
                if j > 0 {
                    pred_counts[id] += 1;
                }
            }
        }
        pred_counts
    } else {
        // Linear chain fallback.
        let mut pred_counts = vec![1u32; n_nodes];
        pred_counts[0] = 0;
        pred_counts
    }
}

/// Measure the cost of the inter-sweep reset operation for both strategies.
///
/// Returns `(mean_ns_generational, mean_ns_eager)`.
///
/// Only the reset step itself is timed — not task execution, not graph
/// traversal.  This isolates the O(1) `bump_generation` vs the O(N) loop
/// of [`EagerResolution::reset`].
///
/// - Generational timing measures a single `fetch_add` on one `AtomicU32`.
/// - Eager timing measures N atomic stores (`Relaxed`), matching TaskFlow's
///   `_set_up_join_counter` loop.
///
/// To prevent the optimiser from eliding either loop, `black_box` is applied
/// to the structures themselves so their contents are considered observable.
pub fn bench_reset_cost(n_nodes: usize, iterations: usize) -> (f64, f64) {
    let pred_counts = build_pred_counts(n_nodes);

    let gen_res = GenerationalResolution::new(&pred_counts);
    let eager_res = EagerResolution::new(&pred_counts);

    // ── Generational: time bump_generation only ───────────────────────────
    let mut gen_total_ns: u128 = 0;
    for _ in 0..iterations {
        // black_box prevents the call from being sunk outside the timing window.
        let t0 = Instant::now();
        black_box(&gen_res).bump_generation();
        gen_total_ns += t0.elapsed().as_nanos();
    }

    // ── Eager: time the full O(N) reset loop ─────────────────────────────
    let mut eager_total_ns: u128 = 0;
    for _ in 0..iterations {
        let t0 = Instant::now();
        black_box(&eager_res).reset();
        eager_total_ns += t0.elapsed().as_nanos();
    }

    // Precision loss is acceptable here: nanosecond totals for benchmark
    // reporting fit well within f64 mantissa for all practical iteration counts.
    #[allow(clippy::cast_precision_loss)]
    let mean_gen = gen_total_ns as f64 / iterations as f64;
    #[allow(clippy::cast_precision_loss)]
    let mean_eager = eager_total_ns as f64 / iterations as f64;

    (mean_gen, mean_eager)
}
