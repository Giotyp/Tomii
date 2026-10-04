//! Shared kernel for the runtime-level microbenchmarks (items 2 and 5):
//! slot scaling (linear chain) and scheduler/ready-queue scalability
//! (wide fan-out).
//!
//! `busy_ns` is a self-calibrating busy-wait: it spins on `Instant::elapsed`
//! until at least `ns` nanoseconds have passed, so task granularity is
//! portable across machines without hand-tuning an iteration count to a
//! specific clock speed. The `Instant::now()` polling overhead (~20-40ns
//! per syscall-free `clock_gettime` on this kernel) is negligible next to
//! the microsecond-scale task sizes used in both experiments (>=500ns).

#![allow(improper_ctypes_definitions)]

use std::time::Instant;
use tomii_macro::tomii_export;

/// Seed value for a chain / fan-out root.
#[tomii_export]
pub fn init_x() -> f64 {
    1.0
}

/// Busy-wait for `ns` nanoseconds, doing real (non-optimizable) FP work,
/// then return a value derived from the input so results can't be
/// constant-folded and the dependency edge is real.
#[tomii_export]
pub fn busy_ns(x: f64, ns: usize) -> f64 {
    let start = Instant::now();
    let mut acc = x;
    let target = ns as u128;
    loop {
        // A handful of FP ops per poll keeps the Instant::now() call from
        // dominating at the smallest (500ns) task size while still checking
        // often enough to hit the target duration accurately.
        for _ in 0..8 {
            acc = acc.mul_add(1.000000119, 0.000000119);
        }
        if start.elapsed().as_nanos() >= target {
            break;
        }
    }
    acc
}
