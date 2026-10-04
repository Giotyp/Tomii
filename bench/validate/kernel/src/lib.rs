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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Once;
use std::time::Instant;
use tomii_macro::tomii_export;

/// Seed value for a chain / fan-out root.
#[tomii_export]
pub fn init_x() -> f64 {
    1.0
}

/// Ground-truth call counter for the item-5 W=1/W=2 anomaly diagnosis
/// (2026-09-27, coordinator follow-up): independent of the runtime's own
/// telemetry (`total_tasks_per_frame`/`per_node[].invocations`), used to
/// confirm (rather than assume) that those figures accurately reflect how
/// few `busy_ns` invocations actually run per frame at low W — see
/// `RESULTS.md` item 5's "W=1/W=2... frame-completion race" note for the
/// real mechanism (a `pending_tasks`/`processing_count` race in
/// `tomii-core/src/runtime/{init,slot_lifecycle,task_execution}.rs` for
/// successor-less wide fan-out roots, not a bulk-dispatch recording gap).
/// This counter increments inside the kernel itself, so it counts every
/// real `busy_ns` invocation regardless of which dispatch path the runtime
/// used. Dumped to stderr at process exit via `libc::atexit` (registered
/// once, on the first call) as `BUSY_NS_CALLS=<n>`, so the harness can read
/// it from the subprocess's captured stderr without adding any node to the
/// graph (which would itself change the fan-out's elasticity decision).
static BUSY_CALLS: AtomicU64 = AtomicU64::new(0);
static ATEXIT_REGISTERED: Once = Once::new();

extern "C" fn dump_busy_calls() {
    eprintln!("BUSY_NS_CALLS={}", BUSY_CALLS.load(Ordering::Relaxed));
}

/// Busy-wait for `ns` nanoseconds, doing real (non-optimizable) FP work,
/// then return a value derived from the input so results can't be
/// constant-folded and the dependency edge is real.
#[tomii_export]
pub fn busy_ns(x: f64, ns: usize) -> f64 {
    ATEXIT_REGISTERED.call_once(|| unsafe {
        libc::atexit(dump_busy_calls);
    });
    BUSY_CALLS.fetch_add(1, Ordering::Relaxed);
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

/// Trivial sink for item 5's corrected graph (2026-09-27, coordinator
/// follow-up): a `$barrier`-only successor of the wide fan-out node,
/// gating frame completion on all `factor` fan-out instances via the
/// runtime's normal `pending_tasks`/edge-arrival counting instead of the
/// `is_initial`-exempt path a bare no-successor root takes (see
/// `RESULTS.md` item 5's frame-completion-race note). Takes no real
/// arguments -- the `$barrier` dependency is scheduling-only, like
/// `bench/anti-diag-bench/tomii/src/lib.rs`'s `wf_cell` chain.
#[tomii_export]
pub fn sink_probe() {}

// --- Root-validate additions (2026-09-27): condition evaluator for the
// conditional-root / conditional-successor graphs in
// bench/root-validate/root_validate.py. `busy_ns` above is byte-identical to
// the item-5 kernel, so throughput numbers are directly comparable.

/// Condition evaluator: `x > thr`. With `thr` a graph variable the harness
/// makes every instance pass (thr = -1e300) or fail (thr = +1e300).
#[tomii_export]
pub fn cond_gt(x: f64, thr: f64) -> bool {
    x > thr
}
