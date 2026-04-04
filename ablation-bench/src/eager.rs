#![allow(clippy::inline_always)] // performance-critical hot path; mirrors distributed.rs
#![allow(clippy::doc_markdown)] // "TaskFlow" is a proper noun, not a code identifier

use std::sync::atomic::{AtomicU32, Ordering};

/// Eager O(N) reset — TaskFlow's approach.
///
/// Between sweeps, every node's dependency counter must be written back to
/// its initial predecessor count before the next sweep can start.  This
/// mirrors `Node::_set_up_join_counter()` from TaskFlow's
/// `taskflow/core/graph.hpp`, which iterates over all dependents and calls
/// `_join_counter.store(count, memory_order_relaxed)` on each.
///
/// The O(N) [`EagerResolution::reset`] here is the direct analogue.
/// The hot-path [`EagerResolution::decrement`] is identical to
/// [`crate::distributed::DistributedResolution`]'s: a single `fetch_sub(AcqRel)`.
///
/// This struct exists as a distinct type (rather than a rename of
/// `DistributedResolution`) so that `reset_bench` can time the reset step
/// independently and label its output unambiguously.
pub struct EagerResolution {
    dep_remaining: Vec<AtomicU32>,
    initial_deps: Vec<u32>,
}

impl EagerResolution {
    /// Create a new [`EagerResolution`] from per-node predecessor counts.
    pub fn new(pred_counts: &[u32]) -> Self {
        let dep_remaining = pred_counts
            .iter()
            .map(|&d| AtomicU32::new(d))
            .collect();
        Self {
            dep_remaining,
            initial_deps: pred_counts.to_vec(),
        }
    }

    /// O(N) reset: write every dep counter back to its initial value.
    ///
    /// This is the cost that TaskFlow pays between re-runs.  Called
    /// synchronously before each timed sweep in `run_sweeps_eager`.
    #[inline(never)] // keep visible in profiles; not on the task hot path
    pub fn reset(&self) {
        for (atomic, &init) in self.dep_remaining.iter().zip(self.initial_deps.iter()) {
            atomic.store(init, Ordering::Relaxed);
        }
    }

    /// Decrement `succ_id`'s counter inline (same as `DistributedResolution`).
    ///
    /// Returns `true` iff the counter reached zero (successor is ready).
    #[inline(always)]
    pub fn decrement(&self, succ_id: usize) -> bool {
        self.dep_remaining[succ_id].fetch_sub(1, Ordering::AcqRel) == 1
    }
}
