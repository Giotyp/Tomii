use std::sync::atomic::{AtomicU32, Ordering};

/// Distributed, per-node dependency resolution.
///
/// Each node has its own `AtomicU32` counter initialised to its predecessor count.
/// The completing worker thread atomically decrements the counter inline — no coordinator
/// thread, no lock, no cross-thread message. This is the Resolve technique from §3.2.
///
/// For the single-predecessor case (most wavefront nodes), resolution reduces to:
///   1 fetch_sub (AcqRel) + 1 comparison
pub struct DistributedResolution {
    dep_remaining: Vec<AtomicU32>,
    initial_deps: Vec<u32>,
}

impl DistributedResolution {
    pub fn new(pred_counts: &[u32]) -> Self {
        let dep_remaining = pred_counts.iter().map(|&d| AtomicU32::new(d)).collect();
        Self { dep_remaining, initial_deps: pred_counts.to_vec() }
    }

    /// Reset all counters to initial values. Called between sweeps (outside timing).
    pub fn reset(&self) {
        for (atomic, &init) in self.dep_remaining.iter().zip(self.initial_deps.iter()) {
            atomic.store(init, Ordering::Relaxed);
        }
    }

    /// Decrement `succ_id`'s counter inline. Returns `true` iff this completion
    /// made the successor ready (counter reached zero).
    ///
    /// Called directly on the completing worker thread — no coordinator involved.
    #[inline(always)]
    pub fn decrement(&self, succ_id: usize) -> bool {
        // AcqRel: the decrement that reaches 0 establishes happens-before with
        // the successor's first memory access. All other decrements are pure Release.
        self.dep_remaining[succ_id].fetch_sub(1, Ordering::AcqRel) == 1
    }
}
