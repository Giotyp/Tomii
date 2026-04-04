#![allow(clippy::inline_always)] // performance-critical hot path; mirrors distributed.rs
#![allow(clippy::doc_markdown)] // "SynStream" is a proper noun, not a code identifier

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

// ── Bit-packing helpers ───────────────────────────────────────────────────

/// Pack a `(generation, value)` pair into a single `u64`.
/// Upper 32 bits = generation; lower 32 bits = value.
#[inline(always)]
const fn gen_pack(gen: u32, val: u32) -> u64 {
    ((gen as u64) << 32) | (val as u64)
}

#[inline(always)]
const fn gen_unpack_gen(p: u64) -> u32 {
    (p >> 32) as u32
}

#[inline(always)]
const fn gen_unpack_val(p: u64) -> u32 {
    // Lower 32 bits: truncation is intentional (we stored a u32 there).
    #[allow(clippy::cast_possible_truncation)]
    { p as u32 }
}

// ── GenerationalResolution ────────────────────────────────────────────────

/// Generational O(1) slot reset — SynStream's approach.
///
/// Each slot stores a packed `(generation: u32, dep_remaining: u32)` in an
/// `AtomicU64`. A single global `AtomicU32` tracks the current generation.
///
/// Between sweeps, the only inter-sweep bookkeeping is a single
/// `fetch_add(1)` on `current_generation` (`bump_generation`).  Individual
/// slots are NOT reset eagerly; instead each `decrement` call lazily
/// reinitialises the slot's counter from `initial_deps` when it detects a
/// generation mismatch — effectively amortising the O(N) cost across N
/// concurrent first-access events at the start of each sweep rather than
/// paying it synchronously before the sweep starts.
///
/// This mirrors the packed-(gen,val) layout used in SynStream's
/// `resolution_state.rs`.
pub struct GenerationalResolution {
    /// Packed `(generation, dep_remaining)` per node.
    dep_packed: Vec<AtomicU64>,
    /// Baseline predecessor counts; used for lazy reinit on generation mismatch.
    initial_deps: Vec<u32>,
    /// Monotonically increasing generation counter.
    current_generation: AtomicU32,
}

impl GenerationalResolution {
    /// Build a new [`GenerationalResolution`] from a slice of per-node predecessor counts.
    ///
    /// Generation starts at 1; all slots are initialised to generation 0 so that the
    /// first `decrement` call on any slot will trigger a lazy reinit.
    pub fn new(pred_counts: &[u32]) -> Self {
        let dep_packed = pred_counts
            .iter()
            .map(|&c| AtomicU64::new(gen_pack(0, c)))
            .collect();
        Self {
            dep_packed,
            initial_deps: pred_counts.to_vec(),
            current_generation: AtomicU32::new(1),
        }
    }

    /// Advance the generation counter by one.
    ///
    /// This is the complete inter-sweep reset operation: O(1), a single `fetch_add`
    /// on one cache line.
    #[inline(always)]
    pub fn bump_generation(&self) {
        self.current_generation.fetch_add(1, Ordering::SeqCst);
    }

    /// Return the current generation value.
    #[inline(always)]
    pub fn generation(&self) -> u32 {
        self.current_generation.load(Ordering::Acquire)
    }

    /// Decrement the dependency counter of `succ_id` for `current_gen`.
    ///
    /// If the stored generation does not match `current_gen`, the slot is
    /// treated as having its full initial predecessor count (lazy reinit).
    ///
    /// Returns `true` iff this decrement made the counter reach zero, meaning
    /// the successor is now ready to execute.
    ///
    /// Uses a CAS loop so that concurrent decrements from multiple worker
    /// threads are safe without additional locks.
    #[inline(always)]
    pub fn decrement(&self, succ_id: usize, current_gen: u32) -> bool {
        let pred_count = self.initial_deps[succ_id];
        loop {
            let old = self.dep_packed[succ_id].load(Ordering::Acquire);
            let stored_gen = gen_unpack_gen(old);

            // Lazy reinit: if the stored generation is stale, treat the slot
            // as fully unresolved (pred_count predecessors remaining).
            let current_val = if stored_gen == current_gen {
                gen_unpack_val(old)
            } else {
                pred_count
            };

            // Guard against a race where two threads both see a stale slot and
            // both attempt the reinit — only one CAS will succeed; the loser
            // retries and sees the already-updated value.
            if current_val == 0 {
                // Already reached zero in this generation (shouldn't fire in a
                // correct graph, but guard defensively).
                return false;
            }

            let new_val = current_val - 1;
            let new_packed = gen_pack(current_gen, new_val);

            // AcqRel: the decrement that drives `new_val` to 0 establishes
            // happens-before with the successor's first memory access.
            if self
                .dep_packed[succ_id]
                .compare_exchange(old, new_packed, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return new_val == 0;
            }
            // CAS failed — another thread raced us; retry.
        }
    }
}
