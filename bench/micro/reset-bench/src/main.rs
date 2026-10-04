//! Generational (lazy) slot reset vs. eager (Taskflow-style) slot reset.
//!
//! Mirrors the real `tomii-core` data structures (see
//! `tomii-core/src/buffers/node_dep.rs::NodeDependencyEntry` and
//! `tomii-core/src/runtime/slot_lifecycle.rs::reset_slot_state`), reproduced
//! here because `tomii-core::buffers` is `pub(crate)` and this harness must
//! not modify `tomii-core` visibility to link against it directly (out of
//! scope per the eval protocol). The packing scheme, atomic types, and
//! memory orderings below are copied verbatim from the real implementation:
//!
//!   - packed u64 = (gen: u32 << 32) | (value: u32), see `gen_pack` /
//!     `gen_unpack_gen` / `gen_unpack_val` in `tomii-core/src/buffers/mod.rs`
//!   - reset = single `generation.fetch_add(1, SeqCst)`
//!     (`reset_slot_state`, slot_lifecycle.rs:248)
//!   - first touch after a generation bump = `fetch_update(SeqCst, SeqCst, ..)`
//!     that compares the stored generation and lazily reinitialises to the
//!     initial value on mismatch (`decrease_and_get_ready_into`,
//!     node_dep.rs:167-178)
//!
//! The eager baseline models the Taskflow-style approach explicitly named in
//! the task: N separate atomic stores at reset time (one per dependency
//! entry), then a plain fetch_sub on first touch (no generation check
//! needed because the store already reinitialised the value).
//!
//! Both paths use the same atomic RMW class (`fetch_update` and `fetch_sub`
//! are both single-instruction-ish CAS/RMW ops on x86_64) so the comparison
//! isolates the *reset-time* cost, not an apples-to-oranges op mismatch.

use std::env;
use std::hint::black_box;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Instant;

const INIT_VAL: u32 = 1_000_000;

#[inline(always)]
fn gen_pack(gen: u32, val: u32) -> u64 {
    ((gen as u64) << 32) | (val as u64)
}
#[inline(always)]
fn gen_unpack_gen(packed: u64) -> u32 {
    (packed >> 32) as u32
}
#[inline(always)]
fn gen_unpack_val(packed: u64) -> u32 {
    packed as u32
}

/// Mirrors `NodeDependencyEntry.remaining_deps` + `slot_data.generation[slot]`.
struct GenSlot {
    generation: AtomicU32,
    entries: Vec<AtomicU64>,
}

impl GenSlot {
    fn new(n: usize) -> Self {
        Self {
            generation: AtomicU32::new(0),
            entries: (0..n).map(|_| AtomicU64::new(gen_pack(0, INIT_VAL))).collect(),
        }
    }

    /// Mirrors `reset_slot_state`: one fetch_add, O(1) regardless of N.
    #[inline(always)]
    fn reset(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
    }

    /// Mirrors the lazy-reinit fetch_update in `decrease_and_get_ready_into`.
    #[inline(always)]
    fn touch(&self, idx: usize) -> u32 {
        let slot_gen = self.generation.load(Ordering::SeqCst);
        let prev = self.entries[idx]
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |packed| {
                let stored_gen = gen_unpack_gen(packed);
                let cur = if stored_gen == slot_gen {
                    gen_unpack_val(packed)
                } else {
                    INIT_VAL
                };
                Some(gen_pack(slot_gen, cur.wrapping_sub(1)))
            })
            .unwrap();
        let stored_gen = gen_unpack_gen(prev);
        if stored_gen == slot_gen {
            gen_unpack_val(prev)
        } else {
            INIT_VAL
        }
    }
}

/// Taskflow-style eager reset: N independent atomic counters, no generation.
struct EagerSlot {
    entries: Vec<AtomicU32>,
}

impl EagerSlot {
    fn new(n: usize) -> Self {
        Self {
            entries: (0..n).map(|_| AtomicU32::new(INIT_VAL)).collect(),
        }
    }

    /// N atomic stores — the reset step the task explicitly asks to measure.
    ///
    /// Uses `Release`: the minimum ordering a correct single-writer reset
    /// needs (workers `Acquire`-load the counter before touching it). This
    /// is the fair choice for Taskflow-style resets — they do not need
    /// `SeqCst`'s total-store-order guarantee. See RESULTS.md for the
    /// `SeqCst` sensitivity figure (~17x slower at N=16384; store+mfence
    /// vs a plain `mov`).
    #[inline(always)]
    fn reset(&self) {
        for e in &self.entries {
            e.store(INIT_VAL, Ordering::Release);
        }
    }

    /// `SeqCst` sensitivity variant — same op, worst-case ordering.
    #[inline(always)]
    fn reset_seqcst(&self) {
        for e in &self.entries {
            e.store(INIT_VAL, Ordering::SeqCst);
        }
    }

    #[inline(always)]
    fn touch(&self, idx: usize) -> u32 {
        self.entries[idx].fetch_sub(1, Ordering::AcqRel)
    }

    #[inline(always)]
    fn touch_seqcst(&self, idx: usize) -> u32 {
        self.entries[idx].fetch_sub(1, Ordering::SeqCst)
    }
}

struct Sample {
    n: usize,
    reset_ns_per_call: f64,
    touch_ns_per_entry: f64,
    total_ns_per_frame: f64,
}

fn bench_gen(n: usize, frames: usize, warmup: usize) -> Sample {
    let slot = GenSlot::new(n);
    for _ in 0..warmup {
        slot.reset();
        for i in 0..n {
            black_box(slot.touch(i));
        }
    }
    let mut t_reset_ns = 0u128;
    let mut t_touch_ns = 0u128;
    for _ in 0..frames {
        let t0 = Instant::now();
        slot.reset();
        t_reset_ns += t0.elapsed().as_nanos();

        let t1 = Instant::now();
        for i in 0..n {
            black_box(slot.touch(black_box(i)));
        }
        t_touch_ns += t1.elapsed().as_nanos();
    }
    Sample {
        n,
        reset_ns_per_call: t_reset_ns as f64 / frames as f64,
        touch_ns_per_entry: t_touch_ns as f64 / (frames * n) as f64,
        total_ns_per_frame: (t_reset_ns + t_touch_ns) as f64 / frames as f64,
    }
}

fn bench_eager(n: usize, frames: usize, warmup: usize) -> Sample {
    let slot = EagerSlot::new(n);
    for _ in 0..warmup {
        slot.reset();
        for i in 0..n {
            black_box(slot.touch(i));
        }
    }
    let mut t_reset_ns = 0u128;
    let mut t_touch_ns = 0u128;
    for _ in 0..frames {
        let t0 = Instant::now();
        slot.reset();
        t_reset_ns += t0.elapsed().as_nanos();

        let t1 = Instant::now();
        for i in 0..n {
            black_box(slot.touch(black_box(i)));
        }
        t_touch_ns += t1.elapsed().as_nanos();
    }
    Sample {
        n,
        reset_ns_per_call: t_reset_ns as f64 / frames as f64,
        touch_ns_per_entry: t_touch_ns as f64 / (frames * n) as f64,
        total_ns_per_frame: (t_reset_ns + t_touch_ns) as f64 / frames as f64,
    }
}

fn bench_eager_seqcst(n: usize, frames: usize, warmup: usize) -> Sample {
    let slot = EagerSlot::new(n);
    for _ in 0..warmup {
        slot.reset_seqcst();
        for i in 0..n {
            black_box(slot.touch_seqcst(i));
        }
    }
    let mut t_reset_ns = 0u128;
    let mut t_touch_ns = 0u128;
    for _ in 0..frames {
        let t0 = Instant::now();
        slot.reset_seqcst();
        t_reset_ns += t0.elapsed().as_nanos();

        let t1 = Instant::now();
        for i in 0..n {
            black_box(slot.touch_seqcst(black_box(i)));
        }
        t_touch_ns += t1.elapsed().as_nanos();
    }
    Sample {
        n,
        reset_ns_per_call: t_reset_ns as f64 / frames as f64,
        touch_ns_per_entry: t_touch_ns as f64 / (frames * n) as f64,
        total_ns_per_frame: (t_reset_ns + t_touch_ns) as f64 / frames as f64,
    }
}

fn correctness_check() {
    // Generational: after reset(), the first touch on any entry must observe
    // INIT_VAL regardless of how many times it was touched in a prior generation.
    let g = GenSlot::new(4);
    assert_eq!(g.touch(0), INIT_VAL);
    assert_eq!(g.touch(0), INIT_VAL - 1);
    g.reset();
    assert_eq!(g.touch(0), INIT_VAL, "lazy reinit must fire on gen mismatch");

    let e = EagerSlot::new(4);
    assert_eq!(e.touch(0), INIT_VAL);
    e.reset();
    assert_eq!(e.touch(0), INIT_VAL);
}

fn main() {
    correctness_check();

    let ns: Vec<usize> = vec![64, 256, 1024, 4096, 16384];
    // Bound total touches to ~4M per cell so wall-clock stays reasonable
    // while N=16384 still gets >=200 frames.
    let frames_for = |n: usize| -> usize { (4_000_000 / n).clamp(200, 20_000) };
    let warmup_frames = 50;

    let json = env::args().any(|a| a == "--json");

    if !json {
        println!(
            "{:>7} {:>10} {:>18} {:>18} {:>18}",
            "N", "scheme", "reset_ns/call", "touch_ns/entry", "total_ns/frame"
        );
    } else {
        println!("n,scheme,reset_ns_per_call,touch_ns_per_entry,total_ns_per_frame");
    }

    for &n in &ns {
        let frames = frames_for(n);
        let g = bench_gen(n, frames, warmup_frames);
        let e = bench_eager(n, frames, warmup_frames);
        let e_sc = bench_eager_seqcst(n, frames, warmup_frames);
        for (tag, s) in [("gen", &g), ("eager", &e), ("eager-seqcst", &e_sc)] {
            if json {
                println!(
                    "{},{},{:.3},{:.3},{:.3}",
                    n, tag, s.reset_ns_per_call, s.touch_ns_per_entry, s.total_ns_per_frame
                );
            } else {
                println!(
                    "{:>7} {:>10} {:>18.3} {:>18.3} {:>18.3}",
                    s.n, tag, s.reset_ns_per_call, s.touch_ns_per_entry, s.total_ns_per_frame
                );
            }
        }
    }
}
