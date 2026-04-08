//! Per-slot, per-node runtime buffers for the SynStream task graph.
//!
//! # Generational lazy-reinit pattern
//!
//! ## The problem
//!
//! SynStream runs many streams through the same slot in sequence.  Each slot
//! owns a set of [`NodeDependencyEntry`] counters and sent-flags that must be
//! reset to their initial values at the start of every new stream.  A naive
//! O(nodes × factor) sweep before each stream is both slow and racy: worker
//! threads completing the tail of stream N can still be decrementing counters
//! while the reset for stream N+1 is in progress.
//!
//! ## The solution — pack generation + value into one `AtomicU64`
//!
//! Every counter is stored as a **packed `u64`**:
//!
//! ```text
//!  63            32 31             0
//! ┌───────────────┬───────────────┐
//! │  generation   │     value     │
//! └───────────────┴───────────────┘
//! ```
//!
//! - **Upper 32 bits**: a generation counter, incremented once per stream in
//!   `slot_data.generation[slot]` when a slot completes.
//! - **Lower 32 bits**: the actual value (remaining dependency count, or a
//!   0/1 sent-flag).
//!
//! A CAS operation compares-and-swaps the full 64 bits.  When a thread reads
//! a counter and finds that the stored generation is **older** than the current
//! slot generation, it knows the value is stale and treats it as if it holds
//! the initial value — **without any explicit reset store**.  The next writer
//! that successfully CASes in a new packed word with the current generation
//! effectively resets the counter for that stream.
//!
//! This makes slot reinitialisation O(1): bump one `AtomicU64` (the slot
//! generation) and all counters lazily reinitialise themselves on first access.
//!
//! ## Where the pattern is used
//!
//! | Field | Location | Packed value |
//! |-------|----------|-------------|
//! | `remaining_deps` | [`NodeDependencyEntry`] | remaining predecessor count per group |
//! | `instances_sent` | [`NodeDependencyEntry`] | per-instance completion flag (0 / 1) |
//! | `cond_instances_to_spawn` | `SlotData` | remaining condition-node spawns per node |
//!
//! The helpers [`gen_pack`], [`gen_unpack_gen`], and [`gen_unpack_val`] encode
//! and decode this layout throughout the hot path.
//!
//! ## Stale-task detection (related but distinct)
//!
//! Worker threads also carry a generation stamp at dispatch time in
//! `WORKER_STATE.executing_gen`.  If a slot completes and its generation is
//! bumped while a worker is still mid-execution, the worker's stamp will no
//! longer match `slot_data.generation[slot]`.  `populate_cached_args_into`
//! detects this mismatch and sets `stale_task_detected = true`; `execute_task`
//! then discards the task without decrementing the new stream's counters.
//! This is complementary to the lazy-reinit pattern: lazy reinit handles
//! counter reset correctness; stale detection handles task-level correctness.

mod node_info;
mod node_dep;
mod result_map;

pub use node_info::*;
pub use node_dep::*;
pub use result_map::*;

// ---------------------------------------------------------------------------
// Generational pack/unpack helpers
// ---------------------------------------------------------------------------

/// Pack generation `gen` and value `val` into a single u64.
#[inline(always)]
pub fn gen_pack(gen: u32, val: u32) -> u64 {
    ((gen as u64) << 32) | (val as u64)
}

/// Extract the generation from a packed u64.
#[inline(always)]
pub fn gen_unpack_gen(packed: u64) -> u32 {
    (packed >> 32) as u32
}

/// Extract the value from a packed u64.
#[inline(always)]
pub fn gen_unpack_val(packed: u64) -> u32 {
    packed as u32
}
