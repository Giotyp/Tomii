//! Atomic memory-ordering helpers for slot generation counters.
//!
//! # Why two orderings?
//!
//! In `single_slot_mode` (exactly one concurrent stream) only one stream runs at a time,
//! so pairwise `Acquire`/`AcqRel` synchronisation between the producing and consuming
//! sides is sufficient.
//!
//! In multi-slot mode concurrent reinitialisation creates a scenario where Thread A's
//! decrement on slot 0 must be globally visible to Thread C's completion check on the
//! same slot, even though threads A and C may have no direct synchronisation edge.
//! `SeqCst` establishes total ordering across *all* threads, not just pairwise, which
//! is the only ordering that guarantees this.
//!
//! # Scope
//!
//! These helpers apply **only** to reads and read-modify-write operations on the slot
//! generation counter (`slot_data.generation`) and the per-slot task counters
//! (`pending_tasks`, `pending_cond_tasks`, `processing_count`).  Other atomics may
//! use weaker orderings for reasons documented at their call sites.
//!
//! # Safety contract
//!
//! Do not weaken either helper below its current ordering without first verifying
//! the multi-slot completion path under `loom` or a formal memory model.  The
//! `SeqCst` requirement was confirmed after a series of correctness bugs (see
//! memory notes Bugs #14, #18, #19 in the project history).

use super::shared_data::SharedData;
use std::sync::atomic::Ordering;

/// Appropriate load ordering for slot generation counters.
#[inline(always)]
pub(super) fn slot_gen_load(shared: &SharedData) -> Ordering {
    if shared.config.single_slot_mode {
        Ordering::Acquire
    } else {
        Ordering::SeqCst
    }
}

/// Appropriate read-modify-write ordering for slot generation counters.
#[inline(always)]
pub(super) fn slot_gen_rmw(shared: &SharedData) -> Ordering {
    if shared.config.single_slot_mode {
        Ordering::AcqRel
    } else {
        Ordering::SeqCst
    }
}
