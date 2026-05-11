//! Dependency-counter abstraction for per-node, per-slot arrival tracking.
//!
//! [`DependencyCounter`] is a lock-free trait that tracks how many predecessor
//! completions each node still needs before it becomes ready.  It is NOT a
//! resolution strategy — it does not drive batches or schedule tasks.  The
//! resolution strategy ([`crate::runtime::resolution_strategy::ResolutionStrategy`])
//! calls into this counter to decrement arrivals and collect ready successor
//! indices.
//!
//! # v1 implementation
//! [`MultiThreadedCounter`] covers all production use-cases via atomics and a
//! per-node generational bitset.  It supports up to 64 concurrent slots
//! (enforced at build time by [`crate::runtime::TomiiRtBuilder::build`]).

use crate::buffers::*;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Lock-free per-node dependency-counter abstraction.
///
/// Each method maps directly to one phase of the four-phase completion protocol
/// (see `runtime/ARCHITECTURE.md`).  Implementations must be `Send + Sync`
/// because the same instance is shared across all resolution threads and worker
/// threads simultaneously.
pub trait DependencyCounter: Send + Sync {
    /// Reset the sent flag for a node (used when a condition check fails and the
    /// instance must be re-armed for a future trigger).
    ///
    /// `slot_gen`: current slot generation for generational sent-flag reset.
    fn reset_sent(&self, slot: usize, node_id: usize, index: usize, slot_gen: u32);

    /// Remove `slot` from the completed-slots bitmask (called at the start of a
    /// new stream iteration on the same slot).
    fn unmark_slot_completed(&self, slot: usize);

    /// Atomically claim slot completion: returns `true` only for the one thread
    /// that transitions the completion bit 0→1.  All concurrent callers that
    /// observe the bit already set return `false`.
    ///
    /// This eliminates the TOCTOU window between a read-then-set sequence.
    fn try_complete_slot(&self, slot: usize) -> bool;

    /// Increment the arrival counter for `node_info` and return the new count,
    /// or `None` if the node has already reached its threshold this generation.
    ///
    /// `slot_gen`: current slot generation for lazy generational reinit.
    fn increment_dependency(&self, node_info: &NodeInfo, slot_gen: u32) -> Option<usize>;

    /// Hot-path decrement: subtract `count` arrivals from the counter for
    /// `(slot, node_id)` and write ready instance indices into `ready`.
    ///
    /// `specific_succ_idx`: when `Some(i)`, fire exactly instance `i`
    /// (1:1 non-barrier dispatch); `None` triggers normal threshold logic.
    #[allow(clippy::too_many_arguments)]
    fn decrease_and_get_ready_into(
        &self,
        slot: usize,
        node_id: usize,
        slot_gen: u32,
        group: Option<usize>,
        count: usize,
        specific_succ_idx: Option<usize>,
        ready: &mut Vec<usize>,
    );

    /// Human-readable diagnostic string (used in `tracing::debug!` at startup).
    fn debug_info(&self) -> String;
}

/// Multi-threaded dependency counter — uses atomics for lock-free operation.
///
/// Supports up to 64 concurrent slots (enforced by `TomiiRtBuilder::build`).
/// All slot-level operations on `completed_slots` use `SeqCst` to establish
/// total ordering across all threads; this is required in multi-slot mode
/// (see `runtime/ordering.rs` for the full rationale).
pub struct MultiThreadedCounter {
    /// Per-node dependency tracking with threshold-based spawning.
    node_dep_map: Arc<crate::buffers::NodeDepMap>,

    /// Bitset where bit `i` = 1 means slot `i` has been claimed as completed.
    /// `AtomicU64` supports up to 64 slots (enforced by `TomiiRtBuilder::build`).
    completed_slots: AtomicU64,
}

impl MultiThreadedCounter {
    /// Construct a new counter from the compiled graph metadata.
    pub fn new(
        _num_nodes: usize,
        slots: usize,
        _max_factor: usize,
        dependency_count_vec: Vec<usize>,
        nodes: &[crate::graph_struct::Node],
    ) -> Self {
        let node_dep_map = crate::buffers::NodeDepMap::new(nodes, slots, &dependency_count_vec);
        Self {
            node_dep_map: Arc::new(node_dep_map),
            completed_slots: AtomicU64::new(0),
        }
    }
}

impl DependencyCounter for MultiThreadedCounter {
    #[inline]
    fn reset_sent(&self, slot: usize, node_id: usize, index: usize, slot_gen: u32) {
        self.node_dep_map
            .reset_sent_flag(slot, node_id, slot_gen, index);
    }

    #[inline]
    fn unmark_slot_completed(&self, slot: usize) {
        self.completed_slots
            .fetch_and(!(1u64 << slot), Ordering::SeqCst);
    }

    #[inline]
    fn try_complete_slot(&self, slot: usize) -> bool {
        // Atomically set the bit; returns true only for the thread that transitions 0→1.
        let prev = self
            .completed_slots
            .fetch_or(1u64 << slot, Ordering::SeqCst);
        prev & (1u64 << slot) == 0
    }

    #[inline]
    fn increment_dependency(&self, node_info: &NodeInfo, slot_gen: u32) -> Option<usize> {
        self.node_dep_map.increment_dependency(
            node_info.slot,
            node_info.id as usize,
            slot_gen,
            Some(node_info.index),
        )
    }

    #[inline]
    fn decrease_and_get_ready_into(
        &self,
        slot: usize,
        node_id: usize,
        slot_gen: u32,
        group: Option<usize>,
        count: usize,
        specific_succ_idx: Option<usize>,
        ready: &mut Vec<usize>,
    ) {
        self.node_dep_map.decrease_and_get_ready_into(
            slot,
            node_id,
            slot_gen,
            group,
            count,
            specific_succ_idx,
            ready,
        );
    }

    fn debug_info(&self) -> String {
        format!("{:?}", self)
    }
}

impl fmt::Debug for MultiThreadedCounter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bitmap = self.completed_slots.load(Ordering::Relaxed);
        let completed: Vec<usize> = (0..64).filter(|i| bitmap & (1u64 << i) != 0).collect();
        f.debug_struct("MultiThreadedCounter")
            .field("\nnode_dep_map", &self.node_dep_map)
            .field("\ncompleted_slots", &completed)
            .finish()
    }
}
