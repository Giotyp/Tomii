use crate::buffers::*;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// Trait for resolution state operations
pub trait ResolutionState: Send + Sync {
    // Reset the sent flag for a node (used when conditions not met)
    // slot_gen: current slot generation for generational sent-flag reset
    fn reset_sent(&self, slot: usize, node_id: usize, index: usize, slot_gen: u32);

    // Remove slot from completed set (for new iteration)
    fn unmark_slot_completed(&self, slot: usize);

    // Atomically check-and-mark a slot as completed in a single critical section.
    // Returns true only for the one thread that wins the race — all others return false.
    // This eliminates the TOCTOU window between a read-then-set sequence.
    fn try_complete_slot(&self, slot: usize) -> bool;

    // Increase dependency count and return new count
    // slot_gen: current slot generation for lazy generational reinit
    fn increment_dependency(&self, node_info: &NodeInfo, slot_gen: u32) -> Option<usize>;

    // Hot-path variant: writes ready indices into caller-supplied buffer (no allocation).
    // `specific_succ_idx`: when Some(i), fire exactly instance i (1:1 non-barrier dispatch).
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

    // Debug info for trait object printing
    fn debug_info(&self) -> String;
}

// Multi-threaded resolution state - uses atomics for lock-free operations
pub struct MultiThreadedState {
    // Per-node dependency tracking with threshold-based spawning
    node_dep_map: Arc<crate::buffers::NodeDepMap>,

    // Bitset where bit i = 1 means slot i has been claimed as completed.
    // AtomicU64 supports up to 64 slots (enforced by TomiiRtBuilder::build assert).
    completed_slots: AtomicU64,
}

impl MultiThreadedState {
    pub fn new(
        _num_nodes: usize,
        slots: usize,
        _max_factor: usize,
        dependency_count_vec: Vec<usize>,
        nodes: &[crate::graph_struct::Node],
    ) -> Self {
        // Initialize per-node dependency map with threshold-based spawning
        let node_dep_map = crate::buffers::NodeDepMap::new(nodes, slots, &dependency_count_vec);

        Self {
            node_dep_map: Arc::new(node_dep_map),
            completed_slots: AtomicU64::new(0),
        }
    }
}

impl ResolutionState for MultiThreadedState {
    #[inline]
    fn reset_sent(&self, slot: usize, node_id: usize, index: usize, slot_gen: u32) {
        // Delegate to NodeDepMap
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
        // Delegate to NodeDepMap
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

impl fmt::Debug for MultiThreadedState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bitmap = self.completed_slots.load(Ordering::Relaxed);
        let completed: Vec<usize> = (0..64).filter(|i| bitmap & (1u64 << i) != 0).collect();
        f.debug_struct("MultiThreadedState")
            .field("\nnode_dep_map", &self.node_dep_map)
            .field("\ncompleted_slots", &completed)
            .finish()
    }
}
