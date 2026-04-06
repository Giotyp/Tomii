use crate::buffers::*;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

// Trait for resolution state operations
pub trait ResolutionState: Send + Sync {
    // Reset the sent flag for a node (used when conditions not met)
    // slot_gen: current slot generation for generational sent-flag reset
    fn reset_sent(&self, slot: usize, node_id: usize, index: usize, slot_gen: u32);

    // Check if a slot has been marked as completed
    fn is_slot_completed(&self, slot: usize) -> bool;

    // Mark a slot as completed
    fn mark_slot_completed(&self, slot: usize);

    // Remove slot from completed set (for new iteration)
    fn unmark_slot_completed(&self, slot: usize);

    // Atomically check-and-mark a slot as completed in a single critical section.
    // Returns true only for the one thread that wins the race — all others return false.
    // This eliminates the TOCTOU window between is_slot_completed() and mark_slot_completed().
    fn try_complete_slot(&self, slot: usize) -> bool;

    // Clear all sent flags for a slot
    fn clear_slot_sent_flags(&self, slot: usize);

    // Increase dependency count and return new count
    // slot_gen: current slot generation for lazy generational reinit
    fn increment_dependency(&self, node_info: &NodeInfo, slot_gen: u32) -> Option<usize>;

    // Reinitialize dependency map for a slot
    fn reinit_dependencies(&self, nodes: &Vec<crate::graph_struct::Node>, slot: usize);

    // NEW: Optimized per-node decrements returning batch of ready instances
    // Decrements the dependency counter for a node in a slot by `count` and returns
    // all instance indices that are now ready to spawn. This replaces N per-instance
    // decrements with aggregated decrements, enabling threshold-based spawning.
    // slot_gen: current slot generation (u32) for lazy generational reinit
    // group: None → global decrement (all groups), Some(g) → decrement group g only
    // count: number of decrements to apply (when multiple predecessors complete in same batch)
    fn decrease_and_get_ready(&self, _slot: usize, _node_id: usize, _slot_gen: u32, _group: Option<usize>, _count: usize) -> Vec<usize> {
        // Default implementation for backward compatibility: return empty
        // This allows implementations that don't override it to continue working
        Vec::new()
    }

    // Hot-path variant: writes ready indices into caller-supplied buffer (no allocation).
    // `specific_succ_idx`: when Some(i), fire exactly instance i (1:1 non-barrier dispatch).
    // Default delegates to the Vec-returning version for backward compatibility.
    fn decrease_and_get_ready_into(&self, slot: usize, node_id: usize, slot_gen: u32, group: Option<usize>, count: usize, specific_succ_idx: Option<usize>, ready: &mut Vec<usize>) {
        ready.clear();
        ready.extend(self.decrease_and_get_ready(slot, node_id, slot_gen, group, count));
        let _ = specific_succ_idx; // default impl ignores specific_succ_idx
    }

    // Debug info for trait object printing
    fn debug_info(&self) -> String;
}

// Multi-threaded resolution state - uses atomics for lock-free operations
pub struct MultiThreadedState {
    // Per-node dependency tracking with threshold-based spawning
    node_dep_map: Arc<crate::buffers::NodeDepMap>,

    completed_slots: Arc<Mutex<HashSet<usize>>>,
    dependency_count_vec: Arc<Vec<usize>>,
}

impl MultiThreadedState {
    pub fn new(
        _num_nodes: usize,
        slots: usize,
        _max_factor: usize,
        dependency_count_vec: Vec<usize>,
        nodes: &Vec<crate::graph_struct::Node>,
    ) -> Self {
        // Initialize per-node dependency map with threshold-based spawning
        let node_dep_map = crate::buffers::NodeDepMap::new(nodes, slots, &dependency_count_vec);

        Self {
            node_dep_map: Arc::new(node_dep_map),
            completed_slots: Arc::new(Mutex::new(HashSet::new())),
            dependency_count_vec: Arc::new(dependency_count_vec),
        }
    }
}

impl ResolutionState for MultiThreadedState {
    #[inline]
    fn reset_sent(&self, slot: usize, node_id: usize, index: usize, slot_gen: u32) {
        // Delegate to NodeDepMap
        self.node_dep_map.reset_sent_flag(slot, node_id, slot_gen, index);
    }

    #[inline]
    fn is_slot_completed(&self, slot: usize) -> bool {
        self.completed_slots.lock().contains(&slot)
    }

    #[inline]
    fn mark_slot_completed(&self, slot: usize) {
        self.completed_slots.lock().insert(slot);
    }

    #[inline]
    fn unmark_slot_completed(&self, slot: usize) {
        self.completed_slots.lock().remove(&slot);
    }

    #[inline]
    fn try_complete_slot(&self, slot: usize) -> bool {
        let mut guard = self.completed_slots.lock();
        if guard.contains(&slot) {
            false
        } else {
            guard.insert(slot);
            true
        }
    }

    #[inline]
    fn clear_slot_sent_flags(&self, slot: usize) {
        // Delegate to NodeDepMap
        self.node_dep_map.clear_slot_sent_flags(slot);
    }

    #[inline]
    fn increment_dependency(&self, node_info: &NodeInfo, slot_gen: u32) -> Option<usize> {
        // Delegate to NodeDepMap
        self.node_dep_map
            .increment_dependency(node_info.slot, node_info.id as usize, slot_gen, Some(node_info.index))
    }

    #[inline]
    fn reinit_dependencies(&self, nodes: &Vec<crate::graph_struct::Node>, slot: usize) {
        // Delegate to NodeDepMap
        self.node_dep_map
            .reinit_slot(nodes, slot, &self.dependency_count_vec);
    }

    #[inline]
    fn decrease_and_get_ready(&self, slot: usize, node_id: usize, slot_gen: u32, group: Option<usize>, count: usize) -> Vec<usize> {
        let mut ready = Vec::new();
        self.node_dep_map.decrease_and_get_ready_into(slot, node_id, slot_gen, group, count, None, &mut ready);
        ready
    }

    #[inline]
    fn decrease_and_get_ready_into(&self, slot: usize, node_id: usize, slot_gen: u32, group: Option<usize>, count: usize, specific_succ_idx: Option<usize>, ready: &mut Vec<usize>) {
        self.node_dep_map.decrease_and_get_ready_into(slot, node_id, slot_gen, group, count, specific_succ_idx, ready);
    }

    fn debug_info(&self) -> String {
        format!("{:?}", self)
    }
}

impl fmt::Debug for MultiThreadedState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Collect completed slots from mutex
        let completed_slots = self.completed_slots.lock().clone();

        f.debug_struct("MultiThreadedState")
            .field("\nnode_dep_map", &self.node_dep_map)
            .field("\ncompleted_slots", &completed_slots)
            .field("\ndependency_count_vec", &self.dependency_count_vec)
            .finish()
    }
}
