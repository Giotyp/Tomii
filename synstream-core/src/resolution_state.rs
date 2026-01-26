use crate::buffers::*;
use parking_lot::Mutex;
use std::cell::UnsafeCell;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

// Single-threaded wrapper that provides interior mutability without locks
struct SingleThreadedCell<T> {
    value: UnsafeCell<T>,
}

impl<T> SingleThreadedCell<T> {
    fn new(value: T) -> Self {
        Self {
            value: UnsafeCell::new(value),
        }
    }

    #[inline]
    fn get_mut(&self) -> &mut T {
        // SAFETY: We guarantee single-threaded access through runtime construction
        unsafe { &mut *self.value.get() }
    }
}

// SAFETY: Only constructed when system_threads == 1, guaranteeing single-threaded access
unsafe impl<T> Send for SingleThreadedCell<T> where T: Send {}
unsafe impl<T> Sync for SingleThreadedCell<T> where T: Send {}

// Trait for resolution state operations - allows single-threaded and multi-threaded implementations
pub trait ResolutionState: Send + Sync {
    // Check if a node has been sent to queue, and mark it as sent if not
    fn try_mark_sent(&self, slot: usize, node_id: usize, index: usize) -> bool;

    // Reset the sent flag for a node (used when conditions not met)
    fn reset_sent(&self, slot: usize, node_id: usize, index: usize);

    // Check if a slot has been marked as completed
    fn is_slot_completed(&self, slot: usize) -> bool;

    // Mark a slot as completed
    fn mark_slot_completed(&self, slot: usize);

    // Remove slot from completed set (for new iteration)
    fn unmark_slot_completed(&self, slot: usize);

    // Clear all sent flags for a slot
    fn clear_slot_sent_flags(&self, slot: usize);

    // Decrease dependency count and return new count (legacy per-instance method)
    fn decrease_dependency(&self, node_info: &NodeInfo) -> Option<usize>;

    // Increase dependency count and return new count
    fn increment_dependency(&self, node_info: &NodeInfo) -> Option<usize>;

    // Reinitialize dependency map for a slot
    fn reinit_dependencies(&self, nodes: &Vec<crate::graph_struct::Node>, slot: usize);

    // NEW: Optimized per-node decrements returning batch of ready instances
    // Decrements the dependency counter for a node in a slot once and returns
    // all instance indices that are now ready to spawn. This replaces N per-instance
    // decrements with a single per-node decrement, enabling threshold-based spawning.
    fn decrease_and_get_ready(&self, _slot: usize, _node_id: usize) -> Vec<usize> {
        // Default implementation for backward compatibility: return empty
        // This allows implementations that don't override it to continue working
        Vec::new()
    }

    // Debug info for trait object printing
    fn debug_info(&self) -> String;
}

// Single-threaded resolution state - UnsafeCell for zero-overhead interior mutability
pub struct SingleThreadedState {
    dependency_map: SingleThreadedCell<VecMap<usize>>,
    nodes_sent_to_queue: SingleThreadedCell<Vec<Vec<bool>>>,
    completed_slots: SingleThreadedCell<HashSet<usize>>,
    max_factor: usize,
    node_offsets: Vec<usize>,
    dependency_count_vec: Arc<Vec<usize>>,
}

impl SingleThreadedState {
    pub fn new(
        num_nodes: usize,
        slots: usize,
        max_factor: usize,
        dependency_count_vec: Vec<usize>,
        nodes: &Vec<crate::graph_struct::Node>,
    ) -> Self {
        let mut dependency_map = VecMap::new(0);
        dependency_map.init_map(nodes, slots, Some(&dependency_count_vec));

        let mut nodes_sent = Vec::new();
        for _ in 0..slots {
            nodes_sent.push(vec![false; num_nodes * max_factor]);
        }

        // Compute node_offsets for correct flat index calculation
        let mut node_offsets = Vec::with_capacity(num_nodes);
        let mut offset = 0;
        for node in nodes.iter() {
            node_offsets.push(offset);
            offset += node.factor;
        }

        Self {
            dependency_map: SingleThreadedCell::new(dependency_map),
            nodes_sent_to_queue: SingleThreadedCell::new(nodes_sent),
            completed_slots: SingleThreadedCell::new(HashSet::new()),
            max_factor,
            node_offsets,
            dependency_count_vec: Arc::new(dependency_count_vec),
        }
    }
}

impl ResolutionState for SingleThreadedState {
    #[inline]
    fn try_mark_sent(&self, slot: usize, node_id: usize, index: usize) -> bool {
        // Direct mutable access - no synchronization overhead at all
        let sent = self.nodes_sent_to_queue.get_mut();
        let flat_idx = self.node_offsets[node_id] + index;
        if !sent[slot][flat_idx] {
            sent[slot][flat_idx] = true;
            true
        } else {
            false
        }
    }

    #[inline]
    fn reset_sent(&self, slot: usize, node_id: usize, index: usize) {
        let sent = self.nodes_sent_to_queue.get_mut();
        let flat_idx = self.node_offsets[node_id] + index;
        sent[slot][flat_idx] = false;
    }

    #[inline]
    fn is_slot_completed(&self, slot: usize) -> bool {
        self.completed_slots.get_mut().contains(&slot)
    }

    #[inline]
    fn mark_slot_completed(&self, slot: usize) {
        self.completed_slots.get_mut().insert(slot);
    }

    #[inline]
    fn unmark_slot_completed(&self, slot: usize) {
        self.completed_slots.get_mut().remove(&slot);
    }

    #[inline]
    fn clear_slot_sent_flags(&self, slot: usize) {
        let sent = self.nodes_sent_to_queue.get_mut();
        for flag in sent[slot].iter_mut() {
            *flag = false;
        }
    }

    #[inline]
    fn decrease_dependency(&self, node_info: &NodeInfo) -> Option<usize> {
        self.dependency_map.get_mut().decrease(node_info)
    }

    #[inline]
    fn increment_dependency(&self, node_info: &NodeInfo) -> Option<usize> {
        self.dependency_map.get_mut().increment(node_info)
    }

    #[inline]
    fn reinit_dependencies(&self, nodes: &Vec<crate::graph_struct::Node>, slot: usize) {
        self.dependency_map
            .get_mut()
            .reinit_slot(nodes, slot, Some(&self.dependency_count_vec));
    }

    fn debug_info(&self) -> String {
        format!("{:?}", self)
    }
}

impl fmt::Debug for SingleThreadedState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SingleThreadedState")
            .field("dependency_map", self.dependency_map.get_mut())
            .field(
                "nodes_sent_to_queue",
                &self.nodes_sent_to_queue.get_mut() as &Vec<Vec<bool>>,
            )
            .field("completed_slots", self.completed_slots.get_mut())
            .field("max_factor", &self.max_factor)
            .field("node_offsets", &self.node_offsets)
            .field("dependency_count_vec", &self.dependency_count_vec)
            .finish()
    }
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
    fn try_mark_sent(&self, _slot: usize, _node_id: usize, _index: usize) -> bool {
        // LEGACY METHOD: No longer used - decrease_and_get_ready() handles sent marking internally
        // Kept for trait compatibility with SingleThreadedState
        false
    }

    #[inline]
    fn reset_sent(&self, slot: usize, node_id: usize, index: usize) {
        // Delegate to NodeDepMap
        self.node_dep_map.reset_sent_flag(slot, node_id, index);
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
    fn clear_slot_sent_flags(&self, slot: usize) {
        // Delegate to NodeDepMap
        self.node_dep_map.clear_slot_sent_flags(slot);
    }

    #[inline]
    fn decrease_dependency(&self, _node_info: &NodeInfo) -> Option<usize> {
        // LEGACY METHOD: No longer used - decrease_and_get_ready() is the new API
        // Kept for trait compatibility with SingleThreadedState
        None
    }

    #[inline]
    fn increment_dependency(&self, node_info: &NodeInfo) -> Option<usize> {
        // Delegate to NodeDepMap
        self.node_dep_map
            .increment_dependency(node_info.slot, node_info.id as usize)
    }

    #[inline]
    fn reinit_dependencies(&self, nodes: &Vec<crate::graph_struct::Node>, slot: usize) {
        // Delegate to NodeDepMap
        self.node_dep_map
            .reinit_slot(nodes, slot, &self.dependency_count_vec);
    }

    #[inline]
    fn decrease_and_get_ready(&self, slot: usize, node_id: usize) -> Vec<usize> {
        // Use the optimized per-node dependency tracking from NodeDepMap
        self.node_dep_map.decrease_and_get_ready(slot, node_id)
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
