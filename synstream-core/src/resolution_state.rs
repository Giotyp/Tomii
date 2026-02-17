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

    // Atomically check-and-mark a slot as completed in a single critical section.
    // Returns true only for the one thread that wins the race — all others return false.
    // This eliminates the TOCTOU window between is_slot_completed() and mark_slot_completed().
    fn try_complete_slot(&self, slot: usize) -> bool;

    // Clear all sent flags for a slot
    fn clear_slot_sent_flags(&self, slot: usize);

    // Decrease dependency count and return new count (legacy per-instance method)
    fn decrease_dependency(&self, node_info: &NodeInfo) -> Option<usize>;

    // Increase dependency count and return new count
    fn increment_dependency(&self, node_info: &NodeInfo) -> Option<usize>;

    // Reinitialize dependency map for a slot
    fn reinit_dependencies(&self, nodes: &Vec<crate::graph_struct::Node>, slot: usize);

    // NEW: Optimized per-node decrements returning batch of ready instances
    // Decrements the dependency counter for a node in a slot by `count` and returns
    // all instance indices that are now ready to spawn. This replaces N per-instance
    // decrements with aggregated decrements, enabling threshold-based spawning.
    // group: None → global decrement (all groups), Some(g) → decrement group g only
    // count: number of decrements to apply (when multiple predecessors complete in same batch)
    fn decrease_and_get_ready(&self, _slot: usize, _node_id: usize, _group: Option<usize>, _count: usize) -> Vec<usize> {
        // Default implementation for backward compatibility: return empty
        // This allows implementations that don't override it to continue working
        Vec::new()
    }

    // Debug info for trait object printing
    fn debug_info(&self) -> String;
}

/// Per-node dependency entry for single-threaded threshold-based spawning.
/// Mirrors `NodeDependencyEntry` logic but without atomic overhead.
/// Supports per-group counters for fine-grained barrier dependencies.
struct StNodeDepEntry {
    /// Per-group counters (length = num_groups)
    remaining_deps: Vec<usize>,
    /// Per-instance sent flag (prevents double-spawn)
    instances_sent: Vec<bool>,
    /// Node factor (number of instances)
    factor: usize,
    /// Instances per group
    group_size: usize,
    /// Number of groups
    num_groups: usize,
    /// Dependencies per instance (within a group)
    deps_per_instance: usize,
    /// Whether this node has a barrier dependency
    has_barrier: bool,
    /// Initial total_deps value (for reinit)
    _init_total_deps: usize,
}

impl StNodeDepEntry {
    fn new(factor: usize, total_deps: usize, has_barrier: bool, group_size_opt: Option<usize>) -> Self {
        let (group_size, num_groups) = match group_size_opt {
            Some(gs) if gs > 0 && gs < factor => (gs, factor / gs),
            _ => (factor, 1),
        };
        let deps_per_group = if num_groups > 0 { total_deps / num_groups } else { 0 };
        let deps_per_instance = if group_size > 0 { deps_per_group / group_size } else { 0 };

        Self {
            remaining_deps: vec![deps_per_group; num_groups],
            instances_sent: vec![false; factor],
            factor,
            group_size,
            num_groups,
            deps_per_instance,
            has_barrier,
            _init_total_deps: total_deps,
        }
    }

    #[inline]
    fn threshold_for_instance_in_group(&self, idx_in_group: usize) -> usize {
        if idx_in_group >= self.group_size {
            return usize::MAX;
        }
        (self.group_size - idx_in_group - 1) * self.deps_per_instance
    }

    /// Decrement by count and return all newly-ready instance indices
    fn decrease_and_get_ready(&mut self, group: Option<usize>, count: usize) -> Vec<usize> {
        let groups_to_decrement: Vec<usize> = match group {
            Some(g) if g < self.num_groups => vec![g],
            None => (0..self.num_groups).collect(),
            _ => return Vec::new(),
        };

        let mut ready = Vec::new();

        for &g in &groups_to_decrement {
            if self.remaining_deps[g] > 0 {
                self.remaining_deps[g] = self.remaining_deps[g].saturating_sub(count);
            }
            let new_remaining = self.remaining_deps[g];

            let start = g * self.group_size;
            let end = std::cmp::min(start + self.group_size, self.factor);

            if self.has_barrier {
                if new_remaining == 0 {
                    for idx in start..end {
                        if !self.instances_sent[idx] {
                            self.instances_sent[idx] = true;
                            ready.push(idx);
                        }
                    }
                }
            } else {
                let max_threshold = self.group_size * self.deps_per_instance;
                if new_remaining <= max_threshold {
                    for idx in start..end {
                        let idx_in_group = idx - start;
                        if new_remaining <= self.threshold_for_instance_in_group(idx_in_group) {
                            if !self.instances_sent[idx] {
                                self.instances_sent[idx] = true;
                                ready.push(idx);
                            }
                        }
                    }
                }
            }
        }
        ready
    }

    fn increment(&mut self, instance_idx: Option<usize>) {
        let g = match instance_idx {
            Some(idx) => std::cmp::min(idx / self.group_size, self.num_groups - 1),
            None => 0,
        };
        self.remaining_deps[g] += 1;
    }

    fn reset_sent(&mut self, idx: usize) {
        if idx < self.instances_sent.len() {
            self.instances_sent[idx] = false;
        }
    }

    fn clear_sent(&mut self) {
        for flag in self.instances_sent.iter_mut() {
            *flag = false;
        }
    }

    fn reinit(&mut self, new_total_deps: usize) {
        let deps_per_group = if self.num_groups > 0 { new_total_deps / self.num_groups } else { 0 };
        for counter in self.remaining_deps.iter_mut() {
            *counter = deps_per_group;
        }
        for flag in self.instances_sent.iter_mut() {
            *flag = false;
        }
    }
}

// Single-threaded resolution state - UnsafeCell for zero-overhead interior mutability
pub struct SingleThreadedState {
    /// Per-node dependency tracking: node_deps[slot][node_id]
    node_deps: SingleThreadedCell<Vec<Vec<StNodeDepEntry>>>,
    completed_slots: SingleThreadedCell<HashSet<usize>>,
    dependency_count_vec: Arc<Vec<usize>>,
}

impl SingleThreadedState {
    pub fn new(
        _num_nodes: usize,
        slots: usize,
        _max_factor: usize,
        dependency_count_vec: Vec<usize>,
        nodes: &Vec<crate::graph_struct::Node>,
    ) -> Self {
        let num_nodes = nodes.len();

        // Build per-node dependency entries for each slot
        let mut all_slots = Vec::with_capacity(slots);
        for _ in 0..slots {
            let mut slot_entries = Vec::with_capacity(num_nodes);
            for node_id in 0..num_nodes {
                let node = &nodes[node_id];
                let total_deps = dependency_count_vec[node_id];
                let has_barrier = node.args.iter().any(|arg| arg.is_barrier());
                slot_entries.push(StNodeDepEntry::new(node.factor, total_deps, has_barrier, node.group_size));
            }
            all_slots.push(slot_entries);
        }

        Self {
            node_deps: SingleThreadedCell::new(all_slots),
            completed_slots: SingleThreadedCell::new(HashSet::new()),
            dependency_count_vec: Arc::new(dependency_count_vec),
        }
    }
}

impl ResolutionState for SingleThreadedState {
    #[inline]
    fn try_mark_sent(&self, slot: usize, node_id: usize, index: usize) -> bool {
        let deps = self.node_deps.get_mut();
        if slot < deps.len() && node_id < deps[slot].len() && index < deps[slot][node_id].factor {
            if !deps[slot][node_id].instances_sent[index] {
                deps[slot][node_id].instances_sent[index] = true;
                return true;
            }
        }
        false
    }

    #[inline]
    fn reset_sent(&self, slot: usize, node_id: usize, index: usize) {
        let deps = self.node_deps.get_mut();
        if slot < deps.len() && node_id < deps[slot].len() {
            deps[slot][node_id].reset_sent(index);
        }
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
    fn try_complete_slot(&self, slot: usize) -> bool {
        let completed = self.completed_slots.get_mut();
        if completed.contains(&slot) {
            false
        } else {
            completed.insert(slot);
            true
        }
    }

    #[inline]
    fn clear_slot_sent_flags(&self, slot: usize) {
        let deps = self.node_deps.get_mut();
        if slot < deps.len() {
            for entry in deps[slot].iter_mut() {
                entry.clear_sent();
            }
        }
    }

    #[inline]
    fn decrease_dependency(&self, node_info: &NodeInfo) -> Option<usize> {
        // Legacy per-instance method - no longer primary path but kept for compatibility
        let deps = self.node_deps.get_mut();
        let slot = node_info.slot;
        let node_id = node_info.id as usize;
        if slot < deps.len() && node_id < deps[slot].len() {
            let entry = &mut deps[slot][node_id];
            let g = std::cmp::min(node_info.index / entry.group_size, entry.num_groups - 1);
            if entry.remaining_deps[g] > 0 {
                entry.remaining_deps[g] -= 1;
            }
            return Some(entry.remaining_deps[g]);
        }
        None
    }

    #[inline]
    fn increment_dependency(&self, node_info: &NodeInfo) -> Option<usize> {
        let deps = self.node_deps.get_mut();
        let slot = node_info.slot;
        let node_id = node_info.id as usize;
        if slot < deps.len() && node_id < deps[slot].len() {
            deps[slot][node_id].increment(Some(node_info.index));
            return Some(deps[slot][node_id].remaining_deps[0]);
        }
        None
    }

    #[inline]
    fn reinit_dependencies(&self, nodes: &Vec<crate::graph_struct::Node>, slot: usize) {
        let deps = self.node_deps.get_mut();
        if slot < deps.len() {
            for node_id in 0..nodes.len() {
                if node_id < deps[slot].len() {
                    let total_deps = self.dependency_count_vec[node_id];
                    deps[slot][node_id].reinit(total_deps);
                }
            }
        }
    }

    fn decrease_and_get_ready(&self, slot: usize, node_id: usize, group: Option<usize>, count: usize) -> Vec<usize> {
        let deps = self.node_deps.get_mut();
        if slot < deps.len() && node_id < deps[slot].len() {
            deps[slot][node_id].decrease_and_get_ready(group, count)
        } else {
            Vec::new()
        }
    }

    fn debug_info(&self) -> String {
        format!("{:?}", self)
    }
}

impl fmt::Debug for SingleThreadedState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SingleThreadedState")
            .field("completed_slots", self.completed_slots.get_mut())
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
    fn decrease_dependency(&self, _node_info: &NodeInfo) -> Option<usize> {
        // LEGACY METHOD: No longer used - decrease_and_get_ready() is the new API
        // Kept for trait compatibility with SingleThreadedState
        None
    }

    #[inline]
    fn increment_dependency(&self, node_info: &NodeInfo) -> Option<usize> {
        // Delegate to NodeDepMap
        self.node_dep_map
            .increment_dependency(node_info.slot, node_info.id as usize, Some(node_info.index))
    }

    #[inline]
    fn reinit_dependencies(&self, nodes: &Vec<crate::graph_struct::Node>, slot: usize) {
        // Delegate to NodeDepMap
        self.node_dep_map
            .reinit_slot(nodes, slot, &self.dependency_count_vec);
    }

    #[inline]
    fn decrease_and_get_ready(&self, slot: usize, node_id: usize, group: Option<usize>, count: usize) -> Vec<usize> {
        // Use the optimized per-node dependency tracking from NodeDepMap
        self.node_dep_map.decrease_and_get_ready(slot, node_id, group, count)
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
