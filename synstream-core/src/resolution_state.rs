use crate::buffers::*;
use parking_lot::{Mutex, RwLock};
use std::cell::UnsafeCell;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
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

    // Decrease dependency count and return new count
    fn decrease_dependency(&self, node_info: &NodeInfo) -> Option<usize>;

    // Reinitialize dependency map for a slot
    fn reinit_dependencies(&self, nodes: &Vec<crate::graph_struct::Node>, slot: usize);
}

// Single-threaded resolution state - UnsafeCell for zero-overhead interior mutability
pub struct SingleThreadedState {
    dependency_map: SingleThreadedCell<VecMap<usize>>,
    nodes_sent_to_queue: SingleThreadedCell<Vec<Vec<bool>>>,
    completed_slots: SingleThreadedCell<HashSet<usize>>,
    max_factor: usize,
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

        Self {
            dependency_map: SingleThreadedCell::new(dependency_map),
            nodes_sent_to_queue: SingleThreadedCell::new(nodes_sent),
            completed_slots: SingleThreadedCell::new(HashSet::new()),
            max_factor,
            dependency_count_vec: Arc::new(dependency_count_vec),
        }
    }
}

impl ResolutionState for SingleThreadedState {
    #[inline]
    fn try_mark_sent(&self, slot: usize, node_id: usize, index: usize) -> bool {
        // Direct mutable access - no synchronization overhead at all
        let sent = self.nodes_sent_to_queue.get_mut();
        let flat_idx = node_id * self.max_factor + index;
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
        let flat_idx = node_id * self.max_factor + index;
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
    fn reinit_dependencies(&self, nodes: &Vec<crate::graph_struct::Node>, slot: usize) {
        self.dependency_map
            .get_mut()
            .reinit_slot(nodes, slot, Some(&self.dependency_count_vec));
    }
}

// Multi-threaded resolution state - uses atomics for lock-free operations
pub struct MultiThreadedState {
    dependency_map: Arc<RwLock<VecMap<usize>>>,
    nodes_sent_to_queue: Arc<Vec<Vec<AtomicBool>>>,
    completed_slots: Arc<Mutex<HashSet<usize>>>,
    max_factor: usize,
    dependency_count_vec: Arc<Vec<usize>>,
}

impl MultiThreadedState {
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
            let mut slot_sent = Vec::with_capacity(num_nodes * max_factor);
            for _ in 0..(num_nodes * max_factor) {
                slot_sent.push(AtomicBool::new(false));
            }
            nodes_sent.push(slot_sent);
        }

        Self {
            dependency_map: Arc::new(RwLock::new(dependency_map)),
            nodes_sent_to_queue: Arc::new(nodes_sent),
            completed_slots: Arc::new(Mutex::new(HashSet::new())),
            max_factor,
            dependency_count_vec: Arc::new(dependency_count_vec),
        }
    }
}

impl ResolutionState for MultiThreadedState {
    #[inline]
    fn try_mark_sent(&self, slot: usize, node_id: usize, index: usize) -> bool {
        let flat_idx = node_id * self.max_factor + index;
        self.nodes_sent_to_queue[slot][flat_idx]
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    #[inline]
    fn reset_sent(&self, slot: usize, node_id: usize, index: usize) {
        let flat_idx = node_id * self.max_factor + index;
        self.nodes_sent_to_queue[slot][flat_idx].store(false, Ordering::Release);
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
        for flag in self.nodes_sent_to_queue[slot].iter() {
            flag.store(false, Ordering::Release);
        }
    }

    #[inline]
    fn decrease_dependency(&self, node_info: &NodeInfo) -> Option<usize> {
        self.dependency_map.write().decrease(node_info)
    }

    #[inline]
    fn reinit_dependencies(&self, nodes: &Vec<crate::graph_struct::Node>, slot: usize) {
        self.dependency_map
            .write()
            .reinit_slot(nodes, slot, Some(&self.dependency_count_vec));
    }
}
