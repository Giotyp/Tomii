use std::sync::atomic::{AtomicPtr, Ordering};

use crate::graph_struct::Node;

use super::NodeInfo;

/// Lock-free result storage using atomic pointer swaps
/// Eliminates RwLock contention between resolution threads and rayon workers
pub struct LockFreeResultMap {
    /// Flat array: slots * per_slot_size elements
    buffer: Vec<AtomicPtr<synstream_types::CmTypes>>,
    per_slot_size: usize,
    node_offsets: Vec<usize>,
    node_factors: Vec<usize>,
    nodes_len: usize,
    slots: usize,
}

impl LockFreeResultMap {
    pub fn new(nodes: &[Node], slots: usize) -> Self {
        let nodes_len = nodes.len();
        let mut node_factors = Vec::with_capacity(nodes_len);
        let mut node_offsets = Vec::with_capacity(nodes_len);

        let mut offset = 0;
        for node in nodes.iter() {
            node_offsets.push(offset);
            node_factors.push(node.factor);
            offset += node.factor;
        }
        let per_slot_size = offset;

        // Initialize with null pointers
        let total_size = slots * per_slot_size;
        let mut buffer = Vec::with_capacity(total_size);
        for _ in 0..total_size {
            buffer.push(AtomicPtr::new(std::ptr::null_mut()));
        }

        Self {
            buffer,
            per_slot_size,
            node_offsets,
            node_factors,
            nodes_len,
            slots,
        }
    }

    #[inline]
    fn flat_index(&self, node_info: &NodeInfo) -> usize {
        let node_id = node_info.id as usize;
        node_info.slot * self.per_slot_size + self.node_offsets[node_id] + node_info.index
    }

    /// Atomically store a result (called by resolution threads)
    #[inline]
    pub fn set(&self, node_info: &NodeInfo, result: synstream_types::CmTypes) {
        if node_info.slot >= self.slots || (node_info.id as usize) >= self.nodes_len {
            panic!(
                "Invalid node_info: slot={}, id={}",
                node_info.slot, node_info.id
            );
        }

        let node_id = node_info.id as usize;
        if node_info.index >= self.node_factors[node_id] {
            panic!(
                "Index {} out of bounds for node {}",
                node_info.index, node_info.id
            );
        }

        let idx = self.flat_index(node_info);
        let boxed = Box::new(result);
        let new_ptr = Box::into_raw(boxed);

        // Atomic swap with Release ordering (ensures writes before this are visible)
        let old_ptr = self.buffer[idx].swap(new_ptr, Ordering::Release);

        // Free old value if it existed
        if !old_ptr.is_null() {
            // SAFETY: `old_ptr` was produced by `Box::into_raw` in a prior call to `set()`;
            // the atomic swap gives us exclusive ownership, so reconstructing the Box is valid.
            unsafe {
                drop(Box::from_raw(old_ptr));
            }
        }
    }

    /// Atomically load a result (called by rayon workers)
    #[inline]
    pub fn get(&self, node_info: &NodeInfo) -> Option<synstream_types::CmTypes> {
        if node_info.slot >= self.slots || (node_info.id as usize) >= self.nodes_len {
            return None;
        }

        let node_id = node_info.id as usize;
        if node_info.index >= self.node_factors[node_id] {
            return None;
        }

        let idx = self.flat_index(node_info);

        // Atomic load with Acquire ordering (ensures we see writes before the Release store)
        let ptr = self.buffer[idx].load(Ordering::Acquire);

        if ptr.is_null() {
            None
        } else {
            // SAFETY: `ptr` is non-null and was stored by `set()` via `Box::into_raw`; the
            // Acquire load synchronizes with the Release store, so the pointee is fully initialized.
            // We only clone (no mutation), so no aliasing rules are violated.
            Some(unsafe { (*ptr).clone() })
        }
    }

    /// Check if result exists without cloning
    #[inline]
    pub fn result_exists(&self, node_info: &NodeInfo) -> bool {
        if node_info.slot >= self.slots || (node_info.id as usize) >= self.nodes_len {
            return false;
        }

        let node_id = node_info.id as usize;
        if node_info.index >= self.node_factors[node_id] {
            return false;
        }

        let idx = self.flat_index(node_info);
        !self.buffer[idx].load(Ordering::Acquire).is_null()
    }

    /// Clear a slot for reinitialization
    pub fn reinit_slot(
        &self,
        _nodes: &Vec<Node>,
        slot: usize,
        _init_values: Option<&Vec<synstream_types::CmTypes>>,
    ) {
        if slot >= self.slots {
            panic!("Slot {} out of bounds", slot);
        }

        let start = slot * self.per_slot_size;
        let end = start + self.per_slot_size;

        // Free all pointers in this slot
        for idx in start..end {
            let old_ptr = self.buffer[idx].swap(std::ptr::null_mut(), Ordering::SeqCst);
            if !old_ptr.is_null() {
                // SAFETY: `old_ptr` was produced by `Box::into_raw` in `set()`; the SeqCst swap
                // gives us exclusive ownership of this pointer before we drop it.
                unsafe {
                    drop(Box::from_raw(old_ptr));
                }
            }
        }
    }

    /// Extend with a new slot (for dynamic slot addition)
    pub fn extend_slot(&mut self, _nodes: &[Node]) {
        for _ in 0..self.per_slot_size {
            self.buffer.push(AtomicPtr::new(std::ptr::null_mut()));
        }
        self.slots += 1;
    }
}

impl Drop for LockFreeResultMap {
    fn drop(&mut self) {
        // Clean up all allocated results
        for atomic_ptr in &self.buffer {
            let ptr = atomic_ptr.load(Ordering::Acquire);
            if !ptr.is_null() {
                // SAFETY: `ptr` was produced by `Box::into_raw` in `set()`; `drop` has `&mut self`
                // so no other thread can access the map, giving us exclusive ownership.
                unsafe {
                    drop(Box::from_raw(ptr));
                }
            }
        }
    }
}

// Safety: AtomicPtr is Send+Sync, and we only clone (not mutate) through raw pointers
unsafe impl Send for LockFreeResultMap {}
unsafe impl Sync for LockFreeResultMap {}

impl std::fmt::Debug for LockFreeResultMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "LockFreeResultMap {{")?;
        writeln!(f, "  slots: {}", self.slots)?;
        writeln!(f, "  per_slot_size: {}", self.per_slot_size)?;
        writeln!(f, "  nodes: {}", self.nodes_len)?;
        write!(f, "}}")
    }
}
