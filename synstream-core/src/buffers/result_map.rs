use std::sync::atomic::{AtomicPtr, Ordering};

use super::NodeInfo;
use crate::graph_struct::Node;

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
    pub fn reinit_slot(&self, slot: usize) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_struct::{Node, NodePriority};
    use synstream_types::CmTypes;

    fn make_nodes(factors: &[usize]) -> Vec<Node> {
        factors
            .iter()
            .enumerate()
            .map(|(i, &factor)| Node {
                name: format!("node_{}", i),
                args: Vec::new(),
                id: i as crate::IdType,
                loop_args: None,
                factor,
                group_size: None,
                func_name: String::new(),
                loop_: None,
                condition: None,
                priority: NodePriority::Normal,
                use_workers: None,
            })
            .collect()
    }

    fn make_node_info(slot: usize, id: crate::IdType, index: usize, gen: u32) -> NodeInfo {
        NodeInfo {
            id,
            slot,
            index,
            gen,
            bulk_count: 1,
            pred_index: 0,
            post_node: false,
        }
    }

    // -------------------------------------------------------------------------
    // Slot lifecycle: set → get → reinit → set again
    // -------------------------------------------------------------------------

    #[test]
    fn test_slot_lifecycle_set_get_reinit_set() {
        let nodes = make_nodes(&[2]); // one node, factor=2
        let map = LockFreeResultMap::new(&nodes, 1);

        let ni0 = make_node_info(0, 0, 0, 0);
        let ni1 = make_node_info(0, 0, 1, 0);

        // Initially no results
        assert!(map.get(&ni0).is_none());
        assert!(map.get(&ni1).is_none());

        // Set both instances
        map.set(&ni0, CmTypes::I32(10));
        map.set(&ni1, CmTypes::I32(20));
        assert_eq!(map.get(&ni0), Some(CmTypes::I32(10)));
        assert_eq!(map.get(&ni1), Some(CmTypes::I32(20)));

        // Reinit clears the slot
        map.reinit_slot(0);
        assert!(map.get(&ni0).is_none());
        assert!(map.get(&ni1).is_none());

        // Results can be set again after reinit (simulates next stream)
        map.set(&ni0, CmTypes::I32(30));
        assert_eq!(map.get(&ni0), Some(CmTypes::I32(30)));
        assert!(map.get(&ni1).is_none());
    }

    // -------------------------------------------------------------------------
    // Multi-slot isolation: results in slot 0 must not be visible in slot 1
    // -------------------------------------------------------------------------

    #[test]
    fn test_multi_slot_result_isolation() {
        let nodes = make_nodes(&[3]); // one node, factor=3
        let map = LockFreeResultMap::new(&nodes, 2); // 2 slots

        for idx in 0..3 {
            let ni = make_node_info(0, 0, idx, 0);
            map.set(&ni, CmTypes::I32(idx as i32 * 10));
        }

        // Slot 1 should still be empty
        for idx in 0..3 {
            let ni1 = make_node_info(1, 0, idx, 0);
            assert!(
                map.get(&ni1).is_none(),
                "slot 1 idx {} leaked from slot 0",
                idx
            );
        }

        // Setting slot 1 must not overwrite slot 0
        let ni1_0 = make_node_info(1, 0, 0, 0);
        map.set(&ni1_0, CmTypes::I32(99));
        let ni0_0 = make_node_info(0, 0, 0, 0);
        assert_eq!(map.get(&ni0_0), Some(CmTypes::I32(0)));
        assert_eq!(map.get(&ni1_0), Some(CmTypes::I32(99)));
    }

    // -------------------------------------------------------------------------
    // result_exists is consistent with get
    // -------------------------------------------------------------------------

    #[test]
    fn test_result_exists_consistent_with_get() {
        let nodes = make_nodes(&[1]);
        let map = LockFreeResultMap::new(&nodes, 1);
        let ni = make_node_info(0, 0, 0, 0);

        assert!(!map.result_exists(&ni));
        map.set(&ni, CmTypes::Bool(true));
        assert!(map.result_exists(&ni));
        map.reinit_slot(0);
        assert!(!map.result_exists(&ni));
    }

    // -------------------------------------------------------------------------
    // reinit only clears the target slot
    // -------------------------------------------------------------------------

    #[test]
    fn test_reinit_only_clears_target_slot() {
        let nodes = make_nodes(&[1]);
        let map = LockFreeResultMap::new(&nodes, 3);

        for slot in 0..3 {
            let ni = make_node_info(slot, 0, 0, 0);
            map.set(&ni, CmTypes::I32(slot as i32));
        }

        map.reinit_slot(1);

        assert_eq!(map.get(&make_node_info(0, 0, 0, 0)), Some(CmTypes::I32(0)));
        assert!(map.get(&make_node_info(1, 0, 0, 0)).is_none());
        assert_eq!(map.get(&make_node_info(2, 0, 0, 0)), Some(CmTypes::I32(2)));
    }

    // -------------------------------------------------------------------------
    // Concurrent set + get from multiple threads (smoke test)
    // -------------------------------------------------------------------------

    #[test]
    fn test_concurrent_set_get() {
        use std::sync::Arc;
        use std::thread;

        let nodes = make_nodes(&[64]);
        let map = Arc::new(LockFreeResultMap::new(&nodes, 1));

        let writers: Vec<_> = (0..64)
            .map(|idx| {
                let m = Arc::clone(&map);
                thread::spawn(move || {
                    let ni = make_node_info(0, 0, idx, 0);
                    m.set(&ni, CmTypes::I32(idx as i32));
                })
            })
            .collect();

        for w in writers {
            w.join().unwrap();
        }

        for idx in 0..64 {
            let ni = make_node_info(0, 0, idx, 0);
            assert_eq!(map.get(&ni), Some(CmTypes::I32(idx as i32)));
        }
    }
}
