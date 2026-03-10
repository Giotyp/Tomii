use deepsize::DeepSizeOf;

use crate::debug::print_debug;
use crate::graph_struct::Node;
use crate::IdType;
use std::cmp::PartialEq;
use std::fmt::Debug;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};

// ---------------------------------------------------------------------------
// Generational pack/unpack helpers
//
// We pack a 32-bit generation counter and a 32-bit value into a single u64.
// Upper 32 bits = generation, lower 32 bits = value (remaining count or sent flag).
// This lets a single SeqCst CAS atomically reset a counter when a new generation
// starts (lazy reinit), eliminating the O(nodes × factor) slot reset loops.
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

#[derive(Clone)]
pub struct NodeInfo {
    pub id: IdType,
    /// Slot generation at scheduling time. Fits in the 6-byte padding between
    /// id (u16) and slot (usize) with no struct-size increase.
    /// Excluded from PartialEq / Hash / Debug so that result-map lookups and
    /// deduplication logic remain unaffected by generation stamps.
    pub gen: u32,
    pub slot: usize,
    pub index: usize,
    /// Number of consecutive instances this task handles starting at `index`.
    /// 1 = single instance (default). >1 = bulk task covering `index..index+bulk_count`.
    /// Excluded from PartialEq/Hash like `gen` (scheduling metadata, not identity).
    pub bulk_count: usize,
    pub pred_index: usize,
    pub post_node: bool,
}

impl NodeInfo {
    pub fn new(id: IdType, slot: usize, index: usize, pred_index: usize) -> NodeInfo {
        NodeInfo {
            id,
            gen: 0,
            slot,
            index,
            bulk_count: 1,
            pred_index,
            post_node: false,
        }
    }

    pub fn set_post_node(&mut self, post_node: bool) {
        self.post_node = post_node;
    }
}

// Intentionally exclude `gen` from equality and hashing: generation is scheduling
// metadata and must not affect result-map lookups or node deduplication.
impl PartialEq for NodeInfo {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.slot == other.slot
            && self.index == other.index
            && self.pred_index == other.pred_index
            && self.post_node == other.post_node
    }
}
impl Eq for NodeInfo {}

impl std::hash::Hash for NodeInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.slot.hash(state);
        self.index.hash(state);
        self.pred_index.hash(state);
        self.post_node.hash(state);
    }
}

impl std::fmt::Debug for NodeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "NodeID {{ id: {}, index: {}, bulk_count: {}, slot: {}, post_node: {} }}",
            self.id, self.index, self.bulk_count, self.slot, self.post_node
        )
    }
}

#[derive(DeepSizeOf)]
pub struct VecMap<T> {
    // flat buffer: slots * per_slot_size elements
    buffer: Vec<T>,
    init_val: T,
    // metadata for indexing
    slots: usize,
    per_slot_size: usize,
    node_offsets: Vec<usize>,
    node_factors: Vec<usize>,
    nodes_len: usize,
}

impl<T: Clone + PartialEq + Debug> VecMap<T> {
    pub fn new(init_val: T) -> VecMap<T> {
        VecMap {
            buffer: Vec::new(),
            init_val,
            slots: 0,
            per_slot_size: 0,
            node_offsets: Vec::new(),
            node_factors: Vec::new(),
            nodes_len: 0,
        }
    }

    pub fn init_map(&mut self, nodes: &Vec<Node>, slots: usize, init_values: Option<&Vec<T>>) {
        // Only initialize once (preserve previous behaviour)
        if !self.buffer.is_empty() {
            return;
        }

        // Prepare node factor and offsets
        self.nodes_len = nodes.len();
        self.node_factors = nodes.iter().map(|n| n.factor).collect();
        self.node_offsets = Vec::with_capacity(self.nodes_len);
        let mut offset = 0usize;
        for &f in &self.node_factors {
            self.node_offsets.push(offset);
            offset += f;
        }
        self.per_slot_size = offset; // sum of factors

        // Reserve flat buffer and fill with init values
        self.slots = slots;
        self.buffer = Vec::with_capacity(self.slots * self.per_slot_size);
        for _slot in 0..self.slots {
            for node in nodes.iter() {
                let val = if let Some(init_vals) = &init_values {
                    init_vals[node.id as usize].clone()
                } else {
                    self.init_val.clone()
                };
                for _ in 0..node.factor {
                    self.buffer.push(val.clone());
                }
            }
        }
    }

    pub fn extend_map(&mut self, nodes: &Vec<Node>) {
        // Append a new slot initialized with `init_val` for each node's factor.
        // Prefer stored node_factors if already initialized; otherwise derive from `nodes`.
        if self.per_slot_size == 0 {
            // Not initialized previously; derive factors and offsets now
            self.nodes_len = nodes.len();
            self.node_factors = nodes.iter().map(|n| n.factor).collect();
            self.node_offsets = Vec::with_capacity(self.nodes_len);
            let mut off = 0usize;
            for &f in &self.node_factors {
                self.node_offsets.push(off);
                off += f;
            }
            self.per_slot_size = off;
        }

        // fill new slot
        let mut new_slot: Vec<T> = Vec::with_capacity(self.per_slot_size);
        for node_id in 0..self.nodes_len {
            let factor = self.node_factors[node_id];
            for _ in 0..factor {
                new_slot.push(self.init_val.clone());
            }
        }
        self.buffer.extend(new_slot.iter().cloned());
        self.slots += 1;
    }

    pub fn get(&self, node_info: &NodeInfo) -> Option<T> {
        if node_info.slot < self.slots && (node_info.id as usize) < self.nodes_len {
            let node_id = node_info.id as usize;
            let factor = self.node_factors[node_id];
            if node_info.index < factor {
                let idx = node_info.slot * self.per_slot_size
                    + self.node_offsets[node_id]
                    + node_info.index;
                return Some(self.buffer[idx].clone());
            }
        }
        None
    }

    pub fn result_exists(&self, node_info: &NodeInfo) -> bool {
        if node_info.slot < self.slots && (node_info.id as usize) < self.nodes_len {
            let node_id = node_info.id as usize;
            let factor = self.node_factors[node_id];
            if node_info.index < factor {
                let idx = node_info.slot * self.per_slot_size
                    + self.node_offsets[node_id]
                    + node_info.index;
                if self.buffer[idx] != self.init_val {
                    return true;
                }
            }
        }
        false
    }

    pub fn decrease(&mut self, node_info: &NodeInfo) -> Option<usize>
    where
        T: std::ops::Sub<usize, Output = T>,
        T: From<usize>,
        T: PartialOrd,
        usize: From<T>,
    {
        if node_info.slot < self.slots && (node_info.id as usize) < self.nodes_len {
            let node_id = node_info.id as usize;
            let factor = self.node_factors[node_id];
            if node_info.index < factor {
                let idx = node_info.slot * self.per_slot_size
                    + self.node_offsets[node_id]
                    + node_info.index;
                let cur_val = &mut self.buffer[idx];
                let current: usize = (*cur_val).clone().into();
                if current > 0 {
                    *cur_val = T::from(current - 1);
                    return Some(current - 1);
                }
                return Some(current);
            }
        }
        None
    }

    pub fn increment(&mut self, node_info: &NodeInfo) -> Option<usize>
    where
        T: std::ops::Add<usize, Output = T>,
        T: From<usize>,
        usize: From<T>,
    {
        if node_info.slot < self.slots && (node_info.id as usize) < self.nodes_len {
            let node_id = node_info.id as usize;
            let factor = self.node_factors[node_id];
            if node_info.index < factor {
                let idx = node_info.slot * self.per_slot_size
                    + self.node_offsets[node_id]
                    + node_info.index;
                let cur_val = &mut self.buffer[idx];
                let current: usize = (*cur_val).clone().into();
                *cur_val = T::from(current + 1);
                return Some(current + 1);
            }
        }
        None
    }

    pub fn set(&mut self, node_info: &NodeInfo, element: T) {
        if node_info.slot < self.slots && (node_info.id as usize) < self.nodes_len {
            let node_id = node_info.id as usize;
            let factor = self.node_factors[node_id];
            if node_info.index < factor {
                let idx = node_info.slot * self.per_slot_size
                    + self.node_offsets[node_id]
                    + node_info.index;
                self.buffer[idx] = element;
                return;
            } else {
                panic!(
                    "Index {} out of bounds for node {}",
                    node_info.index, node_info.id
                );
            }
        }
        panic!("Slot {} out of bounds", node_info.slot);
    }

    pub fn reinit_slot(&mut self, nodes: &Vec<Node>, slot: usize, init_values: Option<&Vec<T>>) {
        if slot < self.slots {
            let start = slot * self.per_slot_size;

            for node in nodes.iter() {
                let node_id = node.id as usize;
                let val = if let Some(init_vals) = &init_values {
                    init_vals[node_id].clone()
                } else {
                    self.init_val.clone()
                };
                let factor = self.node_factors[node_id];
                let offset = self.node_offsets[node_id];
                for i in 0..factor {
                    self.buffer[start + offset + i] = val.clone();
                }
            }
        } else {
            panic!("Slot {} out of bounds", slot);
        }
    }

    pub fn reinit_elem(&mut self, node_info: &NodeInfo) {
        if node_info.slot < self.slots && (node_info.id as usize) < self.nodes_len {
            let node_id = node_info.id as usize;
            let factor = self.node_factors[node_id];
            if node_info.index < factor {
                let idx = node_info.slot * self.per_slot_size
                    + self.node_offsets[node_id]
                    + node_info.index;
                self.buffer[idx] = self.init_val.clone();
                return;
            } else {
                panic!(
                    "Index {} out of bounds for node {}",
                    node_info.index, node_info.id
                );
            }
        }
        panic!("Slot {} out of bounds", node_info.slot);
    }
}

impl<T: Clone + PartialEq + Debug> Debug for VecMap<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "VecMap {{")?;
        for slot_id in 0..self.slots {
            writeln!(f, "  Slot {}:", slot_id)?;
            let start = slot_id * self.per_slot_size;
            for node_id in 0..self.nodes_len {
                let off = self.node_offsets[node_id];
                let factor = self.node_factors[node_id];
                let mut vec_vals: Vec<&T> = Vec::with_capacity(factor);
                for idx in 0..factor {
                    vec_vals.push(&self.buffer[start + off + idx]);
                }
                writeln!(f, "    Node {}: {:?}", node_id, vec_vals)?;
            }
        }
        write!(f, "}}")
    }
}

// Lock-free atomic version of VecMap for multi-threaded dependency tracking
#[derive(DeepSizeOf)]
pub struct AtomicVecMap {
    // flat buffer: slots * per_slot_size atomic elements
    buffer: Vec<AtomicUsize>,
    // metadata for indexing
    slots: usize,
    per_slot_size: usize,
    node_offsets: Vec<usize>,
    node_factors: Vec<usize>,
    nodes_len: usize,
}

impl AtomicVecMap {
    pub fn new() -> AtomicVecMap {
        AtomicVecMap {
            buffer: Vec::new(),
            slots: 0,
            per_slot_size: 0,
            node_offsets: Vec::new(),
            node_factors: Vec::new(),
            nodes_len: 0,
        }
    }

    pub fn init_map(&mut self, nodes: &Vec<Node>, slots: usize, init_values: Option<&Vec<usize>>) {
        // Only initialize once
        if !self.buffer.is_empty() {
            return;
        }

        // Prepare node factor and offsets
        self.nodes_len = nodes.len();
        self.node_factors = nodes.iter().map(|n| n.factor).collect();
        self.node_offsets = Vec::with_capacity(self.nodes_len);
        let mut offset = 0usize;
        for &f in &self.node_factors {
            self.node_offsets.push(offset);
            offset += f;
        }
        self.per_slot_size = offset; // sum of factors

        // Reserve flat buffer and fill with init values
        self.slots = slots;
        self.buffer.reserve(self.slots * self.per_slot_size);
        for _slot in 0..self.slots {
            for node in nodes.iter() {
                let val = if let Some(init_vals) = &init_values {
                    init_vals[node.id as usize]
                } else {
                    0
                };
                for _ in 0..node.factor {
                    self.buffer.push(AtomicUsize::new(val));
                }
            }
        }
    }

    #[inline]
    fn compute_flat_index(&self, node_info: &NodeInfo) -> usize {
        node_info.slot * self.per_slot_size
            + self.node_offsets[node_info.id as usize]
            + node_info.index
    }

    // Lock-free atomic decrease operation
    // Uses saturating decrement to prevent underflow - if already 0, stays at 0
    #[inline]
    pub fn decrease(&self, node_info: &NodeInfo) -> Option<usize> {
        if node_info.slot < self.slots && (node_info.id as usize) < self.nodes_len {
            let node_id = node_info.id as usize;
            let factor = self.node_factors[node_id];
            if node_info.index < factor {
                let idx = self.compute_flat_index(node_info);
                // Use fetch_update for saturating decrement - prevents underflow
                // SeqCst ordering ensures visibility across all threads (prevents stale reads after slot reset)
                let result =
                    self.buffer[idx].fetch_update(Ordering::SeqCst, Ordering::SeqCst, |val| {
                        if val > 0 {
                            Some(val - 1)
                        } else {
                            None // Don't update if already 0
                        }
                    });

                match result {
                    Ok(prev) => Some(prev - 1),    // Return new value (prev - 1)
                    Err(current) => Some(current), // Already 0, return 0
                }
            } else {
                None
            }
        } else {
            None
        }
    }

    // Lock-free atomic increment operation
    #[inline]
    pub fn increment(&self, node_info: &NodeInfo) -> Option<usize> {
        if node_info.slot < self.slots && (node_info.id as usize) < self.nodes_len {
            let node_id = node_info.id as usize;
            let factor = self.node_factors[node_id];
            if node_info.index < factor {
                let idx = self.compute_flat_index(node_info);
                // SeqCst ordering ensures visibility across all threads
                let prev = self.buffer[idx].fetch_add(1, Ordering::SeqCst);
                return Some(prev + 1);
            }
        }
        None
    }

    // Lock-free atomic get operation
    #[inline]
    pub fn get(&self, node_info: &NodeInfo) -> Option<usize> {
        if node_info.slot < self.slots && (node_info.id as usize) < self.nodes_len {
            let node_id = node_info.id as usize;
            let factor = self.node_factors[node_id];
            if node_info.index < factor {
                let idx = self.compute_flat_index(node_info);
                // SeqCst ordering ensures visibility across all threads
                return Some(self.buffer[idx].load(Ordering::SeqCst));
            }
        }
        None
    }

    // Lock-free atomic set operation
    #[inline]
    pub fn set(&self, node_info: &NodeInfo, value: usize) {
        if node_info.slot < self.slots && (node_info.id as usize) < self.nodes_len {
            let node_id = node_info.id as usize;
            let factor = self.node_factors[node_id];
            if node_info.index < factor {
                let idx = self.compute_flat_index(node_info);
                // SeqCst ordering ensures visibility across all threads
                self.buffer[idx].store(value, Ordering::SeqCst);
                return;
            } else {
                panic!(
                    "Index {} out of bounds for node {} (factor: {})",
                    node_info.index, node_info.id, factor
                );
            }
        }
        panic!(
            "Slot {} or node {} out of bounds",
            node_info.slot, node_info.id
        );
    }

    // Only used during reinitialization (infrequent)
    pub fn reinit_slot(&self, nodes: &Vec<Node>, slot: usize, init_values: Option<&Vec<usize>>) {
        if slot < self.slots {
            let start = slot * self.per_slot_size;

            for node in nodes.iter() {
                let node_id = node.id as usize;
                let val = if let Some(init_vals) = &init_values {
                    init_vals[node_id]
                } else {
                    0
                };
                let factor = self.node_factors[node_id];
                let offset = self.node_offsets[node_id];
                for i in 0..factor {
                    self.buffer[start + offset + i].store(val, Ordering::SeqCst);
                }
            }
        } else {
            panic!("Slot {} out of bounds", slot);
        }
    }
}

impl Debug for AtomicVecMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "AtomicVecMap {{")?;
        for slot_id in 0..self.slots {
            writeln!(f, "  Slot {}:", slot_id)?;
            let start = slot_id * self.per_slot_size;
            for node_id in 0..self.nodes_len {
                let off = self.node_offsets[node_id];
                let factor = self.node_factors[node_id];
                let mut vec_vals: Vec<usize> = Vec::with_capacity(factor);
                for idx in 0..factor {
                    vec_vals.push(self.buffer[start + off + idx].load(Ordering::Relaxed));
                }
                writeln!(f, "    Node {}: {:?}", node_id, vec_vals)?;
            }
        }
        write!(f, "}}")
    }
}

/// Per-node dependency entry for threshold-based spawning
/// Supports per-group counters for fine-grained barrier dependencies.
/// When num_groups == 1, behavior is identical to the original single-counter design.
///
/// # Generational design
///
/// Each `AtomicU64` packs a 32-bit generation counter (upper) and a 32-bit value
/// (lower). When a slot starts a new stream, the runtime increments the
/// `slot_generation` counter. On the next access to an entry, if the stored
/// generation differs from the current one, the entry lazily reinitialises itself
/// to its initial value — no O(N) sweep required at slot reset time.
#[derive(Debug)]
pub struct NodeDependencyEntry {
    /// Per-group packed (gen: u32, remaining: u32) — length = num_groups.
    /// Upper 32 bits: generation. Lower 32 bits: remaining dependency count.
    remaining_deps: Vec<AtomicU64>,

    /// Per-instance packed (gen: u32, sent: u32) — length = factor.
    /// Upper 32 bits: generation. Lower 32 bits: 0 = not sent, 1 = sent.
    instances_sent: Vec<AtomicU64>,

    /// Cached metadata (avoid lookups)
    factor: usize,

    /// Instances per group
    group_size: usize,

    /// Number of groups (= factor / group_size, 1 if no groups)
    num_groups: usize,

    /// Initial dependencies per group (used for lazy reinit on generation mismatch)
    deps_per_group: u32,

    /// Dependencies per instance (within a group for grouped nodes)
    deps_per_instance: usize,

    /// Whether this node has a barrier dependency
    has_barrier: bool,
}

impl NodeDependencyEntry {
    /// Create a new dependency entry for a node in a slot
    /// group_size_opt: None or Some(factor) → single group (backward compatible)
    ///                 Some(gs) where gs < factor → multiple groups
    pub fn new(
        factor: usize,
        total_deps: usize,
        has_barrier: bool,
        group_size_opt: Option<usize>,
    ) -> Self {
        let (group_size, num_groups) = match group_size_opt {
            Some(gs) if gs > 0 && gs < factor => (gs, factor / gs),
            _ => (factor, 1), // No grouping or full-factor group
        };

        let dpg = if num_groups > 0 {
            total_deps / num_groups
        } else {
            0
        };
        let deps_per_instance = if group_size > 0 {
            dpg / group_size
        } else {
            0
        };
        let deps_per_group = dpg as u32;

        // Initialise with generation=0, value=initial_deps_per_group
        let remaining_deps: Vec<AtomicU64> = (0..num_groups)
            .map(|_| AtomicU64::new(gen_pack(0, deps_per_group)))
            .collect();

        // Initialise with generation=0, sent=0 (not sent)
        let instances_sent: Vec<AtomicU64> = (0..factor)
            .map(|_| AtomicU64::new(gen_pack(0, 0)))
            .collect();

        Self {
            remaining_deps,
            instances_sent,
            factor,
            group_size,
            num_groups,
            deps_per_group,
            deps_per_instance,
            has_barrier,
        }
    }

    /// Get the threshold for a specific instance within its group to become ready
    /// Instance at position idx_in_group is ready when:
    ///   remaining_deps[group] <= (group_size - idx_in_group - 1) × deps_per_instance
    #[inline]
    fn threshold_for_instance_in_group(&self, idx_in_group: usize) -> usize {
        if idx_in_group >= self.group_size {
            return usize::MAX;
        }
        (self.group_size - idx_in_group - 1) * self.deps_per_instance
    }

    /// Atomically decrease dependency by count and return indices of newly ready instances.
    /// `slot_gen`: current slot generation (u32); stale entries are lazily reinitialised.
    /// `group`: None → decrement ALL group counters, Some(g) → decrement group g only.
    /// `count`: number of decrements to apply (aggregated from same-batch predecessors).
    /// `specific_succ_idx`: when Some(i), fire exactly successor instance i (1:1 dispatch)
    ///   instead of doing the ordinal threshold scan. Used for non-barrier, equal-factor,
    ///   single-index $res deps where succ[i] always reads pred[i], ensuring the result
    ///   is available before the successor is dispatched (prevents spin_wait deadlock).
    /// Writes newly-ready instance indices into `ready` (cleared before use).
    pub fn decrease_and_get_ready_into(
        &self,
        slot_gen: u32,
        group: Option<usize>,
        count: usize,
        specific_succ_idx: Option<usize>,
        ready: &mut Vec<usize>,
    ) {
        ready.clear();

        let (g_start, g_end) = match group {
            Some(g) if g < self.num_groups => (g, g + 1),
            None => (0, self.num_groups),
            _ => return, // Invalid group
        };

        for g in g_start..g_end {
            // Atomically decrement this group's counter with lazy generational reinit.
            // If the stored generation differs from slot_gen, treat the value as the
            // initial deps_per_group (stale entry from a previous stream).
            let init_val = self.deps_per_group;
            let prev_packed = self.remaining_deps[g]
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |packed| {
                    let stored_gen = gen_unpack_gen(packed);
                    let current = if stored_gen == slot_gen {
                        gen_unpack_val(packed)
                    } else {
                        init_val // Lazy reinit: stale gen → full initial value
                    };
                    let new_val = current.saturating_sub(count as u32);
                    Some(gen_pack(slot_gen, new_val))
                })
                .unwrap(); // fetch_update always succeeds (closure returns Some)

            let new_remaining = {
                let stored_gen = gen_unpack_gen(prev_packed);
                let old_val = if stored_gen == slot_gen {
                    gen_unpack_val(prev_packed)
                } else {
                    init_val
                };
                old_val.saturating_sub(count as u32) as usize
            };

            // Determine instance range for this group
            let start = g * self.group_size;
            let end = std::cmp::min(start + self.group_size, self.factor);

            if self.has_barrier {
                // Barrier: spawn all instances in group when counter reaches 0
                if new_remaining == 0 {
                    for idx in start..end {
                        // CAS from (any_gen, 0) to (slot_gen, 1) — marks as sent this generation
                        loop {
                            let cur = self.instances_sent[idx].load(Ordering::SeqCst);
                            let cur_gen = gen_unpack_gen(cur);
                            let cur_sent = gen_unpack_val(cur) != 0;
                            // If same gen and already sent, skip
                            if cur_gen == slot_gen && cur_sent {
                                break;
                            }
                            let new_packed = gen_pack(slot_gen, 1);
                            if self.instances_sent[idx]
                                .compare_exchange(cur, new_packed, Ordering::SeqCst, Ordering::SeqCst)
                                .is_ok()
                            {
                                // Won the CAS: instance is newly ready for this generation
                                ready.push(idx);
                                break;
                            }
                            // CAS failed (another thread raced), retry
                        }
                    }
                }
            } else if let Some(specific_idx) = specific_succ_idx {
                // 1:1 dispatch: fire the exact successor instance that reads this predecessor.
                // The caller computed specific_idx = (pred_idx - k + factor) % factor so that
                // succ[specific_idx] reads pred[pred_idx] — result is guaranteed available.
                // This prevents spin_wait_for_result deadlock when all Rayon workers block.
                if specific_idx < self.factor {
                    loop {
                        let cur = self.instances_sent[specific_idx].load(Ordering::SeqCst);
                        let cur_gen = gen_unpack_gen(cur);
                        let cur_sent = gen_unpack_val(cur) != 0;
                        if cur_gen == slot_gen && cur_sent {
                            break; // Already sent this generation
                        }
                        let new_packed = gen_pack(slot_gen, 1);
                        if self.instances_sent[specific_idx]
                            .compare_exchange(cur, new_packed, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                        {
                            ready.push(specific_idx);
                            break;
                        }
                    }
                }
            } else {
                // Ordinal threshold-based: check instances in this group
                let max_threshold = self.group_size * self.deps_per_instance;
                if new_remaining <= max_threshold {
                    for idx in start..end {
                        let idx_in_group = idx - start;
                        if new_remaining <= self.threshold_for_instance_in_group(idx_in_group) {
                            loop {
                                let cur = self.instances_sent[idx].load(Ordering::SeqCst);
                                let cur_gen = gen_unpack_gen(cur);
                                let cur_sent = gen_unpack_val(cur) != 0;
                                if cur_gen == slot_gen && cur_sent {
                                    break; // Already sent this generation
                                }
                                let new_packed = gen_pack(slot_gen, 1);
                                if self.instances_sent[idx]
                                    .compare_exchange(
                                        cur,
                                        new_packed,
                                        Ordering::SeqCst,
                                        Ordering::SeqCst,
                                    )
                                    .is_ok()
                                {
                                    ready.push(idx);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn decrease_and_get_ready(
        &self,
        slot_gen: u32,
        group: Option<usize>,
        count: usize,
    ) -> Vec<usize> {
        let mut ready = Vec::new();
        self.decrease_and_get_ready_into(slot_gen, group, count, None, &mut ready);
        ready
    }

    /// Increment dependency counter (used when condition fails and dependency needs to be restored).
    /// Increments the counter for the group that contains the given instance.
    /// `slot_gen`: current slot generation, used for lazy reinit on generation mismatch.
    pub fn increment_dependency(&self, slot_gen: u32, instance_idx: Option<usize>) -> usize {
        let g = match instance_idx {
            Some(idx) => std::cmp::min(idx / self.group_size, self.num_groups - 1),
            None => 0,
        };
        let init_val = self.deps_per_group;
        let prev = self.remaining_deps[g]
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |packed| {
                let stored_gen = gen_unpack_gen(packed);
                let current = if stored_gen == slot_gen {
                    gen_unpack_val(packed)
                } else {
                    init_val
                };
                Some(gen_pack(slot_gen, current.saturating_add(1)))
            })
            .unwrap();
        let stored_gen = gen_unpack_gen(prev);
        let old_val = if stored_gen == slot_gen {
            gen_unpack_val(prev)
        } else {
            init_val
        };
        (old_val + 1) as usize
    }

    /// Reset the sent flag for a specific instance (used when conditions not met).
    /// `slot_gen`: current slot generation — writes generation so future CAS sees correct state.
    pub fn reset_sent_flag(&self, slot_gen: u32, instance_idx: usize) {
        if instance_idx < self.instances_sent.len() {
            self.instances_sent[instance_idx].store(gen_pack(slot_gen, 0), Ordering::SeqCst);
        }
    }

    /// Clear all sent flags for this entry.
    /// No-op under the generational design: the generation bump in slot_generation
    /// makes all existing `instances_sent` entries stale. Kept for trait compat.
    pub fn clear_sent_flags(&self) {
        // No-op: generation bump handles lazy clearing.
    }

    /// Reset this entry for a new slot iteration.
    /// No-op under the generational design: the generation bump in slot_generation
    /// triggers lazy reinit on the next access. Kept for trait compat.
    pub fn reset(&self, _new_total_deps: usize) {
        // No-op: generation bump handles lazy reinit.
    }
}

/// Optimized per-node dependency map using 2D slot×node indexing
/// Replaces per-instance VecMap/AtomicVecMap for significant memory savings
#[derive(Debug)]
pub struct NodeDepMap {
    /// 2D layout: slots[slot][node_id] -> NodeDependencyEntry
    slots: Vec<Vec<NodeDependencyEntry>>,
}

impl NodeDepMap {
    /// Create a new NodeDepMap initialized for all slots and nodes
    pub fn new(nodes: &Vec<Node>, slots: usize, dep_counts: &Vec<usize>) -> Self {
        let num_nodes = nodes.len();
        let mut map_slots = Vec::with_capacity(slots);

        for _ in 0..slots {
            let mut slot_entries = Vec::with_capacity(num_nodes);

            for node_id in 0..num_nodes {
                let node = &nodes[node_id];
                let total_deps = dep_counts[node_id];
                let has_barrier = node.args.iter().any(|arg| arg.is_barrier());

                // Calculate barrier-based grouping for per-group barriers
                let effective_group_size = if has_barrier {
                    // For barrier nodes, compute instances per barrier group
                    // by finding the group_by value from barrier args
                    let mut max_group_by = None;
                    for arg in &node.args {
                        if arg.is_barrier() {
                            if let Some(pred) = &arg.predecessor {
                                if let Some(gb) = pred.group_by {
                                    max_group_by = Some(max_group_by.unwrap_or(0).max(gb));
                                }
                            }
                        }
                    }

                    if let Some(gb) = max_group_by {
                        // Calculate instances per barrier group based on packet grouping
                        // For FFT: 832 packets / 64 group_by = 13 barrier groups
                        // instances_per_group = 832 instances / 13 groups = 64

                        // Find the number of predecessor packets from barrier args
                        let mut num_pred_packets = 0;
                        for arg in &node.args {
                            if arg.is_barrier() {
                                if let Some(pred) = &arg.predecessor {
                                    if pred.group_by.is_some() {
                                        num_pred_packets = num_pred_packets.max(pred.indexes.len());
                                    }
                                }
                            }
                        }

                        let num_barrier_groups = if num_pred_packets > 0 && gb > 0 {
                            num_pred_packets / gb
                        } else {
                            1
                        };

                        let instances_per_barrier_group = if num_barrier_groups > 0 {
                            node.factor / num_barrier_groups
                        } else {
                            node.factor
                        };

                        print_debug(|| {
                            format!("DB: BARRIER GROUPING: node={}, factor={}, total_deps={}, group_by={}, num_pred_packets={}, num_barrier_groups={}, instances_per_group={}",
                                  node.name, node.factor, total_deps, gb, num_pred_packets, num_barrier_groups, instances_per_barrier_group)
                        });

                        Some(instances_per_barrier_group)
                    } else {
                        node.group_size
                    }
                } else {
                    node.group_size
                };

                let entry = NodeDependencyEntry::new(
                    node.factor,
                    total_deps,
                    has_barrier,
                    effective_group_size,
                );
                slot_entries.push(entry);
            }

            map_slots.push(slot_entries);
        }

        Self { slots: map_slots }
    }

    /// Get ready instances for a node in a slot by decrementing dependencies by count.
    /// `slot_gen`: current slot generation for lazy generational reinit.
    /// `specific_succ_idx`: when Some(i), fire exactly instance i (1:1 dispatch).
    /// Writes results into `ready` (cleared before use). No allocation on the hot path.
    #[inline]
    pub fn decrease_and_get_ready_into(
        &self,
        slot: usize,
        node_id: usize,
        slot_gen: u32,
        group: Option<usize>,
        count: usize,
        specific_succ_idx: Option<usize>,
        ready: &mut Vec<usize>,
    ) {
        if slot < self.slots.len() && node_id < self.slots[slot].len() {
            self.slots[slot][node_id].decrease_and_get_ready_into(
                slot_gen,
                group,
                count,
                specific_succ_idx,
                ready,
            );
        } else {
            ready.clear();
        }
    }

    /// Get ready instances for a node in a slot by decrementing dependencies by count.
    /// `slot_gen`: current slot generation for lazy generational reinit.
    /// `group`: None → global decrement, Some(g) → decrement group g only.
    /// `count`: number of decrements to apply.
    #[inline]
    pub fn decrease_and_get_ready(
        &self,
        slot: usize,
        node_id: usize,
        slot_gen: u32,
        group: Option<usize>,
        count: usize,
    ) -> Vec<usize> {
        let mut ready = Vec::new();
        self.decrease_and_get_ready_into(slot, node_id, slot_gen, group, count, None, &mut ready);
        ready
    }

    /// Increment dependency for a specific node (used when condition fails).
    /// `slot_gen`: current slot generation for lazy generational reinit.
    /// Returns the new dependency count.
    #[inline]
    pub fn increment_dependency(
        &self,
        slot: usize,
        node_id: usize,
        slot_gen: u32,
        instance_idx: Option<usize>,
    ) -> Option<usize> {
        if slot < self.slots.len() && node_id < self.slots[slot].len() {
            Some(self.slots[slot][node_id].increment_dependency(slot_gen, instance_idx))
        } else {
            None
        }
    }

    /// Reset the sent flag for an instance (used when conditions not met).
    /// `slot_gen`: current slot generation — writes generation so future CAS sees correct state.
    #[inline]
    pub fn reset_sent_flag(&self, slot: usize, node_id: usize, slot_gen: u32, instance_idx: usize) {
        if slot < self.slots.len() && node_id < self.slots[slot].len() {
            self.slots[slot][node_id].reset_sent_flag(slot_gen, instance_idx);
        }
    }

    /// Clear all sent flags for a slot.
    /// No-op under the generational design: slot_generation bump handles lazy clearing.
    pub fn clear_slot_sent_flags(&self, _slot: usize) {
        // No-op: generation bump handles lazy clearing.
    }

    /// Reset dependencies for a slot.
    /// No-op under the generational design: slot_generation bump triggers lazy reinit.
    pub fn reinit_slot(&self, _nodes: &Vec<Node>, _slot: usize, _dep_counts: &Vec<usize>) {
        // No-op: generation bump handles lazy reinit.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_dependency_entry_creation() {
        // factor=4, total_deps=8 (2 per instance), no groups
        let entry = NodeDependencyEntry::new(4, 8, false, None);
        assert_eq!(entry.factor, 4);
        assert_eq!(entry.deps_per_instance, 2);
        assert_eq!(entry.num_groups, 1);
        assert!(!entry.has_barrier);
    }

    #[test]
    fn test_threshold_calculation() {
        // factor=4, deps_per_inst=2, no groups
        let entry = NodeDependencyEntry::new(4, 8, false, None);
        // Instance 0: (4-0-1)*2 = 6
        // Instance 1: (4-1-1)*2 = 4
        // Instance 2: (4-2-1)*2 = 2
        // Instance 3: (4-3-1)*2 = 0
        assert_eq!(entry.threshold_for_instance_in_group(0), 6);
        assert_eq!(entry.threshold_for_instance_in_group(1), 4);
        assert_eq!(entry.threshold_for_instance_in_group(2), 2);
        assert_eq!(entry.threshold_for_instance_in_group(3), 0);
    }

    #[test]
    fn test_threshold_spawning_factor_4() {
        // factor=4, deps_per_inst=2, total_deps=8, no groups; use gen=0
        let entry = NodeDependencyEntry::new(4, 8, false, None);
        let gen: u32 = 0;

        // Call 1: 8->7, instance 0 threshold=6, not ready (7 > 6)
        let ready = entry.decrease_and_get_ready(gen, None, 1);
        assert!(ready.is_empty());

        // Call 2: 7->6, instance 0 threshold=6, ready! (6 <= 6)
        let ready = entry.decrease_and_get_ready(gen, None, 1);
        assert_eq!(ready, vec![0]);

        // Call 3: 6->5, instance 1 threshold=4, not ready (5 > 4)
        let ready = entry.decrease_and_get_ready(gen, None, 1);
        assert!(ready.is_empty());

        // Call 4: 5->4, instance 1 threshold=4, ready! (4 <= 4)
        let ready = entry.decrease_and_get_ready(gen, None, 1);
        assert_eq!(ready, vec![1]);

        // Call 5: 4->3, instance 2 threshold=2, not ready (3 > 2)
        let ready = entry.decrease_and_get_ready(gen, None, 1);
        assert!(ready.is_empty());

        // Call 6: 3->2, instance 2 threshold=2, ready! (2 <= 2)
        let ready = entry.decrease_and_get_ready(gen, None, 1);
        assert_eq!(ready, vec![2]);

        // Call 7: 2->1, instance 3 threshold=0, not ready (1 > 0)
        let ready = entry.decrease_and_get_ready(gen, None, 1);
        assert!(ready.is_empty());

        // Call 8: 1->0, instance 3 threshold=0, ready! (0 <= 0)
        let ready = entry.decrease_and_get_ready(gen, None, 1);
        assert_eq!(ready, vec![3]);
    }

    #[test]
    fn test_barrier_spawns_all_at_once() {
        // Barrier node with factor=3, total_deps=3, no groups; use gen=0
        let entry = NodeDependencyEntry::new(3, 3, true, None);
        let gen: u32 = 0;

        // Decrease until deps reach 0
        for _ in 0..2 {
            let ready = entry.decrease_and_get_ready(gen, None, 1);
            assert!(ready.is_empty()); // Barrier not ready yet
        }

        // Final decrease brings deps to 0, barrier spawns all
        let ready = entry.decrease_and_get_ready(gen, None, 1);
        assert_eq!(ready.len(), 3);
        assert!(ready.contains(&0));
        assert!(ready.contains(&1));
        assert!(ready.contains(&2));
    }

    #[test]
    fn test_no_double_spawn() {
        let entry = NodeDependencyEntry::new(2, 4, false, None);
        let gen: u32 = 0;

        // factor=2, total_deps=4, deps_per_instance=2
        // Instance 0 threshold = (2-0-1)*2 = 2
        // Instance 1 threshold = (2-1-1)*2 = 0

        // Call 1: 4->3, instance 0 threshold=2, not ready (3 > 2)
        let ready = entry.decrease_and_get_ready(gen, None, 1);
        assert!(ready.is_empty());

        // Call 2: 3->2, instance 0 threshold=2, ready! (2 <= 2)
        let ready = entry.decrease_and_get_ready(gen, None, 1);
        assert_eq!(ready, vec![0]);

        // Call 3: 2->1, instance 1 threshold=0, not ready (1 > 0)
        let ready = entry.decrease_and_get_ready(gen, None, 1);
        assert!(ready.is_empty());

        // Call 4: 1->0, instance 1 threshold=0, ready! (0 <= 0)
        let ready = entry.decrease_and_get_ready(gen, None, 1);
        assert_eq!(ready, vec![1]);

        // Call 5: try another decrement (would underflow)
        // No more deps to satisfy, nothing ready
        let ready = entry.decrease_and_get_ready(gen, None, 1);
        assert!(ready.is_empty());
    }

    #[test]
    fn test_entry_reset() {
        // The generational design lazy-resets on generation change.
        // Simulate "reset" by using gen=1 after initial decrements with gen=0.
        let entry = NodeDependencyEntry::new(2, 4, false, None);

        // Decrease twice with gen=0
        let _ = entry.decrease_and_get_ready(0, None, 1);
        let _ = entry.decrease_and_get_ready(0, None, 1);

        // "Reset" via generation bump: use gen=1 — should behave like new
        let ready = entry.decrease_and_get_ready(1, None, 1);
        assert!(ready.is_empty());
    }

    #[test]
    fn test_per_group_barrier() {
        // factor=6, group_size=3, 2 groups. total_deps=6 (3 per group)
        // Each group has 3 deps. Barrier fires per-group when group counter reaches 0.
        let entry = NodeDependencyEntry::new(6, 6, true, Some(3));
        assert_eq!(entry.num_groups, 2);
        assert_eq!(entry.deps_per_group, 3);
        let gen: u32 = 0;

        // Decrement group 0 twice → not ready yet
        let ready = entry.decrease_and_get_ready(gen, Some(0), 1);
        assert!(ready.is_empty());
        let ready = entry.decrease_and_get_ready(gen, Some(0), 1);
        assert!(ready.is_empty());

        // Decrement group 0 third time → group 0 instances (0,1,2) spawn
        let ready = entry.decrease_and_get_ready(gen, Some(0), 1);
        assert_eq!(ready.len(), 3);
        assert!(ready.contains(&0));
        assert!(ready.contains(&1));
        assert!(ready.contains(&2));

        // Group 1 still blocked
        let ready = entry.decrease_and_get_ready(gen, Some(1), 1);
        assert!(ready.is_empty());
        let ready = entry.decrease_and_get_ready(gen, Some(1), 1);
        assert!(ready.is_empty());

        // Decrement group 1 third time → group 1 instances (3,4,5) spawn
        let ready = entry.decrease_and_get_ready(gen, Some(1), 1);
        assert_eq!(ready.len(), 3);
        assert!(ready.contains(&3));
        assert!(ready.contains(&4));
        assert!(ready.contains(&5));
    }

    #[test]
    fn test_node_dep_map_creation() {
        let nodes = vec![
            Node {
                name: "node0".to_string(),
                args: vec![],
                id: 0,
                loop_args: None,
                factor: 2,
                group_size: None,
                func_ptr: None,
                loop_: None,
                condition: None,
                use_workers: None,
                priority: crate::graph_struct::NodePriority::Normal,
            },
            Node {
                name: "node1".to_string(),
                args: vec![],
                id: 1,
                loop_args: None,
                factor: 3,
                group_size: None,
                func_ptr: None,
                loop_: None,
                condition: None,
                use_workers: None,
                priority: crate::graph_struct::NodePriority::Normal,
            },
        ];
        let dep_counts = vec![4, 6];

        let map = NodeDepMap::new(&nodes, 2, &dep_counts);
        assert_eq!(map.slots.len(), 2); // 2 slots
        assert_eq!(map.slots[0].len(), 2); // 2 nodes
    }
}

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
    pub fn new(nodes: &Vec<Node>, slots: usize) -> Self {
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
            // Safe: pointer is valid and we're only cloning (not mutating)
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
                unsafe {
                    drop(Box::from_raw(old_ptr));
                }
            }
        }
    }

    /// Extend with a new slot (for dynamic slot addition)
    pub fn extend_slot(&mut self, _nodes: &Vec<Node>) {
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

impl Debug for LockFreeResultMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "LockFreeResultMap {{")?;
        writeln!(f, "  slots: {}", self.slots)?;
        writeln!(f, "  per_slot_size: {}", self.per_slot_size)?;
        writeln!(f, "  nodes: {}", self.nodes_len)?;
        write!(f, "}}")
    }
}
