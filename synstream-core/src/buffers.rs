use deepsize::DeepSizeOf;

use crate::graph_struct::Node;
use crate::IdType;
use std::cmp::PartialEq;
use std::fmt::Debug;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct NodeInfo {
    pub id: IdType,
    pub slot: usize,
    pub index: usize,
    pub pred_index: usize,
    pub post_node: bool,
}

impl NodeInfo {
    pub fn new(id: IdType, slot: usize, index: usize, pred_index: usize) -> NodeInfo {
        NodeInfo {
            id,
            slot,
            index,
            pred_index,
            post_node: false,
        }
    }

    pub fn set_post_node(&mut self, post_node: bool) {
        self.post_node = post_node;
    }
}

impl std::fmt::Debug for NodeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "NodeID {{ id: {}, index: {}, slot: {}, post_node: {} }}",
            self.id, self.index, self.slot, self.post_node
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
