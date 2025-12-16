use deepsize::DeepSizeOf;

use crate::graph_struct::Node;
use crate::IdType;
use std::cmp::PartialEq;
use std::fmt::Debug;
use std::vec;

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
    buffer: Vec<Vec<Vec<T>>>,
    init_val: T,
    init_map_copy: Vec<Vec<Vec<T>>>,
}

impl<T: Clone + PartialEq + Debug> VecMap<T> {
    pub fn new(init_val: T) -> VecMap<T> {
        VecMap {
            buffer: Vec::new(),
            init_val,
            init_map_copy: Vec::new(),
        }
    }

    pub fn init_map(&mut self, nodes: &Vec<Node>, slots: usize, init_values: Option<Vec<T>>) {
        if self.buffer.is_empty() {
            for _ in 0..slots {
                self.buffer.push(vec![Vec::new(); nodes.len()]);
            }
        }

        // iterate over the nodes map to create a vector for each node
        for node in nodes.iter() {
            let new_vec = {
                if let Some(init_vals) = &init_values {
                    vec![init_vals[node.id as usize].clone(); node.factor]
                } else {
                    vec![self.init_val.clone(); node.factor]
                }
            };
            // Initialize Vec for each stream
            for stream in 0..self.buffer.len() {
                self.buffer[stream][node.id as usize] = new_vec.clone();
            }
        }

        self.init_map_copy = self.buffer.clone();
    }

    pub fn extend_map(&mut self, nodes: &Vec<Node>) {
        let mut new_buffer = vec![Vec::new(); nodes.len()];

        for node in nodes.iter() {
            let new_vec = vec![self.init_val.clone(); node.factor];
            new_buffer[node.id as usize] = new_vec;
        }
        self.buffer.push(new_buffer);
    }

    pub fn get(&self, node_info: &NodeInfo) -> Option<T> {
        if node_info.slot < self.buffer.len() {
            let slot_vec = &self.buffer[node_info.slot];
            let node_vec = &slot_vec[node_info.id as usize];
            if node_info.index < node_vec.len() {
                return Some(node_vec[node_info.index].clone());
            }
        }

        None
    }

    pub fn result_exists(&self, node_info: &NodeInfo) -> bool {
        if node_info.slot < self.buffer.len() {
            let slot_vec = &self.buffer[node_info.slot];
            let node_vec = &slot_vec[node_info.id as usize];
            if node_info.index < node_vec.len() {
                if node_vec[node_info.index] != self.init_val {
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
        if node_info.slot < self.buffer.len() {
            let slot_vec = &mut self.buffer[node_info.slot];
            let node_vec = &mut slot_vec[node_info.id as usize];

            let cur_val = &mut node_vec[node_info.index];
            let current: usize = (*cur_val).clone().into();
            if current > 0 {
                *cur_val = T::from(current - 1);
                return Some(current - 1);
            }
            return Some(current);
        }
        None
    }

    pub fn set(&mut self, node_info: &NodeInfo, element: T) {
        if node_info.slot < self.buffer.len() {
            let slot_vec = &mut self.buffer[node_info.slot];
            let node_vec = &mut slot_vec[node_info.id as usize];
            if node_info.index < node_vec.len() {
                node_vec[node_info.index] = element;
            } else {
                panic!(
                    "Index {} out of bounds for node {}",
                    node_info.index, node_info.id
                );
            }
        } else {
            panic!("Slot {} out of bounds", node_info.slot);
        }
    }

    pub fn reinit_slot(&mut self, slot: usize) {
        if slot < self.buffer.len() {
            self.buffer[slot] = self.init_map_copy[slot].clone();
        } else {
            panic!("Slot {} out of bounds", slot);
        }
    }

    pub fn reinit_elem(&mut self, node_info: &NodeInfo) {
        if node_info.slot < self.buffer.len() {
            let slot_vec = &mut self.buffer[node_info.slot];
            let node_vec = &mut slot_vec[node_info.id as usize];
            if node_info.index < node_vec.len() {
                node_vec[node_info.index] = self.init_val.clone();
            } else {
                panic!(
                    "Index {} out of bounds for node {}",
                    node_info.index, node_info.id
                );
            }
        } else {
            panic!("Slot {} out of bounds", node_info.slot);
        }
    }
}

impl<T: Clone + PartialEq + Debug> Debug for VecMap<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "VecMap {{")?;
        for (i, slot) in self.buffer.iter().enumerate() {
            writeln!(f, "  Slot {}:", i)?;
            for (j, node) in slot.iter().enumerate() {
                writeln!(f, "    Node {}: {:?}", j, node)?;
            }
        }
        write!(f, "}}")
    }
}
