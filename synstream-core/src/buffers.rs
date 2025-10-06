use crate::graph_struct::Node;
use std::cmp::PartialEq;
use std::fmt::Debug;
use std::{collections::HashMap, vec};

#[derive(Clone, PartialEq)]
pub struct NodeID {
    pub name: String,
    pub slot: usize,
    pub index: usize,
    pub post_node: bool,
}

impl NodeID {
    pub fn new(name: String, slot: usize, index: usize) -> NodeID {
        NodeID {
            name,
            slot,
            index,
            post_node: false,
        }
    }

    pub fn set_post_node(&mut self, post_node: bool) {
        self.post_node = post_node;
    }
}

impl std::fmt::Debug for NodeID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "NodeID {{ name: {}, index: {}, slot: {}, post_node: {} }}",
            self.name, self.index, self.slot, self.post_node
        )
    }
}

pub struct Buffer<T> {
    buffer: Vec<HashMap<String, Vec<T>>>,
    init_val: T,
}

impl<T: Clone + PartialEq + Debug> Buffer<T> {
    pub fn new(init_val: T) -> Buffer<T> {
        Buffer {
            buffer: Vec::new(),
            init_val,
        }
    }

    pub fn init_buffer(&mut self, nodes: &HashMap<String, Node>, slots: usize)
    where
        T: Clone,
    {
        if self.buffer.is_empty() {
            // Initialize buffer with empty HashMaps for each stream
            for _ in 0..slots {
                self.buffer.push(HashMap::new());
            }
        }

        // iterate over the nodes map to create a vector for each node
        for (node_name, node) in nodes.iter() {
            let factor = node.factor;
            let new_vec = vec![self.init_val.clone(); factor];
            // Initialize HashMap for each stream
            for stream in 0..self.buffer.len() {
                self.buffer[stream].insert(node_name.clone(), new_vec.clone());
            }
        }
    }

    pub fn add_buffer(&mut self, nodes: &HashMap<String, Node>)
    where
        T: Clone,
    {
        // Add a new buffer to self.buffer
        let mut new_buffer = HashMap::new();
        for (node_name, node) in nodes.iter() {
            let factor = node.factor;
            let new_vec = vec![self.init_val.clone(); factor];
            new_buffer.insert(node_name.clone(), new_vec);
        }
        self.buffer.push(new_buffer);
    }

    pub fn clear_buffer(&mut self) {
        for buf in self.buffer.iter_mut() {
            buf.clear();
        }
    }

    pub fn clear_slot(&mut self, slot: usize) {
        if slot < self.buffer.len() {
            let clean_buf = &mut self.buffer[slot];
            for node_name in clean_buf.keys().cloned().collect::<Vec<String>>() {
                if let Some(node_vec) = clean_buf.get_mut(&node_name) {
                    for i in 0..node_vec.len() {
                        node_vec[i] = self.init_val.clone();
                    }
                }
            }
        } else {
            panic!("Slot {} out of bounds for buffer", slot);
        }
    }

    pub fn get_buffer(&self, slot: usize) -> &HashMap<String, Vec<T>> {
        &self.buffer[slot]
    }

    pub fn search_node_idx(&self, node_name: &str, index: usize, slot: usize) -> Option<T>
    where
        T: Clone,
    {
        if let Some(vec) = self.buffer[slot].get(node_name) {
            if index < vec.len() {
                Some(vec[index].clone())
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn add_element_index(&mut self, node_name: &str, index: usize, element: T, slot: usize) {
        if let Some(vec) = self.buffer[slot].get_mut(node_name) {
            if index < vec.len() {
                vec[index] = element;
            } else {
                panic!("Index {} out of bounds for node {}", index, node_name);
            }
        } else {
            panic!("Node {} not found in buffer", node_name);
        }
    }

    pub fn result_exists(&self, node_name: &str, index: usize, slot: usize) -> bool {
        if let Some(vec) = self.buffer[slot].get(node_name) {
            if index < vec.len() {
                if &vec[index] == &self.init_val {
                    false
                } else {
                    true
                }
            } else {
                panic!("Index {} out of bounds for node {}", index, node_name);
            }
        } else {
            panic!("Node {} not found in buffer", node_name);
        }
    }
}
