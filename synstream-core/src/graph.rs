use core::panic;
use rapidhash::RapidHashMap;
use std::collections::HashSet;
use std::sync::Arc;

use crate::graph_struct::*;
use crate::{debug::print_debug, IdType};
use synstream_types::*;

/// Graph structure
#[derive(Clone)]
pub struct Graph {
    pub nodes: Vec<Node>,
    pub initial_nodes: Vec<IdType>,
    pub successors: Vec<Vec<IdType>>,
    pub condition_nodes: HashSet<IdType>,
    pub post_nodes: Option<Vec<Node>>,
    pub init_objects: Option<Vec<Vec<CmTypes>>>,
    pub obj_id_map: RapidHashMap<String, usize>,
    pub network_config: Option<Arc<GraphNetworkConfig>>,
}

/// Compute how many predecessor instances will actually send decrements
/// to this successor.  When a pred_index_filter would narrow down which
/// instances send decrements, the count equals the filtered range.
/// Otherwise ALL predecessor instances contribute (pred_factor).
fn contributing_instances(
    pred: &crate::graph_struct::Predecessor,
    nodes: &[Node],
    succ_factor: usize,
) -> usize {
    let pred_factor = nodes[pred.id as usize].factor;
    if pred.indexes.is_empty() {
        return pred_factor;
    }
    // Replicate the pred_index_filter logic from runtime.rs
    let min_idx = *pred.indexes.iter().min().unwrap() as usize;
    let max_idx = *pred.indexes.iter().max().unwrap() as usize;
    let range_len = max_idx - min_idx + 1;

    let should_filter = if pred.group_by.is_some() {
        true
    } else if range_len < pred_factor && range_len == pred.indexes.len() {
        range_len == succ_factor
    } else {
        false
    };

    if should_filter {
        pred.indexes.len()
    } else {
        pred_factor
    }
}

impl GraphStruct for Graph {
    fn add_node(&mut self, node: Node) {
        // assert that node.id === self.nodes.len()
        assert!(node.id as usize == self.nodes.len());

        let mut has_preds = false;

        // Phase 1.3: Use HashSet for O(1) duplicate detection during construction
        // Collect unique predecessors for this node
        let mut unique_preds = HashSet::new();
        for arg in &node.args {
            if let Some(pred) = &arg.predecessor {
                if !has_preds {
                    has_preds = true;
                }
                unique_preds.insert(pred.id);
            }
        }

        // Add this node to each unique predecessor's successor list
        for pred_id in unique_preds {
            // Ensure successors vec is large enough
            while self.successors.len() <= pred_id as usize {
                self.successors.push(Vec::new());
            }

            // O(1) check instead of O(E) contains() - duplicates already eliminated by HashSet
            self.successors[pred_id as usize].push(node.id);
        }

        if !has_preds && node.name != "$network" {
            print_debug(|| {
                format!(
                    "Adding initial node: {} with id {} and factor {}",
                    node.name, node.id, node.factor
                )
            });
            self.initial_nodes.push(node.id);
        }
        // Check for both arg-based conditions (old format) and node-level conditions (new format)
        if Self::has_condition(&node.args) || node.condition.is_some() {
            self.condition_nodes.insert(node.id);
        }
        self.nodes.push(node);
    }

    fn add_post_node(&mut self, node: Node) {
        if let Some(post_nodes) = &mut self.post_nodes {
            assert!(node.id as usize == post_nodes.len());
            post_nodes.push(node);
        } else {
            let mut post_nodes = Vec::new();
            assert!(node.id == 0);
            post_nodes.push(node);
            self.post_nodes = Some(post_nodes);
        }
    }

    fn find_successors(&self, node_id: IdType) -> &Vec<IdType> {
        if node_id as usize >= self.successors.len() {
            panic!(
                "Node id {} out of bounds for successors with length {}",
                node_id,
                self.successors.len()
            );
        }
        &self.successors[node_id as usize]
    }

    fn dependency_count_vec(&self) -> Vec<usize> {
        // Return a vector with the dependency count for each node.
        //
        // dep_count must equal the total number of decrements the node will
        // receive from all predecessor instances.  When a pred_index_filter
        // narrows which predecessor instances send decrements, the count
        // equals the filtered range (pred.indexes.len()).  When no filter
        // applies, ALL predecessor instances contribute, so the count is
        // pred_factor.  Using pred.indexes.len() in the no-filter case
        // caused deps_per_instance to truncate to 0, making every instance
        // ready after a single predecessor completion (Bug #33).
        //
        // For nodes with group_size, global predecessors (no group_by) have
        // their contribution multiplied by num_groups, since each group
        // counter needs the full set of decrements from global predecessors.
        let mut dep_count_vec: Vec<usize> = Vec::new();

        for node in &self.nodes {
            let mut dep_count = 0;
            let mut preds_seen = HashSet::new();

            let num_groups = match node.group_size {
                Some(gs) if gs > 0 && gs < node.factor => node.factor / gs,
                _ => 1,
            };

            // First check barriers - they take precedence for dependency counting
            for arg in &node.args {
                if arg.type_.is_barrier() {
                    if let Some(pred) = &arg.predecessor {
                        if preds_seen.insert(pred.id) {
                            if let Some(group_by_size) = pred.group_by {
                                // Per-group barrier: dependencies based on BARRIER groups, not instance groups
                                // num_barrier_groups = how many packet groups exist (indexes / group_by)
                                // Example CSI: 64 packets / 64 group_by = 1 barrier group needing 64 deps
                                // Example FFT: 832 packets / 64 group_by = 13 barrier groups, each needing 64 deps
                                let num_barrier_groups = if group_by_size > 0 {
                                    pred.indexes.len() / group_by_size
                                } else {
                                    1
                                };

                                let barrier_deps = num_barrier_groups * group_by_size;
                                print_debug(|| {
                                    format!("DEPCOUNT: node={}, factor={}, group_size={:?}, num_barrier_groups={}, group_by={}, indexes.len()={}, dep_count_before={}, adding={}",
                                    node.name, node.factor, node.group_size, num_barrier_groups, group_by_size, pred.indexes.len(), dep_count, barrier_deps)
                                });
                                dep_count += barrier_deps;
                            } else {
                                let contributing =
                                    contributing_instances(pred, &self.nodes, node.factor);
                                if num_groups > 1 {
                                    // Global barrier: each instance group needs all deps
                                    print_debug(|| {
                                        format!("DEPCOUNT: node={}, factor={}, group_size={:?}, num_groups={}, group_by=None, contributing={}, dep_count_before={}, adding={}",
                                        node.name, node.factor, node.group_size, num_groups, contributing, dep_count, contributing * num_groups)
                                    });
                                    dep_count += contributing * num_groups;
                                } else {
                                    print_debug(|| {
                                        format!("DEPCOUNT: node={}, factor={}, group_size={:?}, num_groups={} (<=1), contributing={}, dep_count_before={}, adding={}",
                                        node.name, node.factor, node.group_size, num_groups, contributing, dep_count, contributing)
                                    });
                                    dep_count += contributing;
                                }
                            }
                        }
                    }
                }
            }

            // Then check non-barrier predecessors
            for arg in &node.args {
                if !arg.type_.is_barrier() {
                    if let Some(pred) = &arg.predecessor {
                        if preds_seen.insert(pred.id) {
                            let contributing =
                                contributing_instances(pred, &self.nodes, node.factor);
                            dep_count += contributing;
                        }
                    }
                }
            }

            // Final summary for this node
            let threshold = if node.factor > 0 {
                dep_count / node.factor
            } else {
                0
            };
            print_debug(|| {
                format!(
                    "DEPCOUNT FINAL: node={}, factor={}, total_dep_count={}, threshold={}",
                    node.name, node.factor, dep_count, threshold
                )
            });

            dep_count_vec.push(dep_count);
        }
        dep_count_vec
    }
}

impl Graph {
    pub fn new() -> Graph {
        Graph {
            nodes: Vec::new(),
            initial_nodes: Vec::new(),
            successors: Vec::new(),
            condition_nodes: HashSet::new(),
            post_nodes: None,
            init_objects: None,
            obj_id_map: RapidHashMap::default(),
            network_config: None,
        }
    }

    pub fn set_nodes(&mut self, nodes: Vec<Node>) {
        self.nodes = nodes;
    }

    pub fn set_init_objects(&mut self, init_objects: &Vec<Vec<CmTypes>>) {
        self.init_objects = Some(init_objects.clone());
    }

    pub fn set_post_nodes(&mut self, post_nodes: Option<Vec<Node>>) {
        self.post_nodes = post_nodes;
    }

    pub fn get_condition_indexes(&self) -> Vec<Vec<usize>> {
        let mut condition_indexes: Vec<Vec<usize>> = Vec::new();
        for cond_id in self.condition_nodes.iter() {
            let node = &self.nodes[*cond_id as usize];
            let condition_arg_indexes: Vec<usize> = node
                .args
                .iter()
                .enumerate()
                .filter_map(|(idx, arg)| arg.init_condition.as_ref().map(|_| idx))
                .collect();

            if !condition_arg_indexes.is_empty() {
                condition_indexes.push(condition_arg_indexes);
            }
        }
        condition_indexes
    }

    pub fn has_barrier(&self, node_id: IdType) -> bool {
        let node = &self.nodes[node_id as usize];
        for arg in &node.args {
            if arg.type_.is_barrier() {
                return true;
            }
        }
        false
    }

    pub fn has_condition(args: &Vec<Arg>) -> bool {
        for arg in args {
            if arg.init_condition.is_some() {
                return true;
            }
        }
        false
    }

    pub fn get_pred_indexes(&self, node_id: IdType, pred_id: IdType) -> Vec<isize> {
        let node = &self.nodes[node_id as usize];
        let args = &node.args;
        let mut pred_idxs = Vec::new();
        for arg in args {
            if arg.type_.is_barrier() {
                if let Some(pred) = &arg.predecessor {
                    if pred.id == pred_id {
                        return pred.indexes.clone();
                    }
                }
            }

            if let Some(pred) = &arg.predecessor {
                if pred.id == pred_id {
                    pred_idxs = pred.indexes.clone();
                }
            }
        }
        pred_idxs
    }

    pub fn set_network_config(&mut self, config: &GraphNetworkConfig) {
        self.network_config = Some(Arc::new(config.clone()));
    }

    pub fn network_config(&self) -> Option<Arc<GraphNetworkConfig>> {
        self.network_config.clone()
    }
}

// Display functions
impl Graph {
    pub fn print_init_objects(&self) {
        if let Some(init_objects) = &self.init_objects {
            println!("Initialized Objects:");
            for (id, obj) in init_objects.iter().enumerate() {
                println!("  {}: {:?}", id, obj);
            }
        } else {
            println!("No initialized objects.");
        }
    }

    pub fn print_graph(&self) {
        println!("Graph:");
        for node in &self.nodes {
            println!("  {}: {:?} ({:?})", node.id, node.name, node.factor);
        }
        if let Some(post_nodes) = &self.post_nodes {
            println!("Post Nodes:");
            for node in post_nodes {
                println!("  {}: {:?} ({:?})", node.id, node.name, node.factor);
            }
        } else {
            println!("No post nodes.");
        }
        println!("Initial Nodes: {:?}", self.initial_nodes);
        println!("Successors: {:?}", self.successors);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_struct::{NodePriority, Predecessor};

    fn make_node(id: u16, factor: usize) -> Node {
        Node {
            name: String::new(),
            args: vec![],
            id,
            loop_args: None,
            factor,
            group_size: None,
            func_ptr: None,
            loop_: None,
            condition: None,
            priority: NodePriority::Normal,
            use_workers: None,
        }
    }

    // Case 1: no pred_index_filter → all pred_factor instances contribute.
    // Regression guard: must never return indexes.len() when indexes is empty.
    #[test]
    fn no_indexes_returns_pred_factor() {
        let nodes = vec![make_node(0, 64)];
        let pred = Predecessor { id: 0, indexes: vec![], group_by: None };
        assert_eq!(contributing_instances(&pred, &nodes, 64), 64);
    }

    // Case 2: group_by present → filter always applies regardless of succ_factor.
    #[test]
    fn group_by_always_filters() {
        let nodes = vec![make_node(0, 64)];
        let pred = Predecessor {
            id: 0,
            indexes: (0..14).collect(),
            group_by: Some(14),
        };
        // succ_factor doesn't match indexes.len() — without group_by this would NOT filter
        assert_eq!(contributing_instances(&pred, &nodes, 64), 14);
    }

    // Case 3: compact range whose length equals succ_factor → filter applies.
    // Models a 14-antenna slice feeding 14 FFT instances (pred_factor=64, succ_factor=14).
    #[test]
    fn range_filter_active_when_range_matches_succ_factor() {
        let nodes = vec![make_node(0, 64)];
        let pred = Predecessor { id: 0, indexes: (0..14).collect(), group_by: None };
        assert_eq!(contributing_instances(&pred, &nodes, 14), 14);
    }

    // Case 4 (Bug #33): indexes present, compact range, but range != succ_factor
    // → filter must NOT apply; must return pred_factor, not indexes.len().
    // This is the exact scenario that broke MIMO: pred CSI (factor=64) feeding FFT
    // (factor=64) with a partial index list present but no filter intended.
    #[test]
    fn indexes_present_but_succ_factor_mismatch_returns_pred_factor() {
        let nodes = vec![make_node(0, 64)];
        let pred = Predecessor { id: 0, indexes: (0..14).collect(), group_by: None };
        // succ_factor=64 ≠ range_len=14 → should_filter=false
        assert_eq!(contributing_instances(&pred, &nodes, 64), 64);
    }

    // Case 5: sparse indexes (range_len > indexes.len()) → filter guard fails → pred_factor.
    // Exercises the `range_len == pred.indexes.len()` condition in the else-if branch.
    #[test]
    fn sparse_indexes_returns_pred_factor() {
        let nodes = vec![make_node(0, 64)];
        // Two non-contiguous entries: range_len = 63-0+1 = 64 = pred_factor,
        // but indexes.len()=2 ≠ range_len → should_filter=false.
        let pred = Predecessor { id: 0, indexes: vec![0, 63], group_by: None };
        assert_eq!(contributing_instances(&pred, &nodes, 2), 64);
    }
}
