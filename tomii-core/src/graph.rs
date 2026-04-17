use core::panic;
use std::collections::HashSet;
use std::sync::Arc;

use crate::graph_struct::*;
use crate::{debug::print_debug, IdType};

/// Pure topological description of a task graph.
///
/// Contains nodes, edges, and metadata derived purely from the graph structure.
/// Materialized initialization objects live in [`crate::graph_gen::GraphSpec`] and
/// are stored in `GraphCache` at runtime — not here.
#[derive(Clone)]
pub struct Graph {
    pub nodes: Vec<Node>,
    pub initial_nodes: Vec<IdType>,
    pub successors: Vec<Vec<IdType>>,
    pub condition_nodes: HashSet<IdType>,
    pub post_nodes: Option<Vec<Node>>,
    pub network_config: Option<Arc<GraphNetworkConfig>>,
}

impl Graph {
    pub fn add_node(&mut self, node: Node) {
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

    pub fn add_post_node(&mut self, node: Node) {
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

    pub fn find_successors(&self, node_id: IdType) -> &Vec<IdType> {
        if node_id as usize >= self.successors.len() {
            panic!(
                "Node id {} out of bounds for successors with length {}",
                node_id,
                self.successors.len()
            );
        }
        &self.successors[node_id as usize]
    }

    pub fn dependency_count_vec(&self) -> Vec<usize> {
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
                                let pred_factor = self.nodes[pred.id as usize].factor;
                                let contributing =
                                    pred.contributing_instances(pred_factor, node.factor);
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
                            let pred_factor = self.nodes[pred.id as usize].factor;
                            let contributing =
                                pred.contributing_instances(pred_factor, node.factor);
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

    pub fn new() -> Graph {
        Graph {
            nodes: Vec::new(),
            initial_nodes: Vec::new(),
            successors: Vec::new(),
            condition_nodes: HashSet::new(),
            post_nodes: None,
            network_config: None,
        }
    }

    pub fn set_nodes(&mut self, nodes: Vec<Node>) {
        self.nodes = nodes;
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
    use std::sync::Arc;
    use tomii_types::CmTypes;

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    fn make_node(id: IdType, factor: usize, args: Vec<Arg>) -> Node {
        Node {
            name: format!("n{}", id),
            args,
            id,
            loop_args: None,
            factor,
            group_size: None,
            func_name: String::new(),
            loop_: None,
            condition: None,
            use_workers: None,
            priority: NodePriority::Normal,
        }
    }

    fn make_node_grouped(id: IdType, factor: usize, group_size: usize, args: Vec<Arg>) -> Node {
        let mut n = make_node(id, factor, args);
        n.group_size = Some(group_size);
        n
    }

    fn res_arg(pred_id: IdType, indexes: Vec<isize>) -> Arg {
        Arg {
            type_: CmTypes::Res(0),
            predecessor: Some(Predecessor {
                id: pred_id,
                indexes,
                group_by: None,
            }),
            init_condition: None,
        }
    }

    fn barrier_arg(pred_id: IdType, indexes: Vec<isize>) -> Arg {
        Arg {
            type_: CmTypes::Barrier(Arc::from("$barrier")),
            predecessor: Some(Predecessor {
                id: pred_id,
                indexes,
                group_by: None,
            }),
            init_condition: None,
        }
    }

    fn barrier_arg_group_by(pred_id: IdType, indexes: Vec<isize>, group_by: usize) -> Arg {
        Arg {
            type_: CmTypes::Barrier(Arc::from("$barrier")),
            predecessor: Some(Predecessor {
                id: pred_id,
                indexes,
                group_by: Some(group_by),
            }),
            init_condition: None,
        }
    }

    // ------------------------------------------------------------------
    // add_node / structural tests
    // ------------------------------------------------------------------

    #[test]
    fn test_add_node_no_preds_is_initial() {
        let mut g = Graph::new();
        g.add_node(make_node(0, 1, vec![]));
        assert_eq!(g.initial_nodes, vec![0]);
    }

    #[test]
    fn test_add_node_with_pred_not_initial() {
        let mut g = Graph::new();
        g.add_node(make_node(0, 1, vec![]));
        g.add_node(make_node(1, 1, vec![res_arg(0, vec![0])]));
        assert_eq!(g.initial_nodes, vec![0]);
        assert!(!g.initial_nodes.contains(&1));
    }

    #[test]
    fn test_add_node_builds_successor_list() {
        let mut g = Graph::new();
        g.add_node(make_node(0, 1, vec![]));
        g.add_node(make_node(1, 1, vec![res_arg(0, vec![0])]));
        assert_eq!(g.find_successors(0), &vec![1]);
    }

    #[test]
    fn test_add_node_fan_out_successor_list() {
        let mut g = Graph::new();
        g.add_node(make_node(0, 1, vec![])); // A
        g.add_node(make_node(1, 1, vec![res_arg(0, vec![0])])); // B → A
        g.add_node(make_node(2, 1, vec![res_arg(0, vec![0])])); // C → A
        let succs = g.find_successors(0);
        assert_eq!(succs.len(), 2);
        assert!(succs.contains(&1));
        assert!(succs.contains(&2));
    }

    #[test]
    #[should_panic]
    fn test_find_successors_out_of_bounds_panics() {
        let g = Graph::new();
        g.find_successors(0);
    }

    #[test]
    fn test_add_node_condition_detected() {
        let cond_arg = Arg {
            type_: CmTypes::Res(0),
            predecessor: Some(Predecessor {
                id: 0,
                indexes: vec![0],
                group_by: None,
            }),
            init_condition: Some(InitCondition {
                operation: CondOp::Eq,
                eval_value: CmTypes::Bool(true),
            }),
        };
        let mut g = Graph::new();
        g.add_node(make_node(0, 1, vec![]));
        g.add_node(make_node(1, 1, vec![cond_arg]));
        assert!(g.condition_nodes.contains(&1));
        assert!(!g.condition_nodes.contains(&0));
    }

    // ------------------------------------------------------------------
    // dependency_count_vec — the Bug #33 regression suite
    // ------------------------------------------------------------------

    #[test]
    fn test_dep_count_single_node_no_preds() {
        let mut g = Graph::new();
        g.add_node(make_node(0, 1, vec![]));
        assert_eq!(g.dependency_count_vec(), vec![0]);
    }

    #[test]
    fn test_dep_count_linear_chain() {
        // A(f=1) → B(f=1): B needs 1 dep, threshold = 1/1 = 1
        let mut g = Graph::new();
        g.add_node(make_node(0, 1, vec![]));
        g.add_node(make_node(1, 1, vec![res_arg(0, vec![0])]));
        let counts = g.dependency_count_vec();
        assert_eq!(counts[0], 0);
        assert_eq!(counts[1], 1);
    }

    #[test]
    fn test_dep_count_fan_in_barrier() {
        // A(f=4) → B(f=1, barrier): B needs all 4 deps (contributing=4)
        let mut g = Graph::new();
        g.add_node(make_node(0, 4, vec![]));
        g.add_node(make_node(1, 1, vec![barrier_arg(0, vec![0, 1, 2, 3])]));
        let counts = g.dependency_count_vec();
        assert_eq!(counts[0], 0);
        assert_eq!(counts[1], 4);
    }

    #[test]
    fn test_dep_count_fan_out_broadcast() {
        // A(f=1) → B(f=4): pred_factor=1, no filter, contributing=1, dep_count=1
        // All 4 B instances fire when A's single instance completes.
        let mut g = Graph::new();
        g.add_node(make_node(0, 1, vec![]));
        g.add_node(make_node(1, 4, vec![res_arg(0, vec![0])]));
        let counts = g.dependency_count_vec();
        assert_eq!(counts[1], 1);
    }

    #[test]
    fn test_dep_count_equal_factor_bug33_fix() {
        // Bug #33 regression: A(f=200) → B(f=200, res, indexes=[0]).
        // Before fix: dep_count = indexes.len() = 1 → deps_per_instance=0 → all fire at once.
        // After fix:  dep_count = pred_factor = 200 → 1 dep per instance (correct 1:1).
        let mut g = Graph::new();
        g.add_node(make_node(0, 200, vec![]));
        g.add_node(make_node(1, 200, vec![res_arg(0, vec![0])]));
        let counts = g.dependency_count_vec();
        assert_eq!(
            counts[1], 200,
            "dep_count must equal pred_factor for equal-factor non-filtered edge"
        );
    }

    #[test]
    fn test_dep_count_filtered_subset() {
        // A(f=200) → B(f=2, indexes=[50, 51]): filter applies, contributing=2
        let mut g = Graph::new();
        g.add_node(make_node(0, 200, vec![]));
        g.add_node(make_node(1, 2, vec![res_arg(0, vec![50, 51])]));
        let counts = g.dependency_count_vec();
        assert_eq!(counts[1], 2);
    }

    #[test]
    fn test_dep_count_global_barrier_with_groups() {
        // A(f=8) → B(f=8, group_size=4, barrier, no group_by).
        // num_groups=2, contributing=8 (no filter), dep_count = 8 * num_groups = 16
        let mut g = Graph::new();
        g.add_node(make_node(0, 8, vec![]));
        g.add_node(make_node_grouped(
            1,
            8,
            4,
            vec![barrier_arg(0, vec![0, 1, 2, 3, 4, 5, 6, 7])],
        ));
        let counts = g.dependency_count_vec();
        assert_eq!(counts[1], 16);
    }

    #[test]
    fn test_dep_count_per_group_barrier_group_by() {
        // A(f=8) → B(f=8, barrier with group_by=4, indexes=[0..8]).
        // num_barrier_groups = 8/4 = 2, barrier_deps = 2 * 4 = 8
        let mut g = Graph::new();
        g.add_node(make_node(0, 8, vec![]));
        g.add_node(make_node(
            1,
            8,
            vec![barrier_arg_group_by(
                0,
                (0..8).map(|i| i as isize).collect(),
                4,
            )],
        ));
        let counts = g.dependency_count_vec();
        assert_eq!(counts[1], 8);
    }

    #[test]
    fn test_dep_count_multiple_predecessors() {
        // A(f=1) → C, B(f=1) → C: C has two distinct preds, dep_count = 1 + 1 = 2
        let mut g = Graph::new();
        g.add_node(make_node(0, 1, vec![])); // A
        g.add_node(make_node(1, 1, vec![])); // B
        g.add_node(make_node(
            2,
            1,
            vec![res_arg(0, vec![0]), res_arg(1, vec![0])],
        ));
        let counts = g.dependency_count_vec();
        assert_eq!(counts[2], 2);
    }

    #[test]
    fn test_dep_count_same_pred_twice_deduped() {
        // Same predecessor referenced from two args — preds_seen deduplicates it
        let mut g = Graph::new();
        g.add_node(make_node(0, 1, vec![]));
        g.add_node(make_node(
            1,
            1,
            vec![res_arg(0, vec![0]), res_arg(0, vec![0])],
        ));
        let counts = g.dependency_count_vec();
        assert_eq!(
            counts[1], 1,
            "same pred counted twice would give 2; preds_seen should prevent that"
        );
    }
}
