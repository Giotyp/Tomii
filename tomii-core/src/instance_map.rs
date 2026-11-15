//! Explicit per-instance dependency maps (`instance_map`) on predecessor edges.
//!
//! The classic predecessor convention (`indexes` / `group_by`) expresses only
//! uniform shapes: a single relative offset, a full fan-in, or a `group_by`
//! partition. `instance_map` is a third, mutually-exclusive way to declare an
//! edge: `instance_map[j]` lists the ABSOLUTE producer instance indexes that
//! consumer instance `j` depends on, for arbitrary (irregular) fan-in shapes —
//! e.g. an irregular reduction tree where some internal nodes join a leaf with
//! another internal node, or a neighbourhood (halo) stencil where tile `j`
//! waits on tiles `j-1, j, j+1`.
//!
//! # Scope of this implementation
//!
//! - Only `$barrier` and `$dep` arguments may carry an `instance_map` — never
//!   `$res`. A mapped edge never delivers a value (barrier/dep semantics are
//!   gating-only already), so this restriction avoids extending the
//!   value-delivery path (`arg_resolution.rs`) for marginal benefit. See the
//!   top-level report for the rationale.
//! - A node with `group_size` set may not have an `instance_map` predecessor
//!   (rejected at validation) — the two partitioning schemes are not
//!   reconciled in this release.
//! - A consumer instance whose TOTAL threshold (summed across every
//!   predecessor, mapped or not) is zero — an empty `instance_map[j]` entry
//!   on every mapped predecessor, with no coexisting predecessor
//!   contributing either — is dispatched exactly once per frame at frame
//!   admission, like a root/initial node, instead of through the
//!   dependency-counter path (nothing would ever decrement it: no routing
//!   table targets an instance whose forward map is empty). See
//!   [`validate_and_build`]'s `zero_threshold_instances` output and
//!   `runtime::slot_management::initial_nodes`.
//! - A non-mapped predecessor may coexist on the same node as a mapped one
//!   (e.g. a mapped `$barrier` plus an ordinary 1:1 `$res`), but only in
//!   shapes with a uniform per-instance contribution: a literal/filtered
//!   `$barrier` or `$dep` (broadcast — every instance waits for the same N
//!   arrivals), a producer with factor 1 (broadcast of a single arrival), or
//!   an exact 1:1 same-factor single-offset edge. Any other shape (the
//!   "ordinal" fallback used for irregular fan-in/fan-out without a declared
//!   1:1/filter/barrier) is rejected with a clear error — see
//!   `classify_coexisting_predecessor`.
//!
//! # Threshold / union semantics
//!
//! `threshold(node, j)` is the sum, over every predecessor argument of
//! `node`, of that argument's contribution to instance `j`:
//! - A mapped argument contributes `instance_map[j].len()`.
//! - Multiple mapped arguments on the SAME predecessor contribute the size of
//!   the UNION of their `instance_map[j]` lists (not the sum) — mirrors the
//!   existing union convention for ordinary edges (`Graph::union_pred_indexes`,
//!   from the same-predecessor multi-arg fixes). A producer instance listed by
//!   two mapped arguments of the same consumer instance is therefore counted,
//!   and decremented, exactly once.
//! - A non-mapped (coexisting) predecessor contributes the same, uniform
//!   amount to every instance — see `classify_coexisting_predecessor`.
//!
//! # Routing
//!
//! For each mapped `(pred, succ)` edge, [`MappedEdgeRouting::build`]
//! pre-inverts the per-consumer forward map into a per-producer reverse map:
//! `producer_to_consumers[i]` lists every consumer instance `j` such that `i`
//! is in the (unioned) `instance_map[j]`. On completion of producer instance
//! `i`, the runtime decrements exactly those listed consumer instances, once
//! per listing — see `runtime::successor::decrement_and_collect_ready_mapped`.

use std::collections::HashSet;
use std::sync::Arc;

use crate::graph::Graph;
use crate::graph_struct::{pred_is_ambiguous_multi_arg, Arg, Node, Predecessor};
use crate::IdType;

/// Reverse producer→consumer routing table for one mapped `(pred, succ)` edge.
///
/// `producer_to_consumers[i]` lists every consumer instance that depends on
/// producer instance `i` through this edge (the union of every mapped
/// argument on this predecessor, inverted). Built once at graph-compile time;
/// shared (via `Arc`) by every slot.
#[derive(Debug)]
pub struct MappedEdgeRouting {
    pub producer_to_consumers: Vec<Vec<u32>>,
}

impl MappedEdgeRouting {
    /// Build the reverse routing table from the (already unioned) forward map.
    /// `pred_factor` sizes the reverse array; `forward[j]` entries are assumed
    /// already validated to be `< pred_factor` with no duplicates.
    fn build(forward: &[Vec<usize>], pred_factor: usize) -> Self {
        let mut producer_to_consumers: Vec<Vec<u32>> = vec![Vec::new(); pred_factor];
        for (j, list) in forward.iter().enumerate() {
            for &i in list {
                producer_to_consumers[i].push(j as u32);
            }
        }
        Self {
            producer_to_consumers,
        }
    }
}

/// Per-node tables produced by [`validate_and_build`] for every node that has
/// at least one `instance_map` predecessor.
#[derive(Debug)]
pub struct InstanceMapTables {
    /// `[succ][pred] -> Some(routing)` for every mapped edge; dense, matches
    /// the shape of the existing `pred_index_filter`/`pred_group_by` tables.
    pub pred_instance_map: Vec<Vec<Option<Arc<MappedEdgeRouting>>>>,
    /// `[node] -> Some(thresholds)` where `thresholds[j]` is the total
    /// dependency count for consumer instance `j`. `None` for nodes with no
    /// `instance_map` predecessor (the overwhelming majority — unaffected).
    pub node_thresholds: Vec<Option<Arc<Vec<u32>>>>,
    /// `[node] -> bool`, mirrors `node_thresholds[node].is_some()`; kept as a
    /// separate flat bitmap for cheap per-edge lookup at arena-build time.
    pub has_instance_map: Vec<bool>,
    /// `[node] -> Some(instance indices)` for every `instance_map` node that
    /// has at least one consumer instance whose total dependency threshold is
    /// 0 (an empty map entry on every mapped predecessor, with no other
    /// predecessor contributing). `None` when every instance has a nonzero
    /// threshold (the common case). These specific instances are never
    /// targeted by any routing table (nothing decrements them — see
    /// `MappedEdgeRouting::build`), so they are dispatched once per frame at
    /// admission time instead, exactly like a root/initial node — see
    /// `runtime::slot_management::initial_nodes`'s `zero_threshold_instances`
    /// handling and `runtime::shared_data::GraphCache::zero_threshold_instances`.
    pub zero_threshold_instances: Vec<Option<Arc<Vec<u32>>>>,
}

/// How a non-mapped predecessor coexisting with a mapped one on the same node
/// contributes to every instance's threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UniformKind {
    /// Each producer instance `i` fires exactly consumer instance `j = i - k`
    /// (mod factor) — one decrement, to one specific instance, per arrival.
    OneToOne,
    /// Every arrival decrements EVERY consumer instance by one — the producer
    /// is a literal/filtered barrier, a `$dep`, or a factor-1 fan-out source.
    Broadcast,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct UniformContribution {
    /// Only read by tests (`classify_coexisting_predecessor_kind_matches_shape`),
    /// which assert this classification agrees with the independently-derived
    /// `SuccEdge::is_one_to_one()` shape the runtime actually dispatches on —
    /// a deliberate cross-check between the two call sites documented in
    /// `runtime::init::build_predecessor_tables`'s `filter_succ_factor` override.
    #[allow(dead_code)]
    pub kind: UniformKind,
    /// Total contribution added to every instance's threshold.
    pub amount: u32,
}

/// Classify a non-mapped predecessor coexisting, on `node`, with at least one
/// `instance_map` predecessor. Returns the uniform per-instance contribution,
/// or `Err(reason)` when the edge's shape has no uniform interpretation (the
/// "ordinal fallback" shape — see module docs).
pub(crate) fn classify_coexisting_predecessor(
    graph: &Graph,
    node: &Node,
    pred_id: IdType,
) -> Result<UniformContribution, String> {
    let pred_factor = graph.nodes[pred_id as usize].factor;
    let succ_factor = node.factor;

    if pred_is_ambiguous_multi_arg(node, pred_id) {
        // Two or more genuinely different, barrier-free args on the SAME
        // non-mapped predecessor (e.g. `$res "0"` + `$res "-1"`). Reconciling
        // this with `runtime::init::build_predecessor_tables`'s filter
        // computation for a mapped successor (which must force
        // `succ_factor=1` semantics — see below) would require threading that
        // override through the "genuinely differing args" branch too; reject
        // instead and keep this release's scope to the well-defined single
        // literal-barrier / single-$dep / exact-1:1 shapes.
        return Err(format!(
            "predecessor '{}' has two or more differing dependency arguments on node \
             '{}' (ambiguous multi-arg) — not supported coexisting with an instance_map \
             predecessor on the same node.",
            graph.nodes[pred_id as usize].name, node.name
        ));
    }

    let args_on_pred: Vec<&Arg> = node
        .args
        .iter()
        .filter(|a| a.predecessor.as_ref().is_some_and(|p| p.id == pred_id))
        .collect();
    let any_barrier = args_on_pred.iter().any(|a| a.is_barrier());
    let any_group_by = args_on_pred
        .iter()
        .any(|a| a.predecessor.as_ref().unwrap().group_by.is_some());
    if any_group_by {
        // A grouped barrier is routed through a separate branch of
        // `runtime::init::build_predecessor_tables` (the `grouped_barrier`
        // case) that this module's forced-`succ_factor=1` filter override
        // does not touch — reconciling the two would require threading the
        // override through that branch as well. Reject instead; the mapped
        // node's own `group_size` is already unsupported in this release, so
        // a coexisting predecessor's `group_by` buys it nothing extra anyway
        // (no dependency-counter groups exist to align it with).
        return Err(format!(
            "predecessor '{}' uses `group_by` on node '{}' — not supported coexisting with \
             an instance_map predecessor on the same node.",
            graph.nodes[pred_id as usize].name, node.name
        ));
    }

    let union_indexes = graph.union_pred_indexes(node, pred_id);
    let mut sorted = union_indexes.clone();
    sorted.sort_unstable();

    let single_1to1 =
        !any_barrier && !any_group_by && sorted.len() == 1 && succ_factor == pred_factor;
    let contiguous_range_1to1 = !any_barrier
        && !any_group_by
        && sorted.len() == succ_factor
        && succ_factor > 0
        && sorted.windows(2).all(|w| w[1] == w[0] + 1);

    if single_1to1 || contiguous_range_1to1 {
        return Ok(UniformContribution {
            kind: UniformKind::OneToOne,
            amount: 1,
        });
    }

    // Broadcast-eligible shapes: a literal barrier (any filter), a factor-1
    // producer (single arrival fans out to everyone), or a filtered range
    // (forcing succ_factor=1 semantics in `index_filter`, i.e. "this consumer
    // needs exactly this absolute subset", which is true uniformly here since
    // every instance of a broadcast edge needs the SAME subset).
    let union_pred = Predecessor {
        id: pred_id,
        indexes: union_indexes,
        group_by: None,
        instance_map: None,
    };
    let filter_applies = union_pred.index_filter(pred_factor, 1).is_some();
    if any_barrier || pred_factor == 1 || filter_applies {
        let contributing = union_pred.contributing_instances(pred_factor, 1);
        return Ok(UniformContribution {
            kind: UniformKind::Broadcast,
            amount: contributing as u32,
        });
    }

    Err(format!(
        "node '{}': predecessor '{}' cannot coexist with an instance_map predecessor on the \
         same node — its dependency shape (factor {} -> {}, no exact 1:1 offset, no barrier, \
         no filtered range) has no uniform per-instance interpretation. Use a $barrier, a \
         filtered range (e.g. `.out(a, b)`), or a 1:1 (same-factor, single-offset) edge instead.",
        node.name, graph.nodes[pred_id as usize].name, pred_factor, succ_factor
    ))
}

/// Union, across every `instance_map` argument `node` has on `pred_id`, of the
/// per-consumer-instance absolute index lists. Mirrors
/// `Graph::union_pred_indexes`'s role for the classic convention: a producer
/// instance listed by two mapped arguments of the same consumer instance is
/// counted, and later decremented, exactly once.
fn union_instance_maps(node: &Node, pred_id: IdType, factor: usize) -> Vec<Vec<usize>> {
    let mut union: Vec<HashSet<usize>> = vec![HashSet::new(); factor];
    for arg in &node.args {
        let Some(pred) = &arg.predecessor else {
            continue;
        };
        if pred.id != pred_id {
            continue;
        }
        let Some(map) = &pred.instance_map else {
            continue;
        };
        for (j, list) in map.iter().enumerate() {
            if j < factor {
                union[j].extend(list.iter().copied());
            }
        }
    }
    union
        .into_iter()
        .map(|s| {
            let mut v: Vec<usize> = s.into_iter().collect();
            v.sort_unstable();
            v
        })
        .collect()
}

/// Validate every `instance_map` declaration in `graph` and build the routing
/// tables the runtime needs (reverse producer→consumer map, per-instance
/// thresholds). Returns `Err` with a human-readable reason on the first
/// violation found — see module docs for the exact rules enforced.
pub fn validate_and_build(graph: &Graph) -> Result<InstanceMapTables, String> {
    let num_nodes = graph.nodes.len();
    let mut pred_instance_map: Vec<Vec<Option<Arc<MappedEdgeRouting>>>> =
        vec![vec![None; num_nodes]; num_nodes];
    let mut node_thresholds: Vec<Option<Arc<Vec<u32>>>> = vec![None; num_nodes];
    let mut has_instance_map: Vec<bool> = vec![false; num_nodes];
    let mut zero_threshold_instances: Vec<Option<Arc<Vec<u32>>>> = vec![None; num_nodes];

    for node in &graph.nodes {
        let succ_id = node.id as usize;

        // Predecessors referenced via instance_map on this node (deduped).
        let mut mapped_preds: Vec<IdType> = Vec::new();
        // Predecessors NOT referenced via instance_map on this node (deduped).
        let mut plain_preds: Vec<IdType> = Vec::new();
        for arg in &node.args {
            let Some(pred) = &arg.predecessor else {
                continue;
            };
            if pred.instance_map.is_some() {
                if !mapped_preds.contains(&pred.id) {
                    mapped_preds.push(pred.id);
                }
            } else if !plain_preds.contains(&pred.id) {
                plain_preds.push(pred.id);
            }
        }

        if mapped_preds.is_empty() {
            continue;
        }

        // Reject mixing mapped and non-mapped args on the SAME predecessor.
        for &pid in &mapped_preds {
            if plain_preds.contains(&pid) {
                return Err(format!(
                    "node '{}': predecessor '{}' is referenced via instance_map in one \
                     argument and via positional indexes/group_by in another — use \
                     instance_map consistently for every argument that reads this \
                     predecessor.",
                    node.name, graph.nodes[pid as usize].name
                ));
            }
        }

        // $res + instance_map is rejected (see module docs): a mapped edge
        // never delivers a value in this release.
        for arg in &node.args {
            let Some(pred) = &arg.predecessor else {
                continue;
            };
            if pred.instance_map.is_some() && arg.type_.is_result() {
                return Err(format!(
                    "node '{}': predecessor '{}' uses instance_map on a $res argument — \
                     not supported in this release (instance_map is only valid on \
                     $barrier/$dep arguments; a mapped edge never delivers a value). Use \
                     $barrier or $dep for the gating dependency and read values through an \
                     ordinary $res/$ref edge instead.",
                    node.name, graph.nodes[pred.id as usize].name
                ));
            }
        }

        if node.group_size.is_some() {
            return Err(format!(
                "node '{}': `group_size` is not supported together with an instance_map \
                 predecessor in this release.",
                node.name
            ));
        }
        if node.condition.is_some() || Graph::has_condition(&node.args) {
            return Err(format!(
                "node '{}': a runtime condition is not supported together with an \
                 instance_map predecessor in this release.",
                node.name
            ));
        }

        let factor = node.factor;
        let mut thresholds: Vec<u32> = vec![0; factor];

        // Validate + union + route every mapped predecessor.
        for &pred_id in &mapped_preds {
            let pred_factor = graph.nodes[pred_id as usize].factor;
            let pred_name = &graph.nodes[pred_id as usize].name;

            // Per-arg length validation against the DECLARED map (pre-union):
            // every arg on this pred must itself declare exactly `factor` lists.
            for arg in &node.args {
                let Some(pred) = &arg.predecessor else {
                    continue;
                };
                if pred.id != pred_id {
                    continue;
                }
                let Some(map) = &pred.instance_map else {
                    continue;
                };
                if map.len() != factor {
                    return Err(format!(
                        "node '{}': instance_map on predecessor '{}' has {} entries, \
                         expected one per consumer instance (factor {})",
                        node.name,
                        pred_name,
                        map.len(),
                        factor
                    ));
                }
                for (j, list) in map.iter().enumerate() {
                    for &idx in list {
                        if idx >= pred_factor {
                            return Err(format!(
                                "node '{}': instance_map[{}] on predecessor '{}' lists \
                                 producer index {}, out of range for producer factor {}",
                                node.name, j, pred_name, idx, pred_factor
                            ));
                        }
                    }
                }
            }

            let union = union_instance_maps(node, pred_id, factor);
            for (j, list) in union.iter().enumerate() {
                thresholds[j] += list.len() as u32;
            }
            let routing = MappedEdgeRouting::build(&union, pred_factor);
            pred_instance_map[succ_id][pred_id as usize] = Some(Arc::new(routing));
        }

        // Non-mapped, coexisting predecessors: uniform contribution per instance.
        for &pred_id in &plain_preds {
            let contribution = classify_coexisting_predecessor(graph, node, pred_id)
                .map_err(|e| format!("node '{}': {}", node.name, e))?;
            for t in thresholds.iter_mut() {
                *t += contribution.amount;
            }
        }

        // A consumer instance whose total threshold is 0 (an empty map entry
        // on every mapped predecessor, with no coexisting predecessor
        // contributing either) is never targeted by any routing table built
        // above — nothing will ever decrement it, so it would otherwise never
        // run. Dispatch it once per frame at admission instead, exactly like a
        // root/initial node (see `runtime::slot_management::initial_nodes`).
        let zero_at: Vec<u32> = thresholds
            .iter()
            .enumerate()
            .filter(|&(_, &t)| t == 0)
            .map(|(j, _)| j as u32)
            .collect();
        if !zero_at.is_empty() {
            zero_threshold_instances[succ_id] = Some(Arc::new(zero_at));
        }

        node_thresholds[succ_id] = Some(Arc::new(thresholds));
        has_instance_map[succ_id] = true;
    }

    Ok(InstanceMapTables {
        pred_instance_map,
        node_thresholds,
        has_instance_map,
        zero_threshold_instances,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_struct::{NodeCondition, NodePriority};
    use tomii_types::CmTypes;

    fn make_node(id: IdType, factor: usize, args: Vec<Arg>) -> Node {
        Node {
            name: format!("n{id}"),
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

    fn dep_arg(pred_id: IdType, map: Vec<Vec<usize>>) -> Arg {
        Arg {
            type_: CmTypes::Dep(pred_id as usize),
            predecessor: Some(Predecessor {
                id: pred_id,
                indexes: Vec::new(),
                group_by: None,
                instance_map: Some(map),
            }),
            init_condition: None,
        }
    }

    fn barrier_arg(pred_id: IdType, indexes: Vec<isize>) -> Arg {
        Arg {
            type_: CmTypes::Barrier(std::sync::Arc::from("$barrier")),
            predecessor: Some(Predecessor {
                id: pred_id,
                indexes,
                group_by: None,
                instance_map: None,
            }),
            init_condition: None,
        }
    }

    fn res_arg(pred_id: IdType, indexes: Vec<isize>) -> Arg {
        Arg {
            type_: CmTypes::Res(pred_id as usize),
            predecessor: Some(Predecessor {
                id: pred_id,
                indexes,
                group_by: None,
                instance_map: None,
            }),
            init_condition: None,
        }
    }

    /// Irregular binary-tree shape: 4 leaves (factor 4), one internal node
    /// (factor 1) that fuses leaf 0 + leaf 1 via instance_map (equivalent to a
    /// 2-element indexes list, exercised here via the map path instead).
    #[test]
    fn simple_fuse_two_leaves_builds_threshold_two() {
        let mut g = Graph::new();
        g.add_node(make_node(0, 4, vec![])); // leaves
        g.add_node(make_node(
            1,
            1,
            vec![dep_arg(0, vec![vec![0, 1]])], // fuse: instance 0 needs leaves {0,1}
        ));
        let tables = validate_and_build(&g).unwrap();
        assert_eq!(
            *tables.node_thresholds[1].as_ref().unwrap().as_ref(),
            vec![2]
        );
        let routing = tables.pred_instance_map[1][0].as_ref().unwrap();
        assert_eq!(routing.producer_to_consumers[0], vec![0]);
        assert_eq!(routing.producer_to_consumers[1], vec![0]);
        assert_eq!(routing.producer_to_consumers[2], Vec::<u32>::new());
    }

    #[test]
    fn halo_wrap_n8_radius1_thresholds_all_three() {
        // tile j depends on tiles j-1, j, j+1 (mod 8)
        let mut g = Graph::new();
        g.add_node(make_node(0, 8, vec![]));
        let n = 8usize;
        let map: Vec<Vec<usize>> = (0..n)
            .map(|j| vec![(j + n - 1) % n, j, (j + 1) % n])
            .collect();
        g.add_node(make_node(1, 8, vec![dep_arg(0, map)]));
        let tables = validate_and_build(&g).unwrap();
        let thresholds = tables.node_thresholds[1].as_ref().unwrap();
        assert!(thresholds.iter().all(|&t| t == 3));
        let routing = tables.pred_instance_map[1][0].as_ref().unwrap();
        // producer 0 feeds consumers 7, 0, 1
        let mut feeds = routing.producer_to_consumers[0].clone();
        feeds.sort_unstable();
        assert_eq!(feeds, vec![0, 1, 7]);
    }

    #[test]
    fn union_across_two_mapped_args_on_same_pred_counts_once() {
        let mut g = Graph::new();
        g.add_node(make_node(0, 4, vec![]));
        g.add_node(make_node(
            1,
            1,
            vec![
                dep_arg(0, vec![vec![0, 1]]),
                dep_arg(0, vec![vec![1, 2]]), // overlap on producer 1
            ],
        ));
        let tables = validate_and_build(&g).unwrap();
        // union {0,1,2} -> threshold 3, not 2+2=4
        assert_eq!(
            *tables.node_thresholds[1].as_ref().unwrap().as_ref(),
            vec![3]
        );
        let routing = tables.pred_instance_map[1][0].as_ref().unwrap();
        assert_eq!(routing.producer_to_consumers[1], vec![0]); // listed once, not twice
    }

    #[test]
    fn classify_coexisting_predecessor_kind_matches_shape() {
        // 1:1 shape: factor 4, pred factor 4, single index.
        let mut g1 = Graph::new();
        g1.add_node(make_node(0, 4, vec![]));
        let node1 = make_node(1, 4, vec![res_arg(0, vec![0])]);
        let c1 = classify_coexisting_predecessor(&g1, &node1, 0).unwrap();
        assert_eq!(c1.kind, UniformKind::OneToOne);
        assert_eq!(c1.amount, 1);

        // Broadcast shape: full fan-in barrier, pred factor 6, any succ factor.
        let mut g2 = Graph::new();
        g2.add_node(make_node(0, 6, vec![]));
        let node2 = make_node(1, 1, vec![barrier_arg(0, (0..6).collect())]);
        let c2 = classify_coexisting_predecessor(&g2, &node2, 0).unwrap();
        assert_eq!(c2.kind, UniformKind::Broadcast);
        assert_eq!(c2.amount, 6);
    }

    #[test]
    fn mapped_consumer_coexists_with_ordinary_1to1_edge() {
        // node 2: factor 4, instance_map on pred 0 (factor 4, 1:1-like via map)
        // PLUS an ordinary 1:1 $res edge on pred 1 (factor 4).
        let mut g = Graph::new();
        g.add_node(make_node(0, 4, vec![])); // mapped pred
        g.add_node(make_node(1, 4, vec![])); // plain 1:1 pred
        let map: Vec<Vec<usize>> = (0..4).map(|j| vec![j]).collect();
        g.add_node(make_node(2, 4, vec![dep_arg(0, map), res_arg(1, vec![0])]));
        let tables = validate_and_build(&g).unwrap();
        let thresholds = tables.node_thresholds[2].as_ref().unwrap();
        // 1 (mapped) + 1 (1:1 uniform) = 2 per instance
        assert!(thresholds.iter().all(|&t| t == 2));
    }

    #[test]
    fn zero_threshold_instance_is_accepted_and_recorded() {
        let mut g = Graph::new();
        g.add_node(make_node(0, 4, vec![]));
        g.add_node(make_node(
            1,
            2,
            vec![dep_arg(0, vec![vec![0, 1], vec![]])], // instance 1 has empty map
        ));
        let tables = validate_and_build(&g).unwrap();
        let thresholds = tables.node_thresholds[1].as_ref().unwrap();
        assert_eq!(thresholds.as_slice(), &[2, 0]);
        let zero = tables.zero_threshold_instances[1]
            .as_ref()
            .expect("node 1 must have a recorded zero-threshold instance list");
        assert_eq!(zero.as_slice(), &[1]);
        // Node 0 (the plain producer, no instance_map predecessor) has none.
        assert!(tables.zero_threshold_instances[0].is_none());
    }

    #[test]
    fn every_instance_nonzero_leaves_zero_threshold_instances_none() {
        let mut g = Graph::new();
        g.add_node(make_node(0, 4, vec![]));
        g.add_node(make_node(
            1,
            2,
            vec![dep_arg(0, vec![vec![0, 1], vec![2, 3]])],
        ));
        let tables = validate_and_build(&g).unwrap();
        assert!(tables.zero_threshold_instances[1].is_none());
    }

    #[test]
    fn multiple_zero_threshold_instances_on_the_same_node_are_all_recorded() {
        let mut g = Graph::new();
        g.add_node(make_node(0, 4, vec![]));
        g.add_node(make_node(
            1,
            4,
            vec![dep_arg(0, vec![vec![0], vec![], vec![1], vec![]])],
        ));
        let tables = validate_and_build(&g).unwrap();
        let zero = tables.zero_threshold_instances[1].as_ref().unwrap();
        assert_eq!(zero.as_slice(), &[1, 3]);
    }

    #[test]
    fn rejects_duplicate_index_within_one_list() {
        // caught earlier in graph_gen::parse_predecessor for JSON input; here we
        // exercise the producer-factor bound check instead (duplicates within a
        // single list are deduped harmlessly by the union step if they ever slip
        // through a non-JSON builder, so the authoritative duplicate check lives
        // at parse time — this test documents that choice).
        let mut g = Graph::new();
        g.add_node(make_node(0, 2, vec![]));
        g.add_node(make_node(1, 1, vec![dep_arg(0, vec![vec![5]])])); // out of range
        let err = validate_and_build(&g).unwrap_err();
        assert!(err.contains("out of range"), "{err}");
    }

    #[test]
    fn rejects_res_arg_with_instance_map() {
        let mut g = Graph::new();
        g.add_node(make_node(0, 4, vec![]));
        let bad_res = Arg {
            type_: CmTypes::Res(0),
            predecessor: Some(Predecessor {
                id: 0,
                indexes: Vec::new(),
                group_by: None,
                instance_map: Some(vec![vec![0]]),
            }),
            init_condition: None,
        };
        g.add_node(make_node(1, 1, vec![bad_res]));
        let err = validate_and_build(&g).unwrap_err();
        assert!(err.contains("$res"), "{err}");
    }

    #[test]
    fn rejects_group_size_on_mapped_node() {
        let mut g = Graph::new();
        g.add_node(make_node(0, 4, vec![]));
        let mut node = make_node(
            1,
            4,
            vec![dep_arg(0, vec![vec![0], vec![1], vec![2], vec![3]])],
        );
        node.group_size = Some(2);
        g.add_node(node);
        let err = validate_and_build(&g).unwrap_err();
        assert!(err.contains("group_size"), "{err}");
    }

    #[test]
    fn rejects_condition_on_mapped_node() {
        let mut g = Graph::new();
        g.add_node(make_node(0, 4, vec![]));
        let mut node = make_node(
            1,
            4,
            vec![dep_arg(0, vec![vec![0], vec![1], vec![2], vec![3]])],
        );
        node.condition = Some(NodeCondition {
            operation: crate::graph_struct::CondOp::Eq,
            eval_value: CmTypes::Bool(true),
            func_name: "f".to_string(),
            args: vec![],
        });
        g.add_node(node);
        let err = validate_and_build(&g).unwrap_err();
        assert!(err.contains("condition"), "{err}");
    }

    #[test]
    fn rejects_mixing_mapped_and_plain_on_same_pred() {
        let mut g = Graph::new();
        g.add_node(make_node(0, 4, vec![]));
        g.add_node(make_node(
            1,
            1,
            vec![dep_arg(0, vec![vec![0, 1]]), barrier_arg(0, vec![2, 3])],
        ));
        let err = validate_and_build(&g).unwrap_err();
        assert!(err.contains("consistently"), "{err}");
    }

    #[test]
    fn rejects_ordinal_fallback_coexisting_shape() {
        // pred 1: factor 3, no filter, no barrier, no 1:1 (succ factor 1 != 3,
        // and not a single index either) -> unsupported coexisting shape.
        let mut g = Graph::new();
        g.add_node(make_node(0, 4, vec![])); // mapped pred
        g.add_node(make_node(1, 3, vec![])); // problematic plain pred
        g.add_node(make_node(
            2,
            1,
            vec![
                dep_arg(0, vec![vec![0, 1, 2, 3]]),
                res_arg(1, vec![0, 1, 2]), // 3 indexes, succ_factor=1, not filtered (full range)
            ],
        ));
        let err = validate_and_build(&g).unwrap_err();
        assert!(
            err.contains("no uniform per-instance interpretation"),
            "{err}"
        );
    }

    #[test]
    fn accepts_broadcast_barrier_coexisting_shape() {
        let mut g = Graph::new();
        g.add_node(make_node(0, 4, vec![])); // mapped pred
        g.add_node(make_node(1, 6, vec![])); // barrier pred, full fan-in
        g.add_node(make_node(
            2,
            1,
            vec![
                dep_arg(0, vec![vec![0, 1, 2, 3]]),
                barrier_arg(1, (0..6).collect()),
            ],
        ));
        let tables = validate_and_build(&g).unwrap();
        // 4 (mapped) + 6 (barrier broadcast) = 10
        assert_eq!(
            *tables.node_thresholds[2].as_ref().unwrap().as_ref(),
            vec![10]
        );
    }

    #[test]
    fn not_mapped_nodes_are_untouched() {
        let mut g = Graph::new();
        g.add_node(make_node(0, 4, vec![]));
        g.add_node(make_node(1, 4, vec![res_arg(0, vec![0])]));
        let tables = validate_and_build(&g).unwrap();
        assert!(tables.node_thresholds[0].is_none());
        assert!(tables.node_thresholds[1].is_none());
        assert!(!tables.has_instance_map[1]);
    }

    // -----------------------------------------------------------------------
    // Property-style cross-check: random small single-pred maps → the
    // computed per-instance threshold must equal a brute-force count of the
    // decrements the reverse routing table will actually send, and the
    // reverse table must be the exact inverse of the (unioned) forward map.
    // No external crate: a tiny xorshift stands in for `proptest`.
    // -----------------------------------------------------------------------

    /// Deterministic xorshift64 — enough spread for small bounded ranges,
    /// fully reproducible across runs (no external randomness dependency).
    struct Xorshift64(u64);
    impl Xorshift64 {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        fn range(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    #[test]
    fn property_random_single_pred_maps_threshold_and_routing_agree() {
        let mut rng = Xorshift64(0x9E3779B97F4A7C15);
        for trial in 0..200u32 {
            let pred_factor = 1 + rng.range(10);
            let succ_factor = 1 + rng.range(10);

            // Random forward map: each consumer instance gets a random subset
            // (as a dedup'd Vec) of producer indexes, size 0..=pred_factor.
            let forward: Vec<Vec<usize>> = (0..succ_factor)
                .map(|_| {
                    let mut seen = HashSet::new();
                    let picks = rng.range(pred_factor + 1);
                    for _ in 0..picks {
                        seen.insert(rng.range(pred_factor));
                    }
                    let mut v: Vec<usize> = seen.into_iter().collect();
                    v.sort_unstable();
                    v
                })
                .collect();

            let mut g = Graph::new();
            g.add_node(make_node(0, pred_factor, vec![]));
            g.add_node(make_node(1, succ_factor, vec![dep_arg(0, forward.clone())]));

            // An empty map entry gives that instance a zero threshold — accepted
            // (see `zero_threshold_instance_is_accepted_and_recorded`), not
            // rejected. Cross-check `zero_threshold_instances` against the
            // brute-force empty-list set below instead of skipping the trial.
            let expected_zero: Vec<u32> = forward
                .iter()
                .enumerate()
                .filter(|(_, l)| l.is_empty())
                .map(|(j, _)| j as u32)
                .collect();

            let tables = validate_and_build(&g)
                .unwrap_or_else(|e| panic!("trial {trial}: unexpected rejection: {e}"));
            let thresholds = tables.node_thresholds[1].as_ref().unwrap();
            let routing = tables.pred_instance_map[1][0].as_ref().unwrap();

            if expected_zero.is_empty() {
                assert!(
                    tables.zero_threshold_instances[1].is_none(),
                    "trial {trial}: no empty map entries, but zero_threshold_instances[1] is \
                     Some — forward={forward:?}"
                );
            } else {
                let got = tables.zero_threshold_instances[1]
                    .as_ref()
                    .unwrap_or_else(|| {
                        panic!(
                            "trial {trial}: expected zero_threshold_instances[1] = \
                             {expected_zero:?}, got None — forward={forward:?}"
                        )
                    });
                assert_eq!(
                    got.as_slice(),
                    expected_zero.as_slice(),
                    "trial {trial}: zero_threshold_instances[1] mismatch, forward={forward:?}"
                );
            }

            // Threshold[j] must equal len(forward[j]) exactly (single mapped
            // predecessor, no coexisting edges — union is a no-op here).
            for (j, list) in forward.iter().enumerate() {
                assert_eq!(
                    thresholds[j] as usize,
                    list.len(),
                    "trial {trial}: threshold[{j}] mismatch, forward={forward:?}"
                );
            }

            // Brute-force the number of decrements EVERY instance will
            // receive by replaying the reverse table against every producer
            // index, and compare against the forward map directly (the
            // reverse table must be its exact inverse).
            let mut brute_force_received: Vec<usize> = vec![0; succ_factor];
            for i in 0..pred_factor {
                for &j in &routing.producer_to_consumers[i] {
                    brute_force_received[j as usize] += 1;
                }
            }
            for (j, list) in forward.iter().enumerate() {
                assert_eq!(
                    brute_force_received[j],
                    list.len(),
                    "trial {trial}: replaying the reverse routing table for consumer {j} \
                     yields {} decrements, expected {} (forward={forward:?})",
                    brute_force_received[j],
                    list.len()
                );
            }

            // And the reverse table must list EXACTLY the producers forward[j]
            // declares — not a superset/subset.
            for i in 0..pred_factor {
                for &j in &routing.producer_to_consumers[i] {
                    assert!(
                        forward[j as usize].contains(&i),
                        "trial {trial}: reverse table lists producer {i} for consumer {j}, \
                         but forward[{j}] = {:?} does not contain it",
                        forward[j as usize]
                    );
                }
            }
        }
    }
}
