//! Flattened successor-edge arena — the Phase-3 hot-loop data layout.
//!
//! # Motivation
//!
//! Phase 3 of the batch protocol (successor collection + dependency decrement) is the
//! hottest loop in the runtime: it runs once per completed node instance, on both the
//! resolution-thread path (`batch_resolution::process_batch_inner`) and the worker
//! fast path (`task_execution::worker_resolve_successors`).  Before the arena, every
//! edge visit chased 3–4 independent heap structures:
//!
//! ```text
//! graph.successors[pred]            → Vec<IdType>
//! pred_index_filter[succ][pred]     → Arc<Vec<Vec<Option<(usize, usize)>>>>   (×2: filter + range_start)
//! pred_group_by[succ][pred]         → Arc<Vec<Vec<Option<usize>>>>
//! pred_succ_1to1_offset[succ][pred] → Arc<Vec<Vec<Option<isize>>>>
//! node_cache[succ]                  → factor / is_condition / is_fanout_bulk
//! ```
//!
//! The arena joins all of these at graph-compile time into one contiguous
//! `Vec<SuccEdge>` per predecessor, so the hot loop iterates a single cache-friendly
//! slice with zero indirect lookups.  The N×N tables are retained in `GraphCache`
//! only for the cold argument-resolution reads in `arg_resolution.rs`.
//!
//! # Invariants
//!
//! - The arena is built once in `GraphSpec::compile` and never mutated — Tomii graph
//!   topology is immutable after build (see `ARCHITECTURE.md`, "graph IR").
//! - `edges_for(pred)` returns edges in the same order as `graph.successors[pred]`,
//!   preserving the dispatch order of the pre-arena implementation.
//! - Every field of [`SuccEdge`] is a pure function of the graph topology; no
//!   per-slot or per-frame state lives here.

use super::node_cache::NodeCacheEntry;
use crate::IdType;

/// `filter_end` sentinel: no predecessor-index filter on this edge.
const NO_FILTER: u32 = u32::MAX;
/// `group_by` sentinel: no grouping — decrement the successor's global counter.
const NO_GROUP: u32 = 0;

/// One precomputed predecessor→successor edge.
///
/// A `SuccEdge` answers, without any further memory lookups, the three questions the
/// Phase-3 loop asks about a completed predecessor instance:
///
/// 1. *Does this instance drive the successor at all?* — [`SuccEdge::passes_filter`]
/// 2. *Which dependency-counter group does it decrement?* — [`SuccEdge::pred_group`]
/// 3. *Does it map 1:1 onto a specific successor instance?* — [`SuccEdge::one_to_one_succ_idx`]
///
/// Fields are kept compact (28 bytes, two edges per cache line); the semantics live
/// in the accessor methods so call sites stay self-describing.
#[derive(Clone, Copy, Debug)]
pub struct SuccEdge {
    /// Successor node ID.
    pub succ_id: IdType,
    /// True when the successor carries a condition (legacy arg conditions or
    /// node-level `condition:`).  Condition successors must be evaluated on a
    /// resolution thread; `worker_resolvable` nodes never see these.
    pub has_condition: bool,
    /// True when the successor is eligible for 1:1 fanout-bulk dispatch (Upgrade 5).
    /// Mirrors `NodeCacheEntry::is_fanout_bulk` to avoid a node-cache chase.
    pub is_fanout_bulk: bool,
    /// Successor's parallel factor. Mirrors `NodeCacheEntry::factor`.
    succ_factor: u32,
    /// Predecessor-instance index range `[filter_start, filter_end)` that drives this
    /// successor.  `filter_end == NO_FILTER` means every instance drives it.
    /// `filter_start` doubles as the base for relative-index group computation.
    filter_start: u32,
    filter_end: u32,
    /// `group_by` divisor for grouped barriers; `NO_GROUP` (0) means no grouping.
    group_by: u32,
    /// Offset (`predecessor.indexes[0]`) for 1:1 equal-factor non-barrier `$res`
    /// edges.  Only meaningful when `has_one_to_one` is true.
    one_to_one_offset: i32,
    /// True when the 1:1 instance mapping applies to this edge.
    has_one_to_one: bool,
}

impl SuccEdge {
    /// Successor's parallel factor.
    #[inline]
    pub fn succ_factor(&self) -> usize {
        self.succ_factor as usize
    }

    /// Does predecessor instance `pred_index` drive this successor?
    ///
    /// Mirrors the `pred_index_filter` range check: an edge with a declared index
    /// range only fires for predecessor instances inside `[start, end)`.
    #[inline]
    pub fn passes_filter(&self, pred_index: usize) -> bool {
        self.filter_end == NO_FILTER
            || (pred_index >= self.filter_start as usize && pred_index < self.filter_end as usize)
    }

    /// Dependency-counter group decremented by predecessor instance `pred_index`.
    ///
    /// `Some(group)` for `group_by` barriers — the group is the predecessor's index
    /// *relative to the edge's filter range start*, divided by the `group_by` divisor.
    /// `None` means the successor's global counter is decremented.
    #[inline]
    pub fn pred_group(&self, pred_index: usize) -> Option<usize> {
        if self.group_by == NO_GROUP {
            None
        } else {
            let relative_idx = pred_index - self.filter_start as usize;
            Some(relative_idx / self.group_by as usize)
        }
    }

    /// The specific successor instance fired by predecessor instance `pred_index`
    /// for 1:1 non-barrier equal-factor `$res` edges.
    ///
    /// When `Some(j)`, predecessor instance `i` fires exactly successor instance
    /// `j = (i - offset).rem_euclid(factor)` — guaranteeing the predecessor's result
    /// is already stored when the successor resolves its `$res` argument (no
    /// `spin_wait` needed).  `None` for fanout / barrier / grouped dependencies.
    #[inline]
    pub fn one_to_one_succ_idx(&self, pred_index: usize) -> Option<usize> {
        if self.has_one_to_one {
            let f = self.succ_factor as isize;
            Some((pred_index as isize - self.one_to_one_offset as isize).rem_euclid(f) as usize)
        } else {
            None
        }
    }
}

/// Contiguous successor-edge storage for the whole graph.
///
/// `edges` holds every predecessor→successor edge, grouped by predecessor;
/// `ranges[pred]` is the `[start, end)` window into `edges` for that predecessor.
/// Built once by [`SuccessorArena::build`]; read-only thereafter.
pub struct SuccessorArena {
    edges: Vec<SuccEdge>,
    ranges: Vec<(u32, u32)>,
}

impl SuccessorArena {
    /// All outgoing edges of `pred_node_id`, in graph-declared successor order.
    ///
    /// Returns an empty slice for nodes with no successors (including node IDs
    /// beyond the successor table, matching the pre-arena bounds behaviour).
    #[inline]
    pub fn edges_for(&self, pred_node_id: usize) -> &[SuccEdge] {
        match self.ranges.get(pred_node_id) {
            Some(&(start, end)) => &self.edges[start as usize..end as usize],
            None => &[],
        }
    }

    /// Join the successor lists with the predecessor routing tables and node cache
    /// into the flattened arena.  Pure function of the compiled topology.
    ///
    /// `successors[pred]` lists successor node IDs (as built by `Graph::add_node`);
    /// the three tables are the output of `init::build_predecessor_tables` and are
    /// indexed `[succ][pred]`.
    pub(crate) fn build(
        successors: &[Vec<IdType>],
        num_nodes: usize,
        node_cache: &[NodeCacheEntry],
        pred_index_filter: &[Vec<Option<(usize, usize)>>],
        pred_group_by: &[Vec<Option<usize>>],
        pred_succ_1to1_offset: &[Vec<Option<isize>>],
    ) -> Self {
        let total_edges: usize = successors.iter().map(|s| s.len()).sum();
        let mut edges = Vec::with_capacity(total_edges);
        let mut ranges = Vec::with_capacity(num_nodes);

        for pred_id in 0..num_nodes {
            let start = edges.len() as u32;
            if let Some(succ_list) = successors.get(pred_id) {
                for &succ_id in succ_list {
                    let succ = succ_id as usize;
                    let entry = &node_cache[succ];

                    let (filter_start, filter_end) = match pred_index_filter[succ][pred_id] {
                        Some((s, e)) => (s as u32, e as u32),
                        None => (0, NO_FILTER),
                    };
                    let group_by = match pred_group_by[succ][pred_id] {
                        Some(gb) => {
                            debug_assert!(gb > 0, "group_by divisor must be >= 1");
                            gb as u32
                        }
                        None => NO_GROUP,
                    };
                    let (has_one_to_one, one_to_one_offset) =
                        match pred_succ_1to1_offset[succ][pred_id] {
                            Some(k) => (true, k as i32),
                            None => (false, 0),
                        };

                    edges.push(SuccEdge {
                        succ_id,
                        has_condition: entry.is_condition,
                        is_fanout_bulk: entry.is_fanout_bulk,
                        succ_factor: entry.factor as u32,
                        filter_start,
                        filter_end,
                        group_by,
                        one_to_one_offset,
                        has_one_to_one,
                    });
                }
            }
            ranges.push((start, edges.len() as u32));
        }

        SuccessorArena { edges, ranges }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::node_cache::ArgCacheEntry;

    /// Minimal NodeCacheEntry for arena-construction tests.
    fn test_entry(factor: usize, is_condition: bool, is_fanout_bulk: bool) -> NodeCacheEntry {
        fn noop(_: &[tomii_types::CmTypes]) -> tomii_types::CmTypes {
            tomii_types::CmTypes::None
        }
        NodeCacheEntry {
            factor,
            pred_vec: Vec::new(),
            name: String::new(),
            func_ptr: noop,
            arg_cache: ArgCacheEntry::default(),
            is_initial: false,
            is_condition,
            cond_index: 0,
            successor_count: 0,
            node_condition: None,
            priority: crate::custom_scheduler::Priority::Normal,
            affinity_group: 0,
            worker_resolvable: false,
            needs_result_store: false,
            bulk_func: None,
            is_fanout_bulk,
            template_stable: false,
            func_name: String::new(),
            uses_unchecked: false,
        }
    }

    /// 3-node graph: 0 → 1 (filtered, grouped), 0 → 2 (1:1), 1 → 2 (plain).
    fn build_test_arena() -> SuccessorArena {
        let n = 3;
        let successors: Vec<Vec<IdType>> = vec![vec![1, 2], vec![2], vec![]];
        let node_cache = vec![
            test_entry(8, false, false),
            test_entry(4, true, false),
            test_entry(8, false, true),
        ];
        let mut filter = vec![vec![None; n]; n];
        let mut group_by = vec![vec![None; n]; n];
        let mut one_to_one = vec![vec![None; n]; n];
        filter[1][0] = Some((2, 6)); // succ 1 reads pred-0 instances [2, 6)
        group_by[1][0] = Some(2); //    ...grouped in pairs
        one_to_one[2][0] = Some(1); //  succ 2 is 1:1 from pred 0 with offset 1

        SuccessorArena::build(&successors, n, &node_cache, &filter, &group_by, &one_to_one)
    }

    #[test]
    fn edges_preserve_successor_order_and_counts() {
        let arena = build_test_arena();
        let e0: Vec<IdType> = arena.edges_for(0).iter().map(|e| e.succ_id).collect();
        assert_eq!(e0, vec![1, 2]);
        assert_eq!(arena.edges_for(1).len(), 1);
        assert!(arena.edges_for(2).is_empty());
        // Out-of-range node IDs return empty (pre-arena bounds behaviour).
        assert!(arena.edges_for(99).is_empty());
    }

    #[test]
    fn filter_range_semantics() {
        let arena = build_test_arena();
        let edge = &arena.edges_for(0)[0]; // 0 → 1, filter [2, 6)
        assert!(!edge.passes_filter(1));
        assert!(edge.passes_filter(2));
        assert!(edge.passes_filter(5));
        assert!(!edge.passes_filter(6));
        // Unfiltered edge accepts every index.
        let plain = &arena.edges_for(1)[0]; // 1 → 2
        assert!(plain.passes_filter(0));
        assert!(plain.passes_filter(1_000_000));
    }

    #[test]
    fn group_is_relative_to_filter_start() {
        let arena = build_test_arena();
        let edge = &arena.edges_for(0)[0]; // filter starts at 2, group_by 2
        assert_eq!(edge.pred_group(2), Some(0)); // (2-2)/2
        assert_eq!(edge.pred_group(3), Some(0)); // (3-2)/2
        assert_eq!(edge.pred_group(4), Some(1)); // (4-2)/2
        assert_eq!(edge.pred_group(5), Some(1));
        // Ungrouped edge decrements the global counter.
        let plain = &arena.edges_for(1)[0];
        assert_eq!(plain.pred_group(3), None);
    }

    #[test]
    fn one_to_one_matches_rem_euclid_formula() {
        let arena = build_test_arena();
        let edge = &arena.edges_for(0)[1]; // 0 → 2, offset 1, factor 8
        for i in 0..8 {
            assert_eq!(
                edge.one_to_one_succ_idx(i),
                Some((i as isize - 1).rem_euclid(8) as usize)
            );
        }
        // Non-1:1 edge yields None.
        let plain = &arena.edges_for(1)[0];
        assert_eq!(plain.one_to_one_succ_idx(0), None);
    }

    #[test]
    fn cached_node_flags_mirror_node_cache() {
        let arena = build_test_arena();
        let to_cond = &arena.edges_for(0)[0]; // succ 1 is a condition node
        assert!(to_cond.has_condition);
        assert!(!to_cond.is_fanout_bulk);
        assert_eq!(to_cond.succ_factor(), 4);
        let to_bulk = &arena.edges_for(0)[1]; // succ 2 is fanout-bulk
        assert!(!to_bulk.has_condition);
        assert!(to_bulk.is_fanout_bulk);
        assert_eq!(to_bulk.succ_factor(), 8);
    }
}
