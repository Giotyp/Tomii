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
//! - `$barrier`, `$dep`, and `$res` arguments may all carry an `instance_map`.
//!   A mapped `$res` arg delivers a value: for consumer instance `j`, the
//!   producer results listed in the (unioned, ascending) forward list are
//!   fetched in that order — exactly one value when the list has length 1,
//!   or a `Vec` spliced in place otherwise (see
//!   [`MappedEdgeRouting::forward`] and
//!   `runtime::arg_resolution::fetch_mapped_res_results`). This mirrors the
//!   existing ordinary-`$res` convention exactly: a single declared index
//!   delivers one value, a list of more than one delivers a `Vec` intended
//!   for a variadic receiving parameter. [`validate_and_build`] itself never
//!   rejects a non-uniform-length `instance_map` (including an empty list for
//!   some instance) — building the routing table is independent of the
//!   receiving kernel's arity. The arity check against the real Rust/Python
//!   function is a SEPARATE pass, [`validate_res_arity`] (see "Arity
//!   validation" below): it runs when the receiving kernel's variadic-ness is
//!   known (the plugin published per-argument metadata) and rejects a scalar
//!   parameter that would receive anything other than exactly one value for
//!   any instance; it is a no-op (skipped) when that metadata is absent, in
//!   which case the runtime's `$res`-splice guard (see below) is the
//!   remaining enforcement. An out-of-range producer
//!   index or a duplicate within one `instance_map[j]` entry is still
//!   rejected (or filtered at the point of entry) by the same checks that
//!   apply to `$barrier`/`$dep` — see [`validate_and_build`].
//! - A node with `group_size` set may not have an `instance_map` predecessor
//!   (rejected at validation) — the two partitioning schemes are not
//!   reconciled in this release.
//!
//! # Arity validation ($res value delivery vs. the receiving kernel's arity)
//!
//! A `$res` arg (mapped or ordinary) delivers a *value*, not just a gating
//! signal — and the number of values it delivers per consumer instance can
//! be 0, 1, or more (see [`MappedEdgeRouting::forward`] for the mapped case;
//! an ordinary multi-index `$res` edge delivers a uniform count across every
//! instance instead). The receiving kernel parameter is either scalar (one
//! `CmTypes` slot, read positionally) or the kernel's trailing
//! `#[tomii_export(variadic)]` `Vec<T>` parameter (any count, including 0).
//!
//! **The variadic tail may be fed by more than one `$res` argument.** A
//! kernel has at most one variadic (trailing `Vec<T>`) parameter, but the
//! GRAPH may wire multiple `$res` edges — mapped, ordinary, or a mix — at or
//! after that parameter's position; every one of them contributes its own
//! values into the SAME `Vec`, concatenated in declared arg order (each
//! edge's own values keep their documented per-instance order within that
//! — ascending absolute producer index for a mapped edge, declaration order
//! for an ordinary range). Dispatch places each edge's values into the
//! argument buffer back-to-front across the whole node (descending
//! declared-argument position — see
//! `runtime::arg_resolution::populate_cached_args_into`'s doc comment),
//! which is what makes several simultaneously variable-width arguments
//! resolve correctly: each buffer splice only ever shifts a position that
//! has ALREADY been written, never one still to be visited. A kernel that
//! needs to know where one edge's contribution ends and the next begins
//! cannot infer it from the `Vec` alone — pass a companion scalar argument
//! carrying each edge's width (see `website/docs/guide/plugins/rust.md`'s
//! "Multiple edges feeding the same variadic parameter" section).
//!
//! [`validate_res_arity`] checks every `$res` arg (mapped AND ordinary)
//! against that arity **when it is available**: both the static (`build.rs`,
//! `func_reg`) and dynamic-registration (`__tomii_exports`) plugin-loading
//! paths already publish, per exported kernel, a per-argument-slot spec
//! (`crate::func_reg::get_func_argspec`) whose *last* entry is the literal
//! `"..."` marker iff the kernel is variadic — the exact mechanism
//! `runtime::init::select_unchecked_wrappers` already consults. No new
//! metadata or plugin-ABI version bump was needed: the variadic BOUNDARY
//! (the number of fixed/scalar parameter slots before it —
//! [`variadic_fixed_boundary`]) is already fully determined by `spec.len()`
//! and `spec.last()`; every `$res` argument at or after that boundary may
//! vary (any count, including 0, independently of every other tail
//! argument on the same node); every argument before it must resolve to
//! exactly one value for EVERY consumer instance.
//!
//! This load-time rule and the dispatch-time guard
//! (`runtime::node_cache::ResPredCache::may_vary_width`, computed in
//! `runtime::init::build_node_cache` from this SAME boundary when a spec is
//! published) agree exactly by construction — a graph that loads
//! successfully can never panic at dispatch over an arity mismatch the
//! load-time check could have caught. A plugin that predates this check, a
//! Python-registered kernel, or any other kernel for which
//! `get_func_argspec` returns `None` skips the load-time check entirely
//! (backward compatible — such a plugin still loads) and falls back to the
//! runtime guard in `runtime::arg_resolution`'s `$res`-splice path
//! (`assert_res_width_is_sound`), which rejects (panics, O(1), on the
//! already-measured `result.len()`) any non-last-value-bearing `$res`
//! argument that resolves to anything other than exactly one value —
//! "non-last" being exactly the structural invariant the macro itself
//! enforces (only the kernel's LAST parameter may be variadic), so it is
//! sound without needing the plugin's metadata at all, though strictly more
//! conservative than the metadata-available case: without the kernel's
//! published arity there is no way to know whether a node's SECOND-to-last
//! `$res` argument also lies within the variadic tail, so only the single
//! structurally last one is ever permitted to vary.
//!
//! Python-registered kernels (`@tomii.export`, `tomii/_export.py`) fall into
//! the metadata-absent case unconditionally, by construction rather than by
//! omission: every Python kernel's graph-node `function` name is always one
//! of exactly two shared Rust bridge symbols, `"py_call_any"` (scalar sink)
//! or `"py_call_void"` (variadic sink — selected by the Python decorator's
//! own `variadic=True`, mirroring `#[tomii_export(variadic)]`); see
//! `tomii/_python_bridge/src/lib.rs`. Neither bridge function is itself
//! `#[tomii_export]`-annotated (they are hand-written `CmPtr`-shaped
//! functions that forward a raw `&[CmTypes]` slice into the wrapped Python
//! callable), so neither publishes a per-argument-slot spec, and
//! `get_func_argspec` correctly returns `None` for both — a per-Python-
//! kernel spec wouldn't be meaningful here anyway, since one bridge symbol
//! stands in for every Python function sharing that variadic-ness. The
//! runtime guard remains the enforcement for Python kernels, and it needs no
//! adjustment to cover them: it is driven entirely by the GRAPH's declared
//! arg shapes (`crate::graph_struct`), independent of which language the
//! kernel is implemented in.
//! - A consumer instance whose TOTAL threshold (summed across every
//!   predecessor, mapped or not) is zero — an empty `instance_map[j]` entry
//!   on every mapped predecessor, with no coexisting predecessor
//!   contributing either — is dispatched exactly once per frame at frame
//!   admission, like a root/initial node, instead of through the
//!   dependency-counter path (nothing would ever decrement it: no routing
//!   table targets an instance whose forward map is empty). See
//!   [`validate_and_build`]'s `zero_threshold_instances` output and
//!   `runtime::slot_management::initial_nodes`.
//! - A non-mapped predecessor may coexist on the same node as a mapped one,
//!   either on a DIFFERENT predecessor or on the SAME predecessor a mapped
//!   argument also reads — but only in shapes with a uniform or exactly
//!   positional per-instance contribution: a literal/filtered `$barrier` or
//!   `$dep` (broadcast — every instance waits for the same N arrivals), a
//!   producer with factor 1 (broadcast of a single arrival), or an exact 1:1
//!   same-factor single-offset / contiguous-range edge. Any other shape (the
//!   "ordinal" fallback used for irregular fan-in/fan-out without a declared
//!   1:1/filter/barrier) is rejected with a clear error, as is a `group_by`
//!   plain argument coexisting with a mapped one (the two partitioning
//!   schemes are not reconciled) — see `classify_coexisting_predecessor` /
//!   `coexisting_predecessor_shape`.
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
//! - A non-mapped (coexisting) predecessor on a DIFFERENT predecessor id
//!   contributes the same, uniform amount to every instance — see
//!   `classify_coexisting_predecessor`.
//! - A non-mapped argument coexisting on the SAME predecessor id as a mapped
//!   argument has its per-instance absolute index set (see
//!   `coexisting_predecessor_per_instance`) unioned directly into that
//!   predecessor's mapped forward map before routing is built, exactly as if
//!   it had been declared as another mapped argument's list for the same
//!   instance. A producer instance reachable through both the mapped
//!   argument and the plain argument is therefore still counted, and
//!   decremented, exactly once (the same union convention, extended to mix
//!   declared and derived lists). This predecessor is then routed ENTIRELY
//!   through the per-instance mechanism — it is excluded from the ordinary
//!   `pred_index_filter`/`pred_group_by`/`pred_succ_1to1_offset` tables (see
//!   `runtime::init::build_predecessor_tables`) so no instance is ever
//!   decremented through two independent paths.
//!
//! # Routing
//!
//! For each mapped `(pred, succ)` edge, [`MappedEdgeRouting::build`]
//! pre-inverts the per-consumer forward map into a per-producer reverse map:
//! `producer_to_consumers[i]` lists every consumer instance `j` such that `i`
//! is in the (unioned) `instance_map[j]`. On completion of producer instance
//! `i`, the runtime decrements exactly those listed consumer instances, once
//! per listing — see `runtime::successor::decrement_and_collect_ready_mapped`.
//! The SAME union is also retained, forward, as [`MappedEdgeRouting::forward`]
//! — `forward[j]` lists, in ascending absolute-producer-index order, exactly
//! the producer instances consumer instance `j` depends on through this edge.
//! A mapped `$res` argument on this edge reads its values from `forward[j]`,
//! in that order, at dispatch time (`runtime::node_cache::ResPredCache::mapped_forward`,
//! `runtime::arg_resolution::fetch_mapped_res_results`) — the gating/threshold
//! and value-delivery mechanisms share one authoritative per-(succ, pred) list,
//! so a `$res` arg sharing a predecessor with another mapped or coexisting
//! plain argument (see above) reads the full unioned list, not a sub-list of
//! only its own declared entries.

use std::collections::HashSet;
use std::sync::Arc;

use crate::graph::Graph;
use crate::graph_struct::{find_pred_index, Arg, Node, Predecessor};
use crate::IdType;
use tomii_types::CmTypes;

/// Reverse producer→consumer routing table for one mapped `(pred, succ)` edge,
/// plus the forward list it was inverted from (retained for `$res` value
/// delivery — see the module docs' "Routing" section).
///
/// `producer_to_consumers[i]` lists every consumer instance that depends on
/// producer instance `i` through this edge (the union of every mapped
/// argument on this predecessor, inverted). Built once at graph-compile time;
/// shared (via `Arc`) by every slot.
#[derive(Debug)]
pub struct MappedEdgeRouting {
    pub producer_to_consumers: Vec<Vec<u32>>,
    /// `forward[j]` = the (unioned, ascending) absolute producer instances
    /// consumer instance `j` depends on through this edge — the exact input
    /// `MappedEdgeRouting::build` inverted. `producer_to_consumers` answers
    /// "who do I notify when producer `i` completes?"; `forward` answers "whose
    /// results does consumer `j` read?" — both views of the same union, kept
    /// side by side so neither the gating path nor the value-delivery path
    /// needs to re-derive the other.
    pub forward: Vec<Vec<u32>>,
}

impl MappedEdgeRouting {
    /// Build the reverse routing table from the (already unioned) forward map,
    /// retaining the forward map itself (see `forward`'s doc comment). `pred_factor`
    /// sizes the reverse array; `forward[j]` entries are assumed already validated
    /// to be `< pred_factor` with no duplicates, and already sorted ascending by
    /// every caller (`union_instance_maps` / the Item 2 same-predecessor merge) —
    /// `$res` value delivery relies on that ascending order.
    fn build(forward: &[Vec<usize>], pred_factor: usize) -> Self {
        let mut producer_to_consumers: Vec<Vec<u32>> = vec![Vec::new(); pred_factor];
        for (j, list) in forward.iter().enumerate() {
            for &i in list {
                producer_to_consumers[i].push(j as u32);
            }
        }
        Self {
            producer_to_consumers,
            forward: forward
                .iter()
                .map(|list| list.iter().map(|&i| i as u32).collect())
                .collect(),
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

/// Fully-resolved shape of a non-mapped predecessor coexisting, on the same
/// node, with at least one `instance_map` predecessor — either on a
/// different predecessor id (the original Phase 1 "coexisting predecessor"
/// case) or on the SAME predecessor id a mapped argument also reads (Item 2:
/// mixed mapped + plain args on one predecessor). Carries enough information
/// to derive either a uniform per-instance contribution
/// ([`classify_coexisting_predecessor`]) or the exact per-instance absolute
/// index list ([`coexisting_predecessor_per_instance`]).
#[derive(Debug, Clone)]
pub(crate) enum CoexistingShape {
    /// Each producer instance `i` fires exactly consumer instance `j = i - k`
    /// (mod factor) — one decrement, to one specific instance, per arrival.
    /// `offset` is `k` for the relative single-index shape, or the first
    /// (smallest) absolute index of a declared contiguous range — both
    /// resolve identically via `find_pred_index(j, offset, pred_factor)`.
    OneToOne { offset: isize },
    /// Every arrival decrements EVERY consumer instance by one, and every
    /// consumer instance's forward map is exactly `members` — the producer is
    /// a literal/filtered barrier, a `$dep`, or a factor-1 fan-out source.
    Broadcast { members: Vec<usize> },
}

/// How a non-mapped predecessor coexisting with a mapped one contributes to
/// every instance's threshold — the uniform-amount view of [`CoexistingShape`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UniformKind {
    OneToOne,
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

/// Classify the shape of every non-mapped argument `node` has on `pred_id`,
/// or `Err(reason)` when the combined shape has no uniform/positional
/// per-instance interpretation (the "ordinal fallback" shape, or a `group_by`
/// argument — see module docs). Shared by both the uniform-contribution view
/// (`classify_coexisting_predecessor`, used when `pred_id` is NOT also
/// mapped) and the per-instance-list view (`coexisting_predecessor_per_instance`,
/// used when `pred_id` IS also mapped — Item 2).
pub(crate) fn coexisting_predecessor_shape(
    graph: &Graph,
    node: &Node,
    pred_id: IdType,
) -> Result<CoexistingShape, String> {
    let pred_factor = graph.nodes[pred_id as usize].factor;
    let succ_factor = node.factor;

    // Scope strictly to the NON-mapped args on `pred_id`. For a predecessor
    // referenced on a DIFFERENT node than any mapped one, this is every arg
    // on `pred_id` (unchanged from before Item 2). For the SAME predecessor
    // as a mapped arg (Item 2: mixed mapped + plain args on one
    // predecessor), a mapped arg's own `indexes`/`group_by` are always empty
    // (mutually exclusive with `instance_map` at load time) and its `$barrier`
    // type tag (if any) describes the MAPPED edge, not this classification —
    // including it here would corrupt both the ambiguity check (an empty
    // mapped `indexes` looks "different" from a real plain `indexes` list)
    // and `any_barrier` (a barrier-typed mapped arg would wrongly force
    // broadcast semantics onto an unrelated plain arg). The mapped arg's own
    // contribution is handled entirely by `union_instance_maps` instead.
    let plain_args_on_pred: Vec<&Arg> = node
        .args
        .iter()
        .filter(|a| {
            a.predecessor
                .as_ref()
                .is_some_and(|p| p.id == pred_id && p.instance_map.is_none())
        })
        .collect();

    // Ambiguous multi-arg check, scoped to the plain args only (mirrors
    // `pred_is_ambiguous_multi_arg`, which can't be reused directly here
    // since it inspects every arg on `pred_id`, including any mapped one).
    if plain_args_on_pred.len() >= 2 && !plain_args_on_pred.iter().any(|a| a.is_barrier()) {
        let first = plain_args_on_pred[0].predecessor.as_ref().unwrap();
        let ambiguous = plain_args_on_pred[1..].iter().any(|a| {
            let p = a.predecessor.as_ref().unwrap();
            p.indexes != first.indexes || p.group_by != first.group_by
        });
        if ambiguous {
            // Two or more genuinely different, barrier-free plain args on the
            // same predecessor (e.g. `$res "0"` and `$res "-1"`). Reconciling
            // this with `runtime::init::build_predecessor_tables`'s filter
            // computation for a mapped successor (which must force
            // `succ_factor=1` semantics — see below) would require threading
            // that override through the "genuinely differing args" branch
            // too; reject instead and keep this release's scope to the
            // well-defined single literal-barrier / single-$dep / exact-1:1
            // shapes.
            return Err(format!(
                "predecessor '{}' has two or more differing dependency arguments on node \
                 '{}' (ambiguous multi-arg) — not supported coexisting with an instance_map \
                 predecessor on the same node.",
                graph.nodes[pred_id as usize].name, node.name
            ));
        }
    }

    let args_on_pred = plain_args_on_pred;
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

    // Union restricted to the plain args' own declared indexes. A mapped
    // arg's `indexes` is always empty (see above), so this agrees with
    // `graph.union_pred_indexes(node, pred_id)` whenever a mapped arg is
    // also present — computed explicitly here (rather than reusing that
    // helper) to make the scoping invariant self-contained and independent
    // of that assumption holding elsewhere.
    let mut union_indexes: Vec<isize> = Vec::new();
    for arg in &args_on_pred {
        let idxs = &arg.predecessor.as_ref().unwrap().indexes;
        for &i in idxs {
            if !union_indexes.contains(&i) {
                union_indexes.push(i);
            }
        }
    }
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
        return Ok(CoexistingShape::OneToOne { offset: sorted[0] });
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
        let members: Vec<usize> = if filter_applies || pred_factor == 1 {
            // Literal absolute indexes (or the sole index of a factor-1
            // producer) — every instance's forward set is exactly this list.
            sorted.iter().map(|&i| i as usize).collect()
        } else {
            // An unfiltered literal barrier: every one of the predecessor's
            // `pred_factor` instances contributes, to every consumer instance.
            (0..pred_factor).collect()
        };
        return Ok(CoexistingShape::Broadcast { members });
    }

    Err(format!(
        "node '{}': predecessor '{}' cannot coexist with an instance_map predecessor on the \
         same node — its dependency shape (factor {} -> {}, no exact 1:1 offset, no barrier, \
         no filtered range) has no uniform per-instance interpretation. Use a $barrier, a \
         filtered range (e.g. `.out(a, b)`), or a 1:1 (same-factor, single-offset) edge instead.",
        node.name, graph.nodes[pred_id as usize].name, pred_factor, succ_factor
    ))
}

/// Classify a non-mapped predecessor coexisting, on `node`, with at least one
/// `instance_map` predecessor, on a DIFFERENT predecessor id. Returns the
/// uniform per-instance contribution, or `Err(reason)` — see
/// `coexisting_predecessor_shape`.
pub(crate) fn classify_coexisting_predecessor(
    graph: &Graph,
    node: &Node,
    pred_id: IdType,
) -> Result<UniformContribution, String> {
    match coexisting_predecessor_shape(graph, node, pred_id)? {
        CoexistingShape::OneToOne { .. } => Ok(UniformContribution {
            kind: UniformKind::OneToOne,
            amount: 1,
        }),
        CoexistingShape::Broadcast { members } => Ok(UniformContribution {
            kind: UniformKind::Broadcast,
            amount: members.len() as u32,
        }),
    }
}

/// Per-consumer-instance absolute producer index list contributed by every
/// non-mapped argument `node` has on `pred_id`, for `pred_id` SAME as a
/// mapped predecessor (Item 2: mixed mapped + plain args on one
/// predecessor). `result[j]` is unioned directly into that predecessor's
/// mapped forward map (see `validate_and_build`) before routing is built —
/// so a producer instance reachable through both the mapped argument and
/// this plain argument is still counted, and decremented, exactly once.
pub(crate) fn coexisting_predecessor_per_instance(
    graph: &Graph,
    node: &Node,
    pred_id: IdType,
) -> Result<Vec<Vec<usize>>, String> {
    let pred_factor = graph.nodes[pred_id as usize].factor;
    let succ_factor = node.factor;
    let shape = coexisting_predecessor_shape(graph, node, pred_id)?;
    Ok(match shape {
        CoexistingShape::OneToOne { offset } => (0..succ_factor)
            .map(|j| vec![find_pred_index(j, offset, pred_factor)])
            .collect(),
        CoexistingShape::Broadcast { members } => {
            (0..succ_factor).map(|_| members.clone()).collect()
        }
    })
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

            let mut union = union_instance_maps(node, pred_id, factor);

            // Item 2: mixed mapped + plain args on the SAME predecessor. A
            // plain (non-instance_map) argument on this SAME `pred_id` — a
            // 1:1/contiguous-range offset or a broadcast (literal barrier,
            // $dep, or factor-1 producer) — contributes its own per-instance
            // absolute index set, unioned directly into the mapped forward
            // map. This mirrors `union_instance_maps`'s own union convention
            // extended to a derived (rather than declared) list: a producer
            // instance reachable through both the mapped arg and this plain
            // arg is still counted, and decremented, exactly once. An
            // unsupported shape (ordinal fallback, or `group_by`) is
            // rejected here with the same error `classify_coexisting_predecessor`
            // would give for a different predecessor.
            if plain_preds.contains(&pred_id) {
                let plain_per_instance = coexisting_predecessor_per_instance(graph, node, pred_id)
                    .map_err(|e| format!("node '{}': {}", node.name, e))?;
                for (j, extra) in plain_per_instance.into_iter().enumerate() {
                    if j >= union.len() {
                        continue;
                    }
                    for idx in extra {
                        if !union[j].contains(&idx) {
                            union[j].push(idx);
                        }
                    }
                    union[j].sort_unstable();
                }
            }

            for (j, list) in union.iter().enumerate() {
                thresholds[j] += list.len() as u32;
            }
            let routing = MappedEdgeRouting::build(&union, pred_factor);
            pred_instance_map[succ_id][pred_id as usize] = Some(Arc::new(routing));
        }

        // Non-mapped, coexisting predecessors on a DIFFERENT predecessor id:
        // uniform contribution per instance. A plain predecessor that is
        // ALSO mapped on this node was already folded into the mapped loop
        // above (Item 2) — skip it here so it is never counted twice.
        for &pred_id in &plain_preds {
            if mapped_preds.contains(&pred_id) {
                continue;
            }
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

// ---------------------------------------------------------------------------
// Arity validation — $res value delivery vs. the receiving kernel's arity.
// See the module docs' "Arity validation" section.
// ---------------------------------------------------------------------------

/// Position, within the flattened argument template the runtime hands to the
/// plugin wrapper, of every arg in `args` — mirroring
/// `runtime::node_cache::build_arg_cache`'s `idx_count` counter exactly:
/// every non-condition arg (including a `$barrier`, whose slot carries no
/// value) occupies one positional slot, in declared order; an
/// `init_condition`-guarded arg is skipped entirely (never materializes a
/// slot) and maps to `None` here. This is the SAME positional convention
/// `runtime::init::provable_variant`/`select_unchecked_wrappers` already rely
/// on when matching a plugin's per-argument-slot spec against a node's
/// declared args.
/// Number of FIXED (non-variadic) parameter slots in a published kernel
/// argument spec — every position `>= fixed` is at or after the kernel's
/// trailing `#[tomii_export(variadic)]` `Vec<T>` parameter, if it has one.
/// `spec`'s last entry is the literal `"..."` marker iff the kernel is
/// variadic (see `runtime::init::select_unchecked_wrappers`, the pre-existing
/// mechanism this also drives). Shared by [`validate_res_arity`] (load-time
/// check) and `runtime::init::build_node_cache` (dispatch-time
/// `ResPredCache::may_vary_width`) so the two stay in agreement by
/// construction rather than by keeping two copies of this arithmetic in sync.
pub(crate) fn variadic_fixed_boundary(spec: &[&str]) -> usize {
    let has_variadic = spec.last().copied() == Some("...");
    spec.len() - usize::from(has_variadic)
}

pub(crate) fn arg_template_positions(args: &[Arg]) -> Vec<Option<usize>> {
    let mut idx_count = 0usize;
    args.iter()
        .map(|arg| {
            if arg.is_condition() {
                None
            } else {
                let pos = idx_count;
                idx_count += 1;
                Some(pos)
            }
        })
        .collect()
}

/// Validate every `$res` argument in `graph` (mapped AND ordinary) against
/// the arity of the kernel function it is delivered to, wherever that arity
/// is knowable — see the module docs' "Arity validation" section for the
/// full design and the metadata source.
///
/// For a node whose kernel's per-argument spec is unavailable
/// (`crate::func_reg::get_func_argspec` returns `None` — an old plugin, a
/// Python-registered kernel, or any other kernel with no published spec),
/// this function skips that node's checks entirely: it cannot know which
/// parameter position is scalar vs. variadic, so it defers to the runtime
/// guard in `runtime::arg_resolution` instead of risking a false rejection.
///
/// For a node whose spec IS available: every `$res` arg at a position before
/// the kernel's fixed/variadic boundary (`spec.len() - 1` when the kernel is
/// variadic, else `spec.len()`) is a SCALAR parameter and must resolve to
/// exactly one value for EVERY consumer instance — a mapped `$res` arg's
/// per-instance width is `routing.forward[j].len()` (see
/// [`MappedEdgeRouting::forward`]); an ordinary `$res` arg's width is uniform
/// across every instance, mirroring `runtime::init::build_node_cache`'s
/// `all_width_one` computation exactly (`indexes.len() <= 1` or
/// `indexes.len() == node.factor` delivers 1; any other multi-index list
/// delivers `indexes.len()`, uniformly, to every instance). A `$res` arg at
/// or beyond the variadic boundary is unconstrained — any count, including
/// 0, is valid; that is the whole point of a variadic receiver.
///
/// Must run AFTER [`validate_and_build`] has succeeded (it reads the routing
/// tables that function builds for every mapped edge) — both call sites
/// (`graph_gen::from_json_str`, `graph_gen::GraphSpec::compile`) already
/// sequence the two calls this way.
pub fn validate_res_arity(graph: &Graph, tables: &InstanceMapTables) -> Result<(), String> {
    for node in &graph.nodes {
        if node.func_name.is_empty() {
            // Virtual nodes (e.g. the synthetic root added by the JSON parser)
            // call no kernel — nothing to check.
            continue;
        }
        let Some(spec) = crate::func_reg::get_func_argspec(&node.func_name) else {
            // No published arity metadata for this kernel — see doc comment
            // above. The runtime guard (`runtime::arg_resolution`) remains
            // the enforcement for this node.
            continue;
        };
        validate_node_res_arity_against_boundary(
            graph,
            tables,
            node,
            variadic_fixed_boundary(spec),
        )?;
    }
    Ok(())
}

/// Per-node arity check against an already-resolved `fixed` boundary (the
/// number of fixed/scalar parameter slots — see [`variadic_fixed_boundary`]).
///
/// Extracted from [`validate_res_arity`] so the boundary-agreement logic
/// (ANY `$res` arg at or after `fixed` may vary, every one before it must be
/// exactly width 1) can be unit-tested directly with a synthetic `fixed`,
/// without needing a real plugin's published metadata — this crate's own
/// `cargo test` build always resolves `get_func_argspec` to `None` (see
/// `tests::validate_res_arity_is_a_noop_without_published_kernel_metadata`),
/// so `validate_res_arity` itself can only be exercised end-to-end with a
/// real plugin (`tomii-core/tests/res_arity_validation.rs`).
///
/// This is also, by construction, the exact rule
/// `runtime::init::build_node_cache` uses to compute
/// `ResPredCache::may_vary_width` when a spec is published — see that
/// function's doc comment — so load-time acceptance and dispatch-time
/// support can never disagree for a node whose kernel publishes a spec.
fn validate_node_res_arity_against_boundary(
    graph: &Graph,
    tables: &InstanceMapTables,
    node: &Node,
    fixed: usize,
) -> Result<(), String> {
    let positions = arg_template_positions(&node.args);
    for (arg, pos) in node.args.iter().zip(positions.iter()) {
        let Some(pos) = *pos else { continue };
        if pos >= fixed {
            // At or beyond the variadic boundary (or simply beyond the
            // kernel's declared parameter count, which the generated
            // wrapper already ignores harmlessly) — any count is valid,
            // including 0, and including more than one `$res` arg sharing
            // this tail (each contributes its own values, concatenated in
            // declared order — see `runtime::arg_resolution`'s splice-order
            // doc comment).
            continue;
        }
        if !matches!(arg.type_, CmTypes::Res(_)) {
            // Only a `$res` arg delivers a value whose count can vary;
            // `$dep`/`$barrier` are gating-only and always resolve to
            // exactly one (`None`-valued) slot or no slot at all.
            continue;
        }
        let Some(pred) = &arg.predecessor else {
            continue;
        };
        let pred_name = &graph.nodes[pred.id as usize].name;

        if pred.instance_map.is_some() {
            let routing = tables
                .pred_instance_map
                .get(node.id as usize)
                .and_then(|row| row.get(pred.id as usize))
                .and_then(|opt| opt.as_ref())
                .ok_or_else(|| {
                    format!(
                        "node '{}': instance_map routing missing for predecessor '{pred_name}' \
                         (internal error — validate_res_arity must run after validate_and_build)",
                        node.name
                    )
                })?;
            for (j, list) in routing.forward.iter().enumerate() {
                if list.len() != 1 {
                    return Err(format!(
                        "node '{}': argument #{pos} (predecessor '{pred_name}', via \
                         instance_map) delivers {} value(s) to consumer instance {j}, but \
                         the kernel parameter at this position is scalar — it is not the \
                         kernel's trailing `#[tomii_export(variadic)]` `Vec<T>` parameter, \
                         so it must receive exactly one value for every instance. Either \
                         make this the kernel's trailing variadic parameter, or adjust the \
                         instance_map so every consumer instance maps to exactly one \
                         producer instance.",
                        node.name,
                        list.len()
                    ));
                }
            }
        } else {
            let width = if pred.indexes.len() <= 1 || pred.indexes.len() == node.factor {
                1
            } else {
                pred.indexes.len()
            };
            if width != 1 {
                return Err(format!(
                    "node '{}': argument #{pos} (predecessor '{pred_name}') delivers {width} \
                     values (a {}-index range) to every consumer instance, but the kernel \
                     parameter at this position is scalar — it is not the kernel's trailing \
                     `#[tomii_export(variadic)]` `Vec<T>` parameter, so it must receive \
                     exactly one value. Either make this the kernel's trailing variadic \
                     parameter, or declare a single index (or a 1:1-length range) instead.",
                    node.name,
                    pred.indexes.len()
                ));
            }
        }
    }
    Ok(())
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

    /// A mapped `$res` arg — the value-carrying counterpart of `dep_arg`/`barrier_arg`.
    fn mapped_res_arg(pred_id: IdType, map: Vec<Vec<usize>>) -> Arg {
        Arg {
            type_: CmTypes::Res(pred_id as usize),
            predecessor: Some(Predecessor {
                id: pred_id,
                indexes: Vec::new(),
                group_by: None,
                instance_map: Some(map),
            }),
            init_condition: None,
        }
    }

    /// A plain (non-mapped) `$dep` arg — used for Item 2 tests (mixed mapped
    /// + plain args on the SAME predecessor), where the plain arg's shape is
    /// gating-only and type-independent of the mapped arg it coexists with.
    fn plain_dep_arg(pred_id: IdType, indexes: Vec<isize>) -> Arg {
        Arg {
            type_: CmTypes::Dep(pred_id as usize),
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
    fn res_arg_with_instance_map_is_accepted_and_forward_list_retained() {
        // Mirrors the original rejection test's shape, now an acceptance test:
        // factor-4 producer, single factor-1 consumer instance reading producer 0.
        let mut g = Graph::new();
        g.add_node(make_node(0, 4, vec![]));
        g.add_node(make_node(1, 1, vec![mapped_res_arg(0, vec![vec![0]])]));
        let tables = validate_and_build(&g).unwrap();
        assert!(tables.has_instance_map[1]);
        assert_eq!(tables.node_thresholds[1].as_ref().unwrap()[0], 1);
        let routing = tables.pred_instance_map[1][0].as_ref().unwrap();
        assert_eq!(routing.forward[0], vec![0]);
        assert_eq!(routing.producer_to_consumers[0], vec![0]);
        // No other producer instance is listed.
        assert_eq!(routing.producer_to_consumers[1], Vec::<u32>::new());
    }

    #[test]
    fn mapped_res_arg_non_uniform_length_is_accepted_unconditionally() {
        // Length 0 (an instance with nothing to read) and length 2 (a "range" shape
        // for a variadic receiver) both load cleanly, mirroring ordinary multi-index
        // $res's lack of a load-time variadic check — see module docs.
        let mut g = Graph::new();
        g.add_node(make_node(0, 4, vec![]));
        g.add_node(make_node(
            1,
            3,
            vec![mapped_res_arg(0, vec![vec![], vec![0, 1], vec![2, 3]])],
        ));
        let tables = validate_and_build(&g).unwrap();
        let routing = tables.pred_instance_map[1][0].as_ref().unwrap();
        assert_eq!(routing.forward[0], Vec::<u32>::new());
        assert_eq!(routing.forward[1], vec![0, 1]);
        assert_eq!(routing.forward[2], vec![2, 3]);
        let thresholds = tables.node_thresholds[1].as_ref().unwrap();
        assert_eq!(**thresholds, vec![0, 2, 2]);
        // Instance 0's threshold is 0 (empty map, no coexisting predecessor) —
        // dispatched at admission, not through the dependency counter.
        assert_eq!(
            tables.zero_threshold_instances[1]
                .as_ref()
                .unwrap()
                .as_ref(),
            &vec![0]
        );
    }

    #[test]
    fn mapped_res_arg_out_of_range_producer_index_is_rejected() {
        // The SAME bounds check that already applies to $barrier/$dep (see
        // `property_random_single_pred_maps_threshold_and_routing_agree`) applies to
        // $res unchanged — this restriction was never specific to the arg type.
        let mut g = Graph::new();
        g.add_node(make_node(0, 2, vec![]));
        g.add_node(make_node(1, 1, vec![mapped_res_arg(0, vec![vec![5]])]));
        let err = validate_and_build(&g).unwrap_err();
        assert!(err.contains("out of range"), "{err}");
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
    fn mixed_mapped_and_plain_broadcast_on_same_pred_is_unioned() {
        // Item 2: a mapped $dep (instance 0 -> producers {0,1}) PLUS a plain
        // literal $barrier on the SAME predecessor (producers {2,3}, filtered
        // broadcast since succ_factor forces absolute-subset semantics).
        // Every producer instance 0..4 must end up routed to consumer 0
        // exactly once, union {0,1} u {2,3} = {0,1,2,3} -> threshold 4.
        let mut g = Graph::new();
        g.add_node(make_node(0, 4, vec![]));
        g.add_node(make_node(
            1,
            1,
            vec![dep_arg(0, vec![vec![0, 1]]), barrier_arg(0, vec![2, 3])],
        ));
        let tables = validate_and_build(&g).unwrap();
        assert_eq!(
            *tables.node_thresholds[1].as_ref().unwrap().as_ref(),
            vec![4]
        );
        let routing = tables.pred_instance_map[1][0].as_ref().unwrap();
        assert_eq!(
            routing.producer_to_consumers,
            vec![vec![0], vec![0], vec![0], vec![0]]
        );
    }

    #[test]
    fn mixed_mapped_and_plain_1to1_on_same_pred_is_unioned_per_instance() {
        // A mapped $dep with an irregular per-instance map on pred 0 (factor
        // 4), PLUS a plain 1:1 same-factor $res on the SAME predecessor
        // (succ[j] reads pred[j]). Each consumer instance's threshold is the
        // union of its declared map entry and its own 1:1 cell.
        let mut g = Graph::new();
        g.add_node(make_node(0, 4, vec![]));
        g.add_node(make_node(
            1,
            4,
            vec![
                dep_arg(0, vec![vec![1], vec![], vec![0], vec![3]]),
                res_arg(0, vec![0]),
            ],
        ));
        let tables = validate_and_build(&g).unwrap();
        let thresholds = tables.node_thresholds[1].as_ref().unwrap();
        // j=0: {1} u {0} = {0,1} -> 2.  j=1: {} u {1} = {1} -> 1.
        // j=2: {0} u {2} = {0,2} -> 2.  j=3: {3} u {3} = {3} -> 1.
        assert_eq!(thresholds.as_slice(), &[2, 1, 2, 1]);
        let routing = tables.pred_instance_map[1][0].as_ref().unwrap();
        // Per-instance forward maps after the union: j0={0,1}, j1={1},
        // j2={0,2}, j3={3}. Inverted: producer 0 -> {0,2}; producer 1 ->
        // {0,1}; producer 2 -> {2}; producer 3 -> {3}.
        assert_eq!(routing.producer_to_consumers[0], vec![0, 2]);
        assert_eq!(routing.producer_to_consumers[1], vec![0, 1]);
        assert_eq!(routing.producer_to_consumers[2], vec![2]);
        assert_eq!(routing.producer_to_consumers[3], vec![3]);
    }

    #[test]
    fn mixed_same_pred_ordinal_fallback_shape_is_rejected() {
        // The plain arg's shape (no barrier, no 1:1, no filtered range) has no
        // uniform/positional per-instance interpretation even when coexisting
        // with a mapped arg on the SAME predecessor.
        let mut g = Graph::new();
        g.add_node(make_node(0, 4, vec![]));
        g.add_node(make_node(
            1,
            1,
            vec![
                dep_arg(0, vec![vec![0, 1, 2, 3]]),
                res_arg(0, vec![0, 2]), // 2 of 4, non-contiguous-with-hole -> no filter
            ],
        ));
        let err = validate_and_build(&g).unwrap_err();
        assert!(
            err.contains("no uniform per-instance interpretation"),
            "{err}"
        );
    }

    #[test]
    fn mixed_same_pred_group_by_is_rejected() {
        let mut g = Graph::new();
        g.add_node(make_node(0, 4, vec![]));
        let mut grouped_res = res_arg(0, vec![0, 1, 2, 3]);
        grouped_res.predecessor.as_mut().unwrap().group_by = Some(2);
        g.add_node(make_node(
            1,
            1,
            vec![dep_arg(0, vec![vec![0, 1, 2, 3]]), grouped_res],
        ));
        let err = validate_and_build(&g).unwrap_err();
        assert!(err.contains("group_by"), "{err}");
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

    // -----------------------------------------------------------------------
    // Item 2 property cross-check: a plain argument coexisting on the SAME
    // predecessor as the mapped one. The expected per-instance forward map is
    // the union of the random mapped map with the plain arg's OWN derived
    // per-instance list (computed independently, in the test, from the plain
    // shape — not by calling production code) — exactly the invariant
    // `validate_and_build` is supposed to establish.
    // -----------------------------------------------------------------------

    #[test]
    fn property_random_mixed_same_pred_1to1_threshold_and_routing_agree() {
        // 1:1 shape requires succ_factor == pred_factor (the exact-offset
        // convention); offset `k` is a random relative index into the SAME
        // factor.
        let mut rng = Xorshift64(0xD1B54A32D192ED03);
        for trial in 0..200u32 {
            let factor = 1 + rng.range(10);
            let offset = rng.range(factor) as isize;

            let forward: Vec<Vec<usize>> = (0..factor)
                .map(|_| {
                    let mut seen = HashSet::new();
                    let picks = rng.range(factor + 1);
                    for _ in 0..picks {
                        seen.insert(rng.range(factor));
                    }
                    let mut v: Vec<usize> = seen.into_iter().collect();
                    v.sort_unstable();
                    v
                })
                .collect();

            let mut g = Graph::new();
            g.add_node(make_node(0, factor, vec![]));
            g.add_node(make_node(
                1,
                factor,
                vec![dep_arg(0, forward.clone()), plain_dep_arg(0, vec![offset])],
            ));
            let tables = validate_and_build(&g).unwrap_or_else(|e| {
                panic!(
                    "trial {trial}: unexpected rejection (factor={factor}, offset={offset}): {e}"
                )
            });
            let thresholds = tables.node_thresholds[1].as_ref().unwrap();
            let routing = tables.pred_instance_map[1][0].as_ref().unwrap();

            // Expected union: forward[j] u {(j + offset) mod factor} —
            // computed independently of `coexisting_predecessor_per_instance`.
            let expected: Vec<Vec<usize>> = (0..factor)
                .map(|j| {
                    let one_to_one = ((j as isize + offset).rem_euclid(factor as isize)) as usize;
                    let mut v = forward[j].clone();
                    if !v.contains(&one_to_one) {
                        v.push(one_to_one);
                    }
                    v.sort_unstable();
                    v
                })
                .collect();

            for (j, exp) in expected.iter().enumerate() {
                assert_eq!(
                    thresholds[j] as usize,
                    exp.len(),
                    "trial {trial} (factor={factor}, offset={offset}): threshold[{j}] mismatch, \
                     forward={forward:?}, expected={expected:?}"
                );
            }

            let mut brute_force_received: Vec<usize> = vec![0; factor];
            for i in 0..factor {
                for &j in &routing.producer_to_consumers[i] {
                    brute_force_received[j as usize] += 1;
                }
            }
            for (j, exp) in expected.iter().enumerate() {
                assert_eq!(
                    brute_force_received[j],
                    exp.len(),
                    "trial {trial} (factor={factor}, offset={offset}): consumer {j} received \
                     {} decrements via the reverse table, expected {} — expected={expected:?}",
                    brute_force_received[j],
                    exp.len()
                );
            }
            for i in 0..factor {
                for &j in &routing.producer_to_consumers[i] {
                    assert!(
                        expected[j as usize].contains(&i),
                        "trial {trial} (factor={factor}, offset={offset}): reverse table lists \
                         producer {i} for consumer {j}, but expected[{j}] = {:?} does not \
                         contain it",
                        expected[j as usize]
                    );
                }
            }
        }
    }

    #[test]
    fn property_random_mixed_same_pred_broadcast_threshold_and_routing_agree() {
        // Broadcast shape: a plain, filtered, CONTIGUOUS absolute index
        // range on the same predecessor as the mapped arg — every consumer
        // instance's forward map gains the SAME extra members.
        let mut rng = Xorshift64(0x27D4EB2F165667C5);
        for trial in 0..200u32 {
            let pred_factor = 2 + rng.range(10); // >= 2 so a proper subset exists
            let succ_factor = 1 + rng.range(10);

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

            // A proper, contiguous subset [lo, lo+len) of the predecessor's
            // factor — guaranteed to qualify as a filtered broadcast (see
            // `coexisting_predecessor_shape`). A contiguous range whose
            // length happens to equal `succ_factor` is a GENUINELY different,
            // correctly-handled shape (the `contiguous_range_1to1` 1:1
            // interpretation takes priority — see
            // `property_random_mixed_same_pred_1to1_threshold_and_routing_agree`
            // for that case) — skip trials that land on it so this test
            // exercises only the unambiguous broadcast shape.
            let len = 1 + rng.range(pred_factor - 1);
            // A single-index member set on a same-factor predecessor is ALSO
            // the (prioritized) `single_1to1` shape — skip that collision
            // too, for the same reason as the `len == succ_factor` skip
            // above.
            if len == succ_factor || (len == 1 && pred_factor == succ_factor) {
                continue;
            }
            let lo = rng.range(pred_factor - len + 1);
            let members: Vec<isize> = (lo..lo + len).map(|i| i as isize).collect();

            let mut g = Graph::new();
            g.add_node(make_node(0, pred_factor, vec![]));
            g.add_node(make_node(
                1,
                succ_factor,
                vec![
                    dep_arg(0, forward.clone()),
                    plain_dep_arg(0, members.clone()),
                ],
            ));
            let tables = validate_and_build(&g).unwrap_or_else(|e| {
                panic!(
                    "trial {trial}: unexpected rejection (pred_factor={pred_factor}, \
                     succ_factor={succ_factor}, members={members:?}): {e}"
                )
            });
            let thresholds = tables.node_thresholds[1].as_ref().unwrap();
            let routing = tables.pred_instance_map[1][0].as_ref().unwrap();

            let members_usize: Vec<usize> = members.iter().map(|&i| i as usize).collect();
            let expected: Vec<Vec<usize>> = forward
                .iter()
                .map(|list| {
                    let mut v = list.clone();
                    for &m in &members_usize {
                        if !v.contains(&m) {
                            v.push(m);
                        }
                    }
                    v.sort_unstable();
                    v
                })
                .collect();

            for (j, exp) in expected.iter().enumerate() {
                assert_eq!(
                    thresholds[j] as usize,
                    exp.len(),
                    "trial {trial} (pred_factor={pred_factor}, succ_factor={succ_factor}, \
                     members={members:?}): threshold[{j}] mismatch, forward={forward:?}, \
                     expected={expected:?}"
                );
            }

            let mut brute_force_received: Vec<usize> = vec![0; succ_factor];
            for i in 0..pred_factor {
                for &j in &routing.producer_to_consumers[i] {
                    brute_force_received[j as usize] += 1;
                }
            }
            for (j, exp) in expected.iter().enumerate() {
                assert_eq!(
                    brute_force_received[j],
                    exp.len(),
                    "trial {trial} (pred_factor={pred_factor}, succ_factor={succ_factor}): \
                     consumer {j} received {} decrements via the reverse table, expected {} — \
                     expected={expected:?}",
                    brute_force_received[j],
                    exp.len()
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // Arity validation ($res value delivery vs. the receiving kernel's arity)
    // -----------------------------------------------------------------------

    #[test]
    fn arg_template_positions_skips_condition_args_but_counts_barriers() {
        // Mirrors `runtime::node_cache::build_arg_cache`'s `idx_count`
        // exactly: an `init_condition`-guarded arg never materializes a
        // slot (`None`); every other arg — including a `$barrier`, whose
        // slot carries no value — occupies the next sequential position.
        let mut cond_literal = res_arg(0, vec![0]);
        cond_literal.init_condition = Some(crate::graph_struct::InitCondition {
            operation: crate::graph_struct::CondOp::Eq,
            eval_value: CmTypes::Bool(true),
        });
        let args = vec![
            res_arg(0, vec![0]),       // position 0
            cond_literal,              // skipped entirely
            barrier_arg(0, vec![0]),   // position 1
            dep_arg(0, vec![vec![0]]), // position 2
        ];
        let positions = arg_template_positions(&args);
        assert_eq!(positions, vec![Some(0), None, Some(1), Some(2)]);
    }

    #[test]
    fn validate_res_arity_is_a_noop_without_published_kernel_metadata() {
        // This crate's own `cargo test` build resolves every plugin function
        // through the `func_reg` stub (see `lib.rs`'s `not(build_rs_ran)`
        // module), which always returns `None` from `get_func_argspec` —
        // exactly the metadata-absent case `validate_res_arity` must treat as
        // "skip, defer to the runtime guard" (see the module docs' "Arity
        // validation" section). A graph shape that WOULD be rejected once a
        // real plugin publishes its kernel's scalar arity — see
        // `tomii-core/tests/res_arity_validation.rs` for that e2e case —
        // must load cleanly here.
        let mut g = Graph::new();
        g.add_node(make_node(0, 4, vec![]));
        let mut bad_node = make_node(1, 1, vec![res_arg(0, vec![0, 1, 2])]); // 3 values, no metadata to check against
        bad_node.func_name = "definitely_not_a_registered_kernel".to_string();
        g.add_node(bad_node);
        let tables = validate_and_build(&g).unwrap();
        validate_res_arity(&g, &tables).expect(
            "metadata-absent fallback: validate_res_arity must not reject when \
             get_func_argspec returns None",
        );
    }

    #[test]
    fn validate_res_arity_skips_virtual_nodes_with_no_func_name() {
        // The synthetic root `graph_gen::from_json_str` adds (and any other
        // node with an empty `func_name`) calls no kernel — must never be
        // looked up in the registry at all, let alone rejected.
        let mut g = Graph::new();
        g.add_node(make_node(0, 4, vec![])); // func_name defaults to "" via make_node
        let tables = validate_and_build(&g).unwrap();
        validate_res_arity(&g, &tables).unwrap();
    }

    // -----------------------------------------------------------------------
    // `validate_node_res_arity_against_boundary` — the per-node, pure boundary
    // check extracted from `validate_res_arity` (see its doc comment). Tested
    // directly with a synthetic `fixed` because this crate's own `cargo test`
    // build always resolves `get_func_argspec` to `None` (see
    // `validate_res_arity_is_a_noop_without_published_kernel_metadata` above),
    // so `validate_res_arity` itself can never reach its load-accepts/rejects
    // branches in an in-crate unit test — only this extracted function can be
    // unit-tested for that agreement; the full load-time behaviour (including
    // the `get_func_argspec` lookup) is covered end-to-end by
    // `tomii-core/tests/res_arity_validation.rs` against a real plugin.
    // -----------------------------------------------------------------------

    /// Two mapped `$res` args, BOTH at or after the variadic boundary
    /// (`fixed = 0`: every position is "tail"), with non-uniform per-instance
    /// widths — including 0 — on BOTH producers simultaneously. This is
    /// exactly the shape the original bug rejected the agreement on: the
    /// load-time check must accept it, matching the dispatcher's
    /// (post-fix) `may_vary_width`/reverse-splice support for multiple
    /// simultaneously-varying tail args (see
    /// `runtime::arg_resolution::mapped_res_value_tests::multiple_variable_width_tail_res_args_splice_independently`
    /// for the corresponding dispatch-level test).
    #[test]
    fn accepts_multiple_variable_width_tail_res_args() {
        let mut g = Graph::new();
        g.add_node(make_node(0, 3, vec![])); // p0
        g.add_node(make_node(1, 3, vec![])); // p1
        g.add_node(make_node(
            2,
            2,
            vec![
                mapped_res_arg(0, vec![vec![0, 1], vec![2]]), // widths 2, 1
                mapped_res_arg(1, vec![vec![], vec![0, 1, 2]]), // widths 0, 3
            ],
        ));
        let tables = validate_and_build(&g).unwrap();
        validate_node_res_arity_against_boundary(&g, &tables, &g.nodes[2], 0).expect(
            "both $res args sit at/after a fixed=0 boundary — any width, including \
             varying simultaneously on both producers, must be accepted",
        );
    }

    /// Same two-mapped-`$res`-arg node as above, but `fixed = 1`: the FIRST
    /// arg (position 0, predecessor `p0`) is now a SCALAR position and must
    /// resolve to exactly one value for every instance — it does not
    /// (instance 0 has width 2) — so this must be rejected, naming the node,
    /// the predecessor, and the scalar/variadic mismatch, even though the
    /// SECOND arg (position 1, at/after the boundary) is allowed to vary.
    #[test]
    fn rejects_mapped_res_arg_at_a_scalar_position_even_with_a_variadic_tail_after_it() {
        let mut g = Graph::new();
        g.add_node(make_node(0, 3, vec![])); // p0
        g.add_node(make_node(1, 3, vec![])); // p1
        g.add_node(make_node(
            2,
            2,
            vec![
                mapped_res_arg(0, vec![vec![0, 1], vec![2]]), // position 0 — now scalar
                mapped_res_arg(1, vec![vec![], vec![0, 1, 2]]), // position 1 — still tail
            ],
        ));
        let tables = validate_and_build(&g).unwrap();
        let err = validate_node_res_arity_against_boundary(&g, &tables, &g.nodes[2], 1)
            .expect_err("position 0 is scalar (fixed=1) and varies in width — must be rejected");
        assert!(
            err.contains("n2") && err.contains("argument #0") && err.contains("scalar"),
            "expected the error to name the node and argument position and explain the \
             scalar/variadic mismatch; got: {err}"
        );
    }

    /// An ORDINARY (non-mapped) multi-index `$res` arg at a scalar position
    /// must still be rejected exactly as before this fix — the boundary
    /// check applies identically to mapped and ordinary edges (only the
    /// per-instance width computation differs; see the module doc's "Arity
    /// validation" section).
    #[test]
    fn rejects_ordinary_multi_index_res_arg_at_a_scalar_position() {
        let mut g = Graph::new();
        g.add_node(make_node(0, 4, vec![])); // p0, factor 4
                                             // factor 1 consumer, indexes.len() == 3 != node.factor (1) and != 1 → width 3
        g.add_node(make_node(1, 1, vec![res_arg(0, vec![0, 1, 2])]));
        let tables = validate_and_build(&g).unwrap();
        // `fixed = 1`: position 0 is BELOW the boundary, i.e. scalar.
        let err = validate_node_res_arity_against_boundary(&g, &tables, &g.nodes[1], 1)
            .expect_err("a 3-index ordinary $res at a scalar (fixed=1) position must be rejected");
        assert!(
            err.contains("n1") && err.contains("3 values") && err.contains("scalar"),
            "expected the error to name the node, the delivered value count, and the \
             scalar/variadic mismatch; got: {err}"
        );
    }

    /// Per-instance width is computed independently for EVERY instance, not
    /// just instance 0 — a mapped `$res` arg at a scalar position whose
    /// FIRST instance happens to have width 1 but a LATER instance does not
    /// must still be rejected (catches an off-by-one that only checks
    /// `routing.forward[0]`).
    #[test]
    fn rejects_mapped_res_arg_when_a_later_instance_varies_even_if_the_first_does_not() {
        let mut g = Graph::new();
        g.add_node(make_node(0, 4, vec![])); // p0
        g.add_node(make_node(
            1,
            3,
            vec![mapped_res_arg(0, vec![vec![0], vec![1], vec![2, 3]])], // instance 2 has width 2
        ));
        let tables = validate_and_build(&g).unwrap();
        // `fixed = 1`: the node's only arg sits at position 0, below the boundary.
        let err = validate_node_res_arity_against_boundary(&g, &tables, &g.nodes[1], 1)
            .expect_err("instance 2's width-2 delivery at a scalar position must be rejected");
        assert!(
            err.contains("instance 2"),
            "expected the error to name the offending consumer instance; got: {err}"
        );
    }

    #[test]
    fn variadic_fixed_boundary_detects_the_trailing_ellipsis_marker() {
        assert_eq!(variadic_fixed_boundary(&["Usize", "Usize", "..."]), 2);
        assert_eq!(variadic_fixed_boundary(&["Usize", "Usize"]), 2);
        assert_eq!(variadic_fixed_boundary(&["..."]), 0);
        assert_eq!(variadic_fixed_boundary(&[]), 0);
    }
}
