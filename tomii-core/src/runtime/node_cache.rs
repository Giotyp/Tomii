//! Per-node cache entries built once at startup to eliminate per-task graph lookups.
//! [`NodeCacheEntry`] mirrors the hot fields of a `Node` with pre-computed flags such as
//! `worker_resolvable` and `needs_result_store` that drive fast-path decisions in the runtime.
use crate::debug::print_debug;
use crate::func_reg::{get_bulk_func, get_func};
use crate::instance_map::MappedEdgeRouting;
use crate::{graph_struct::*, IdType};
use std::sync::Arc;
use tomii_types::*;

/// Resolve a function pointer by name, panicking in production if not found.
/// In test mode, falls back to a no-op so integration tests can run without a plugin.
#[inline]
fn resolve_func(name: &str) -> CmPtr {
    #[cfg(not(any(test, feature = "test-utils")))]
    {
        get_func(name).unwrap_or_else(|| panic!("Function '{}' not found in registry", name))
    }
    #[cfg(any(test, feature = "test-utils"))]
    {
        fn test_noop(_: &[CmTypes]) -> CmTypes {
            CmTypes::None
        }
        get_func(name).unwrap_or(test_noop)
    }
}

/// Resolve the optional bulk kernel pointer for a function.
/// Returns `None` when no `{name}_bulk_cm` symbol is registered (opt-in, not required).
#[inline]
fn resolve_bulk_func(name: &str) -> Option<CmBulkPtr> {
    get_bulk_func(name)
}

// Cache entry for quick node access - stores commonly accessed node fields
#[derive(Clone)]
pub struct NodeCacheEntry {
    pub factor: usize,
    pub pred_vec: Vec<usize>,
    pub name: String,
    pub func_ptr: CmPtr,
    pub arg_cache: ArgCacheEntry,
    // Pre-computed flag: true if this node is in initial_nodes
    pub is_initial: bool,
    // Pre-computed flag: true if this node is in condition_nodes
    pub is_condition: bool,
    // Pre-computed index into cond_indexes array (only valid if is_condition is true)
    pub cond_index: usize,
    // Phase 3B: Number of successors (for inline execution optimization)
    // Allows fast lookup without traversing successors list
    pub successor_count: usize,
    // Node-level condition cache (new format)
    pub node_condition: Option<NodeConditionCache>,
    // Pre-computed scheduler priority (avoids per-task conversion from NodePriority)
    pub priority: crate::custom_scheduler::Priority,
    // Pre-computed scheduler affinity group (avoids per-task use_workers.clone() + lookup)
    pub affinity_group: usize,
    // Pre-computed flag: true if all successors are non-condition nodes,
    // meaning worker threads can resolve dependencies directly without
    // going through the resolution thread's batch_queue.
    pub worker_resolvable: bool,
    // Pre-computed flag: true if any successor reads this node's result via $res.
    // When false, no successor consumes the result and the node_results.set() call
    // can be elided entirely, saving a hash-map write on the hot path.
    pub needs_result_store: bool,
    /// Optional bulk kernel pointer.  Non-`None` when the plugin exports a
    /// `{func_name}_bulk_cm` symbol (registered via `get_bulk_func`).
    ///
    /// When `Some`, `execute_bulk_task` calls the bulk kernel **once** with the full
    /// `(start, end, args)` range instead of looping over `populate_dynamic_args_into`
    /// once per cell.  Nodes with `needs_result_store == true` always fall back to the
    /// per-cell loop regardless of this field.
    pub bulk_func: Option<CmBulkPtr>,
    /// True when this node is eligible for 1:1 fanout bulk dispatch (Upgrade 5).
    ///
    /// Eligibility: single $res predecessor with equal factor, worker_resolvable,
    /// not a condition/network node, no $barrier args.  When true, the arrived
    /// counter in SlotData::fanout_bulk_arrived batches N individual instances into
    /// one bulk task instead of N per-cell dispatches.
    pub is_fanout_bulk: bool,
    /// True when this node's arg vector has a build-time-constant length: every
    /// `$res`/`$dep` argument resolves to exactly one value (no collect-all splice).
    ///
    /// When true, `execute_single_task` reuses a persistent per-(thread, node) arg
    /// template and patches only the dynamic slots per task (P3 Phase A), instead
    /// of re-cloning the static template — and re-dropping it — on every task.
    /// Computed in `build_node_cache` after `res_predecessors` is populated.
    pub template_stable: bool,
    /// Plugin function name (registry key) — retained for the P3 Phase B
    /// unchecked-wrapper selection pass, which needs registry lookups after
    /// the cache is built.
    pub func_name: String,
    /// True when `func_ptr` was swapped to the `_unchecked` wrapper twin
    /// (P3 Phase B): every argument slot's variant was proven at build time,
    /// so the wrapper skips variant matches and bounds checks.
    pub uses_unchecked: bool,
    /// True when this node has at least one `instance_map` predecessor. Such a
    /// node uses the per-instance dependency-counter mechanism
    /// (`buffers::instance_map_dep::InstanceMapDepMap`) for ALL of its
    /// predecessors — mapped and ordinary alike — instead of the pooled/ordinal
    /// `NodeDependencyEntry`. See `crate::instance_map`.
    pub has_instance_map: bool,
}

#[derive(Clone)]
pub struct NodeConditionCache {
    pub operation: CondOp,
    pub eval_value: CmTypes,
    pub func_ptr: CmPtr,
    pub arg_cache: ArgCacheEntry,
}

/// Pre-resolved predecessor metadata for a `$res` or `$dep` argument.
///
/// Cached at build time to eliminate per-task `shared.graph.nodes[...]` lookups
/// in the `populate_cached_args_into` hot path.
#[derive(Clone)]
pub struct ResPredCache {
    /// ID of the current (successor) node — needed for `pred_group_by` table lookups.
    pub node_id: crate::IdType,
    /// ID of the predecessor (result) node.
    pub res_node_id: crate::IdType,
    /// Relative instance offsets declared in the argument (`predecessor.indexes`).
    pub indexes: Vec<isize>,
    /// Predecessor node's parallel factor.
    pub pred_factor: usize,
    /// Predecessor node's `group_size` (for grouped-symbol index calculation).
    pub pred_group_size: Option<usize>,
    /// Current node's `group_size` (for grouped-symbol index calculation).
    pub node_group_size: Option<usize>,
    /// Current node's parallel factor (used for the 1:1-mapping check).
    pub node_factor: usize,
    /// True when this is a `$dep` (ordering-only) argument; result is always `CmTypes::None`.
    pub is_dep: bool,
    /// True when the CURRENT (successor) node's dependency counter is in
    /// forced "release the whole factor together" mode — i.e.
    /// `buffers::node_dep::NodeDepMap::new`'s own `has_barrier` is true for
    /// this node (a literal `$barrier` arg on ANY predecessor, or an
    /// ambiguous multi-arg predecessor; see `node_has_ambiguous_multi_res_args`).
    ///
    /// `per_instance_dep_idx`'s `pred_factor == node_factor` fast path assumes
    /// the triggering `pred_index` already identifies the correct predecessor
    /// cell for the firing successor instance — true only for a genuine 1:1
    /// trigger-paired dispatch (`NodeDependencyEntry`'s `specific_succ_idx`
    /// path). When `node_has_barrier` is true, EVERY instance in the batch is
    /// released together carrying the SAME triggering `pred_index` (whichever
    /// predecessor completion zeroed the shared counter — on ANY predecessor,
    /// not necessarily this `$res` arg's own), so trusting it for this
    /// predecessor's per-instance index is wrong; the declared `indexes[node_index]`
    /// must be used instead. Safe to do so unconditionally in that case: the
    /// shared counter cannot reach zero until every declared cell on every
    /// predecessor has completed, so the result is already stored.
    pub node_has_barrier: bool,
    /// `Some(routing)` when this `$res` argument's predecessor edge carries an
    /// explicit `instance_map` — `routing.forward[node_index]` lists, in
    /// ascending absolute-producer-index order, exactly the producer instances
    /// to fetch (see `crate::instance_map`'s module docs and
    /// `runtime::arg_resolution::fetch_mapped_res_results`). `None` for the
    /// overwhelming majority of `$res`/`$dep` args, which use the classic
    /// `indexes`/`pred_index` resolution in `fetch_res_results` instead. Mapped
    /// routing takes precedence over every other field on this struct when
    /// present — `indexes`/`pred_factor`/`node_has_barrier` etc. are irrelevant
    /// for a mapped edge (every producer index is already absolute and
    /// explicit; there is no ambiguity for `pred_index`/`group_size` to resolve).
    pub mapped_forward: Option<Arc<MappedEdgeRouting>>,
}

#[derive(Clone, Default)]
pub struct ArgCacheEntry {
    // initially store ref indexes for node id
    pub args: Vec<CmTypes>,
    // indexes of buffer ref in args
    pub buffer_ref_indexes: Vec<usize>,
    // buffer values
    pub buffer_values: Vec<Vec<CmTypes>>,
    // indexes of $ref::index in args
    pub rt_idxs_indexes: Vec<usize>,
    // indexes of $ref::worker in args
    pub rt_workers_indexes: Vec<usize>,
    // Indexes of $ref::$frame_id in args. Empty for every node that does not
    // reference `$frame_id` — the overwhelming majority — so the hot-path
    // checks that gate frame-id resolution (`populate_cached_args_into`,
    // `populate_dynamic_args_into`, `execute_bulk_task`'s `has_dynamic`) are
    // simple `is_empty()` reads with no extra work when unused. See
    // `crate::runtime::arg_resolution::process_runtime_refs`.
    pub rt_frame_indexes: Vec<usize>,
    // indexes of $res in args
    pub res_indexes: Vec<usize>,
    // real indexes of $res
    pub real_res_indexes: Vec<usize>,
    // Pre-resolved predecessor/node metadata for each $res/$dep arg.
    // Aligned with res_indexes: res_predecessors[i] corresponds to res_indexes[i].
    // Populated by build_node_cache after all NodeCacheEntry values are constructed.
    pub res_predecessors: Vec<ResPredCache>,
}

impl std::fmt::Debug for ArgCacheEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArgCacheEntry")
            .field("args", &self.args)
            .field("buffer_ref_indexes", &self.buffer_ref_indexes)
            .field("buffer_values", &self.buffer_values)
            .field("rt_idxs_indexes", &self.rt_idxs_indexes)
            .field("rt_workers_indexes", &self.rt_workers_indexes)
            .field("rt_frame_indexes", &self.rt_frame_indexes)
            .field("res_indexes", &self.res_indexes)
            .field("real_res_indexes", &self.real_res_indexes)
            .field("res_predecessors.len", &self.res_predecessors.len())
            .finish()
    }
}

#[inline]
pub(super) fn node_cache_entry(
    node: &Node,
    init_objects: &[Vec<CmTypes>],
    initial_nodes: &[crate::IdType],
    condition_nodes: &std::collections::HashSet<crate::IdType>,
) -> NodeCacheEntry {
    print_debug(|| {
        format!(
            "Creating node cache entry for node {} name {}",
            node.id, node.name
        )
    });

    if node.name == "$network" {
        return NodeCacheEntry {
            factor: node.factor,
            pred_vec: Vec::new(),
            name: node.name.clone(),
            func_ptr: CmTypes::default_pointer(),
            arg_cache: ArgCacheEntry::default(),
            is_initial: false,
            is_condition: false,
            cond_index: 0,
            successor_count: 0,
            node_condition: None,
            priority: crate::custom_scheduler::Priority::Normal,
            affinity_group: 0,
            worker_resolvable: false,
            needs_result_store: false,
            bulk_func: None,
            is_fanout_bulk: false,
            template_stable: false,
            func_name: node.func_name.clone(),
            uses_unchecked: false,
            has_instance_map: false,
        };
    }

    let (arg_cache, pred_hash) = build_arg_cache(&node.args, init_objects, true);
    let pred_vec = build_pred_vec(pred_hash);

    let cond_index = if condition_nodes.contains(&node.id) {
        condition_nodes
            .iter()
            .position(|&x| x == node.id)
            .unwrap_or(0)
    } else {
        0
    };

    NodeCacheEntry {
        factor: node.factor,
        pred_vec,
        name: node.name.clone(),
        func_ptr: resolve_func(&node.func_name),
        arg_cache,
        is_initial: initial_nodes.contains(&node.id),
        is_condition: condition_nodes.contains(&node.id),
        cond_index,
        successor_count: 0,
        node_condition: build_condition_cache(node, init_objects),
        priority: crate::custom_scheduler::Priority::Normal,
        affinity_group: 0,
        worker_resolvable: false,
        needs_result_store: false,
        bulk_func: resolve_bulk_func(&node.func_name),
        is_fanout_bulk: false,  // populated in build_node_cache second pass
        template_stable: false, // populated in build_node_cache second pass
        func_name: node.func_name.clone(),
        uses_unchecked: false, // populated by select_unchecked_wrappers
        has_instance_map: node
            .args
            .iter()
            .any(|a| a.predecessor.as_ref().is_some_and(|p| p.has_instance_map())),
    }
}

/// Build an [`ArgCacheEntry`] from a list of node arguments.
///
/// When `skip_conditions` is true, args where `is_condition() == true` are skipped
/// (used for main node args, which interleave condition and non-condition entries).
/// Also returns the predecessor hash needed to build `pred_vec` (callers that don't
/// need it, e.g. condition arg caches, can simply discard it).
fn build_arg_cache(
    args: &[Arg],
    init_objects: &[Vec<CmTypes>],
    skip_conditions: bool,
) -> (ArgCacheEntry, std::collections::HashMap<IdType, Vec<usize>>) {
    let mut rt_idxs_indexes = Vec::new();
    let mut buffer_ref_indexes = Vec::new();
    let mut buffer_values = Vec::new();
    let mut rt_workers_indexes = Vec::new();
    let mut rt_frame_indexes = Vec::new();
    let mut real_res_indexes = Vec::new();
    let mut res_indexes = Vec::new();
    let mut args_out = vec![CmTypes::None; args.len()];
    let mut pred_hash: std::collections::HashMap<IdType, Vec<usize>> =
        std::collections::HashMap::new();
    let mut idx_count = 0;

    for (idx, arg) in args.iter().enumerate() {
        if skip_conditions && arg.is_condition() {
            continue;
        }
        match &arg.type_ {
            CmTypes::Ref(obj_id) => {
                if *obj_id == 0 {
                    rt_idxs_indexes.push(idx_count);
                } else if *obj_id == 1 {
                    rt_workers_indexes.push(idx_count);
                } else if *obj_id == 2 {
                    rt_frame_indexes.push(idx_count);
                } else {
                    let obj_vec = &init_objects[*obj_id];
                    if obj_vec.len() > 1 {
                        buffer_ref_indexes.push(idx_count);
                        buffer_values.push(obj_vec.clone());
                    } else {
                        args_out[idx_count] = obj_vec[0].clone();
                    }
                }
            }
            CmTypes::Res(_) | CmTypes::Dep(_) => {
                res_indexes.push(idx_count);
                real_res_indexes.push(idx);
                if let Some(pred) = arg.predecessor.as_ref() {
                    pred_hash
                        .entry(pred.id)
                        .or_default()
                        .push(pred.indexes.len());
                }
            }
            CmTypes::Barrier(_) => {}
            _ => {
                args_out[idx_count] = arg.type_.clone();
            }
        }
        idx_count += 1;
    }

    (
        ArgCacheEntry {
            args: args_out,
            buffer_ref_indexes,
            buffer_values,
            rt_idxs_indexes,
            rt_workers_indexes,
            rt_frame_indexes,
            res_indexes,
            real_res_indexes,
            res_predecessors: Vec::new(), // populated later by build_node_cache second pass
        },
        pred_hash,
    )
}

/// Convert the predecessor hash into a dense `pred_vec` indexed by predecessor node ID.
fn build_pred_vec(pred_hash: std::collections::HashMap<IdType, Vec<usize>>) -> Vec<usize> {
    let max_pred_id = pred_hash.keys().max().cloned().unwrap_or(0);
    let mut pred_vec = vec![0usize; max_pred_id as usize + 1];
    for (pred_id, counts) in &pred_hash {
        let unique: std::collections::HashSet<usize> = counts.iter().cloned().collect();
        pred_vec[*pred_id as usize] = *unique.iter().max().unwrap();
    }
    pred_vec
}

/// Build the optional [`NodeConditionCache`] for a node that carries a condition expression.
fn build_condition_cache(node: &Node, init_objects: &[Vec<CmTypes>]) -> Option<NodeConditionCache> {
    let cond = node.condition.as_ref()?;
    let (arg_cache, _) = build_arg_cache(&cond.args, init_objects, false);
    Some(NodeConditionCache {
        operation: cond.operation.clone(),
        eval_value: cond.eval_value.clone(),
        func_ptr: resolve_func(&cond.func_name),
        arg_cache,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_struct::Predecessor;

    fn ref_arg(obj_id: usize) -> Arg {
        Arg {
            type_: CmTypes::Ref(obj_id),
            init_condition: None,
            predecessor: None,
        }
    }

    fn string_arg(s: &str) -> Arg {
        Arg {
            type_: CmTypes::String(std::sync::Arc::from(s)),
            init_condition: None,
            predecessor: None,
        }
    }

    #[test]
    fn frame_id_ref_populates_rt_frame_indexes_only() {
        // obj_id 2 is the reserved `$frame_id` slot (see `crate::obj_gen::init_objects`).
        let args = vec![string_arg("tag"), ref_arg(2), string_arg("suffix")];
        let (cache, _) = build_arg_cache(&args, &[], false);
        assert_eq!(cache.rt_frame_indexes, vec![1]);
        assert!(cache.rt_idxs_indexes.is_empty());
        assert!(cache.rt_workers_indexes.is_empty());
        // Non-$ref args keep their literal values in `args` at their own slots.
        assert_eq!(cache.args[0], CmTypes::String(std::sync::Arc::from("tag")));
        assert_eq!(
            cache.args[2],
            CmTypes::String(std::sync::Arc::from("suffix"))
        );
        // The $frame_id slot itself is a placeholder (overwritten at runtime by
        // `arg_resolution::process_runtime_refs`) — never read before that.
        assert_eq!(cache.args[1], CmTypes::None);
    }

    #[test]
    fn ordinary_node_has_empty_rt_frame_indexes() {
        // A node with no `$frame_id` (or any `$ref`) arg at all: `rt_frame_indexes`
        // must be empty — this is the zero-cost fast path every existing graph
        // takes (see `arg_resolution::populate_cached_args_into`'s early-return
        // guard, which short-circuits entirely on this emptiness).
        let args = vec![
            string_arg("x"),
            Arg {
                type_: CmTypes::Res(0),
                init_condition: None,
                predecessor: Some(Predecessor {
                    id: 0,
                    indexes: vec![0],
                    group_by: None,
                    instance_map: None,
                }),
            },
        ];
        let (cache, _) = build_arg_cache(&args, &[], false);
        assert!(cache.rt_frame_indexes.is_empty());
        assert!(cache.rt_idxs_indexes.is_empty());
        assert!(cache.rt_workers_indexes.is_empty());
    }

    #[test]
    fn frame_id_coexists_with_index_and_workers_refs() {
        // All three runtime-injected refs can appear on the same node,
        // independently, at different slots.
        let args = vec![ref_arg(0), ref_arg(1), ref_arg(2)];
        let (cache, _) = build_arg_cache(&args, &[], false);
        assert_eq!(cache.rt_idxs_indexes, vec![0]);
        assert_eq!(cache.rt_workers_indexes, vec![1]);
        assert_eq!(cache.rt_frame_indexes, vec![2]);
    }

    /// Informational, not a hot-path invariant: `ArgCacheEntry` is a per-NODE
    /// cache entry (one per distinct graph node, built once at compile time and
    /// shared via reference — see `NodeCacheEntry::arg_cache`), never copied
    /// per-task. Adding `rt_frame_indexes: Vec<usize>` grows it by one `Vec`
    /// (24 bytes on a 64-bit target), which is acceptable here precisely
    /// because it is NOT the hot-path struct — contrast with
    /// `buffers::node_info::tests::size_is_unchanged_by_the_frame_id_feature`,
    /// which asserts the per-TASK struct (`NodeInfo`) is byte-for-byte
    /// unchanged.
    #[test]
    fn arg_cache_entry_size_grows_by_one_vec_not_a_hot_path_concern() {
        let sz = std::mem::size_of::<ArgCacheEntry>();
        let vec_sz = std::mem::size_of::<Vec<usize>>();
        // Sanity: the new field is present and sized like every other Vec<usize>
        // field already on this struct (rt_idxs_indexes/rt_workers_indexes).
        assert!(
            sz >= vec_sz,
            "ArgCacheEntry must be at least large enough to hold one Vec<usize>, got {sz}"
        );
    }
}
