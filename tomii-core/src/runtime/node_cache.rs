//! Per-node cache entries built once at startup to eliminate per-task graph lookups.
//! [`NodeCacheEntry`] mirrors the hot fields of a `Node` with pre-computed flags such as
//! `worker_resolvable` and `needs_result_store` that drive fast-path decisions in the runtime.
use crate::debug::print_debug;
use crate::func_reg::get_func;
use crate::{graph_struct::*, IdType};
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
