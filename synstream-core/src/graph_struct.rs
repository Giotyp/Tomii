use crate::network::SocketType;
use crate::prelude::*;
use synstream_types::*;

/// Find the adjusted index of a predecessor node
pub fn find_pred_index(node_idx: usize, pred_idx: isize, pred_factor: usize) -> usize {
    // Find the index of the node in the results
    if pred_factor == 0 {
        panic!("Predecessor factor is 0 - check your graph configuration");
    }
    let req_idx = (node_idx as isize + pred_idx) % pred_factor as isize;
    req_idx as usize
}

/// Comparison operator used in node conditions and argument guards.
#[derive(Clone, Debug)]
pub enum CondOp {
    Eq,
    Neq,
    Gt,
    Lt,
}

/// Task priority for scheduling
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum NodePriority {
    High,
    #[default]
    Normal,
    Low,
}

impl NodePriority {
    pub fn from_str(s: &str) -> Self {
        match s {
            "high" => NodePriority::High,
            "low" => NodePriority::Low,
            _ => NodePriority::Normal,
        }
    }
}

impl CondOp {
    pub fn from_str(op: &str) -> Option<CondOp> {
        match op {
            "Eq" => Some(CondOp::Eq),
            "Neq" => Some(CondOp::Neq),
            "Gt" => Some(CondOp::Gt),
            "Lt" => Some(CondOp::Lt),
            _ => None,
        }
    }
}

/// Optional guard on a `$ref` argument: the argument is only used if
/// `operation(arg_value, eval_value)` is true at initialization time.
#[derive(Clone, Debug)]
pub struct InitCondition {
    pub operation: CondOp,
    pub eval_value: CmTypes,
}

impl InitCondition {
    pub fn evaluate(&self, arg_value: &CmTypes) -> bool {
        // Evaluate against arg_value that is decided during runtime

        match self.operation {
            CondOp::Eq => arg_value == &self.eval_value,
            CondOp::Neq => arg_value != &self.eval_value,
            _ => {
                // Handle other operations (Gt, Lt) as needed
                // Currently returns false
                false
            }
        }
    }
}

/// Runtime condition gating a node's execution: the node only runs if
/// `func_name(args) op eval_value` is true when the node is ready to fire.
/// The function pointer is resolved at cache-build time via the function registry.
#[derive(Clone, Debug)]
pub struct NodeCondition {
    pub operation: CondOp,
    pub eval_value: CmTypes,
    pub func_name: String,
    pub args: Vec<Arg>,
}

impl NodeCondition {
    pub fn evaluate(&self, result_value: &CmTypes) -> bool {
        // Evaluate function result against expected value
        match self.operation {
            CondOp::Eq => result_value == &self.eval_value,
            CondOp::Neq => result_value != &self.eval_value,
            CondOp::Gt => {
                // Implement if needed
                false
            }
            CondOp::Lt => {
                // Implement if needed
                false
            }
        }
    }
}

/// Data dependency on a predecessor node.
///
/// `indexes` is the list of relative instance offsets used to select which
/// predecessor instance(s) a successor consumes. `group_by` enables grouping
/// multiple predecessor instances before firing the successor.
#[derive(Clone, Debug)]
pub struct Predecessor {
    /// ID of the predecessor node.
    pub id: IdType,
    /// Relative instance offsets (e.g. `[0]` = same index, `[-1]` = previous).
    pub indexes: Vec<isize>,
    /// If set, group this many predecessor completions before spawning one successor.
    pub group_by: Option<usize>,
}

impl Predecessor {
    /// Computes the predecessor index filter range if this edge should be filtered.
    ///
    /// Returns `Some((min_idx, max_idx_exclusive))` when only instances in `[min, max)` of
    /// the predecessor will send dependency decrements to the successor. Returns `None` when
    /// all `pred_factor` instances contribute.
    ///
    /// This is the single authoritative definition of the filter predicate — consumed by
    /// `Graph::dependency_count_vec()` (graph.rs) and `build_predecessor_tables()` (runtime/init.rs).
    pub fn index_filter(&self, pred_factor: usize, succ_factor: usize) -> Option<(usize, usize)> {
        if self.indexes.is_empty() {
            return None;
        }
        let min_idx = *self.indexes.iter().min().unwrap() as usize;
        let max_idx = *self.indexes.iter().max().unwrap() as usize;
        let range_len = max_idx - min_idx + 1;

        let should_filter = if self.group_by.is_some() {
            true
        } else {
            range_len < pred_factor && range_len == self.indexes.len() && range_len == succ_factor
        };

        if should_filter {
            Some((min_idx, max_idx + 1))
        } else {
            None
        }
    }

    /// Returns the number of predecessor instances that send dependency decrements to this successor.
    ///
    /// When `index_filter` applies, only the filtered subset (indexes.len()) contributes.
    /// Otherwise all `pred_factor` instances contribute.
    pub fn contributing_instances(&self, pred_factor: usize, succ_factor: usize) -> usize {
        match self.index_filter(pred_factor, succ_factor) {
            Some(_) => self.indexes.len(),
            None => pred_factor,
        }
    }
}

/// A single argument to a graph node.
///
/// An argument is either a literal value (`type_` only), a reference to a
/// predecessor result (`predecessor`), or a conditional init-time guard
/// (`init_condition`).
#[derive(Clone, Debug)]
pub struct Arg {
    /// The type-erased value or type tag for this argument.
    pub type_: CmTypes,
    /// Optional guard evaluated at initialization time.
    pub init_condition: Option<InitCondition>,
    /// If set, this argument is resolved from a predecessor node's output.
    pub predecessor: Option<Predecessor>,
}

impl Arg {
    pub fn is_condition(&self) -> bool {
        self.init_condition.is_some()
    }

    pub fn is_barrier(&self) -> bool {
        self.type_.is_barrier()
    }
}

/// Specifies a loop-back target for a node: after the node runs it re-queues
/// itself (up to `factor` times) to the node named `name`.
#[derive(Clone)]
pub struct Loop {
    /// Name of the target node to loop back to.
    pub name: String,
    /// Maximum number of loop iterations.
    pub factor: usize,
}

/// A node in the task graph.
///
/// Each node represents a callable unit of work with typed arguments and
/// optional data-flow dependencies on predecessor nodes.
#[derive(Clone)]
pub struct Node {
    /// Human-readable node name (used for JSON lookup and debug output).
    pub name: String,
    /// Argument list passed to the node function at execution time.
    pub args: Vec<Arg>,
    /// Unique numeric identifier assigned during graph construction.
    pub id: IdType,
    /// Arguments used only inside a loop body (if this node loops).
    pub loop_args: Option<Vec<Arg>>,
    /// Number of parallel instances (stream fan-out factor).
    pub factor: usize,
    /// If set, instances are grouped in batches of this size before the node fires.
    pub group_size: Option<usize>,
    /// Plugin function name looked up in the function registry at cache-build time.
    /// Empty string for virtual nodes (e.g. `$network`) that have no associated function.
    pub func_name: String,
    /// If set, the node loops back to the named node after execution.
    pub loop_: Option<Loop>,
    /// Optional runtime condition — the node only fires when the condition evaluates to true.
    pub condition: Option<NodeCondition>,
    /// Task scheduling priority (default: `Normal`).
    pub priority: NodePriority,
    /// Worker affinity: `None` = all workers, `Some` = count or range-based allocation.
    pub use_workers: Option<crate::WorkerRangeSpec>,
}

impl Node {
    pub fn condition_args(&self) -> Vec<&Arg> {
        let mut cond_args: Vec<&Arg> = Vec::new();
        for arg in &self.args {
            if arg.is_condition() {
                cond_args.push(arg);
            }
        }
        cond_args
    }
}

/// User-supplied function that extracts a stream ID from a received packet.
///
/// The function receives the predecessor node's output plus any extra `args`
/// and returns a `CmTypes` value used to map the packet to the correct slot.
#[derive(Clone)]
pub struct IdFunction {
    /// Resolved function pointer.
    pub func_ptr: Option<CmPtr>,
    /// Node whose result is forwarded as the first argument.
    pub predecessor: IdType,
    /// Additional static arguments passed after the predecessor result.
    pub args: Vec<Arg>,
}

/// User-supplied function that maps a packet to a node-instance index.
///
/// Called after the stream ID is resolved; the returned value selects which
/// parallel instance of the downstream node should receive the packet.
#[derive(Clone, Debug)]
pub struct IndexFunction {
    /// Resolved function pointer.
    pub func_ptr: Option<CmPtr>,
    /// Arguments passed to the function.
    pub args: Vec<Arg>,
}

/// Network reception configuration attached to a graph.
///
/// Parsed from the `network_config` block in the JSON graph definition.
#[derive(Clone, Debug)]
pub struct GraphNetworkConfig {
    /// Transport protocol for receiving packets.
    pub socket_type: SocketType,
    /// Number of sockets (and receiver threads) to bind.
    pub num_sockets: usize,
    /// Expected byte length of every incoming packet.
    pub packet_length: usize,
    /// Total number of packets that constitute one complete stream.
    pub stream_packets: usize,
    /// How many streams can be buffered before back-pressure is applied.
    pub buffer_depth: usize,
    /// IP address to bind sockets on.
    pub address: String,
    /// First UDP/TCP port; subsequent sockets use `start_port + i`.
    pub start_port: usize,
    /// Optional function that extracts a payload slice from the raw packet bytes.
    pub extract_packet_func: Option<CmPtr>,
    /// Optional function that derives a stream ID from packet bytes.
    pub id_function: Option<CmPtr>,
    /// Optional function that maps a packet to a specific node-instance index.
    pub index_function: Option<IndexFunction>,
}
