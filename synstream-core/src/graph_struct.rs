use crate::prelude::*;
use synstream_types::*;

pub trait GraphStruct {
    fn add_node(&mut self, node: Node);
    fn add_post_node(&mut self, node: Node);
    fn find_successors(&self, node_id: IdType) -> &Vec<IdType>;
    fn dependency_count_vec(&self) -> Vec<usize>;
}

/// Helper functions
/// Find the adjusted index of a predecessor node
pub fn find_pred_index(node_idx: usize, pred_idx: isize, pred_factor: usize) -> usize {
    // Find the index of the node in the results
    if pred_factor == 0 {
        panic!("Predecessor factor is 0 - check your graph configuration");
    }
    let req_idx = (node_idx as isize + pred_idx) % pred_factor as isize;
    req_idx as usize
}

/// Comparison operators
#[derive(Clone, Debug)]
pub enum CondOp {
    Eq,
    Neq,
    Gt,
    Lt,
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

/// Node Initialization  (Optional) Condition
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

#[derive(Clone)]
pub struct Predecessor {
    pub id: IdType,
    pub indexes: Vec<isize>,
}

#[derive(Clone)]
pub struct Arg {
    pub type_: CmTypes,
    // Optional condition for initialization
    pub init_condition: Option<InitCondition>,
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

#[derive(Clone)]
pub struct Loop {
    pub name: String,
    pub factor: usize,
}

#[derive(Clone)]
pub struct Node {
    pub name: String,
    pub args: Vec<Arg>,
    pub id: IdType,
    pub loop_args: Option<Vec<Arg>>,
    // Variable that defines the number of times
    // the node is initiated
    pub factor: usize,
    pub func_ptr: Option<CmPtr>,
    // Optional node to loop after execution
    pub loop_: Option<Loop>,
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

#[derive(Clone)]
pub struct IdFunction {
    pub func_ptr: Option<CmPtr>,
    pub predecessor: IdType,
    pub args: Vec<Arg>,
}
