pub mod async_recorder;
pub mod buffers;
pub mod debug;
pub mod graph;
pub mod graph_gen;
pub mod graph_struct;
pub mod json_structs;
pub mod obj_gen;
pub mod resolution_state;
pub mod runtime;
pub mod runtime_funcs;
pub mod scheduler;
pub mod time_buffer;
pub mod utils_rdtsc;

use lazy_static::lazy_static;
use std::sync::atomic::AtomicU16;

pub type IdType = u16;
lazy_static! {
    pub static ref ObjectCount: AtomicU16 = AtomicU16::new(0);
    pub static ref NodeCount: AtomicU16 = AtomicU16::new(0);
    pub static ref PostNodeCount: AtomicU16 = AtomicU16::new(0);
}

// Record for thread executions
#[derive(Debug, Clone)]
pub struct Record {
    pub slot: usize,
    pub job_id: usize,
    pub start_ns: u128,
    pub end_ns: u128,
    pub worker: usize,
    pub task_id: IdType,
    pub index: usize,
}

pub mod prelude {
    pub use crate::{IdType, NodeCount, ObjectCount, PostNodeCount};
}

pub mod wrappers {
    include!(concat!(env!("OUT_DIR"), "/wrappers.rs"));
}

// Re-exports
pub use crate::async_recorder::AsyncRecorder;
pub mod func_reg {
    include!(concat!(env!("OUT_DIR"), "/func_reg.rs"));
}
pub mod funcs {
    include!(concat!(env!("OUT_DIR"), "/funcs.rs"));
}
