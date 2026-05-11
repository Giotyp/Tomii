pub mod async_recorder;
pub(crate) mod buffers;
pub(crate) mod core_alloc;
pub(crate) mod custom_scheduler;
pub mod debug;
pub mod dependency_counter;
pub mod graph;
pub mod graph_gen;
pub mod graph_struct;
pub mod json_structs;
pub mod network;
pub(crate) mod network_funcs;
pub(crate) mod obj_gen;
/// Backward-compatibility alias — new code should use `dependency_counter`.
#[doc(hidden)]
pub(crate) mod resolution_state;
pub mod runtime;
pub mod scheduler;
pub(crate) mod time_buffer;
pub mod utils_rdtsc;

/// Node and object identifier type. `u16` supports up to 65 535 graph nodes.
pub type IdType = u16;

/// Boxed error type returned by public API boundaries (`from_json`, `init_objects`, etc.).
pub type TomiiError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Errors that can occur while building the Τομί runtime.
#[derive(Debug)]
pub enum BuildError {
    /// A configuration invariant was violated (slots, capacity, etc.).
    InvalidConfig(String),
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuildError::InvalidConfig(msg) => write!(f, "invalid config: {msg}"),
        }
    }
}

impl std::error::Error for BuildError {}

/// Errors that can occur while running the Τομί runtime.
#[derive(Debug)]
pub enum RuntimeError {
    /// A worker or receiver thread failed to spawn.
    SpawnFailed(std::io::Error),
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuntimeError::SpawnFailed(e) => write!(f, "thread spawn failed: {e}"),
        }
    }
}

impl std::error::Error for RuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RuntimeError::SpawnFailed(e) => Some(e),
        }
    }
}

/// Metadata attached to a spawned task for timing and recording.
///
/// Passed from the resolution loop to the scheduler so the recording layer can
/// correlate execution events back to the originating graph node.
#[derive(Debug, Clone, Copy)]
pub struct TaskMeta {
    /// Graph node identifier.
    pub task_id: IdType,
    /// Execution slot (stream index mod `slots`).
    pub slot: usize,
    /// Instance index within a multi-factor node.
    pub index: usize,
    /// Whether this task should be timed and recorded.
    pub should_record: bool,
}

/// A single timing record emitted by a worker thread.
///
/// Written to CSV when `--timing` is enabled. Each record captures the wall-clock
/// interval during which a task ran on a specific worker.
#[derive(Debug, Clone)]
pub struct Record {
    /// Execution slot (stream index mod `slots`).
    pub slot: usize,
    /// Monotonically increasing spawn counter used as a unique job ID.
    pub job_id: usize,
    /// Task start time in nanoseconds relative to the base instant.
    pub start_ns: u128,
    /// Task end time in nanoseconds relative to the base instant.
    pub end_ns: u128,
    /// Physical core ID of the worker that ran this task.
    pub worker: usize,
    /// Graph node identifier.
    pub task_id: IdType,
    /// Instance index within a multi-factor node.
    pub index: usize,
}

pub mod prelude {
    pub use crate::IdType;
}

#[cfg(build_rs_ran)]
pub mod wrappers {
    include!(concat!(env!("OUT_DIR"), "/wrappers.rs"));
}
#[cfg(not(build_rs_ran))]
pub mod wrappers {}

// Re-exports
pub use crate::async_recorder::AsyncRecorder;
pub use crate::custom_scheduler::Priority;
pub use crate::dependency_counter::{DependencyCounter, MultiThreadedCounter};
pub use crate::runtime::RuntimeConfig;
#[cfg(build_rs_ran)]
pub mod func_reg {
    include!(concat!(env!("OUT_DIR"), "/func_reg.rs"));
}
#[cfg(not(build_rs_ran))]
pub mod func_reg {
    use tomii_types::{CmBulkPtr, CmPtr, CmTypes};
    /// Stub used when no plugin registry has been generated (test / check-only builds).
    #[allow(dead_code)]
    pub fn get_func(_: &str) -> Option<CmPtr> {
        None
    }
    /// Stub: no bulk kernels registered in test / check-only builds.
    #[allow(dead_code)]
    pub fn get_bulk_func(_: &str) -> Option<CmBulkPtr> {
        None
    }
}
#[cfg(build_rs_ran)]
pub mod funcs {
    include!(concat!(env!("OUT_DIR"), "/funcs.rs"));
}
#[cfg(not(build_rs_ran))]
pub mod funcs {}

pub mod worker_range;
pub use worker_range::{WorkerRange, WorkerRangeSpec};
