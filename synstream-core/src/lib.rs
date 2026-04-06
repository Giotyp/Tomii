// Allow some clippy warnings that are acceptable in this crate:
// - too_many_arguments: Runtime functions need many parameters for context
// - type_complexity: Graph processing uses complex nested types intentionally
// - ptr_arg: &Vec is sometimes needed for trait compatibility
// - new_without_default: Some constructors need parameters
#![allow(clippy::too_many_arguments)]
#![allow(clippy::type_complexity)]
#![allow(clippy::new_without_default)]

pub mod async_recorder;
pub mod buffers;
pub(crate) mod core_alloc;
pub mod custom_scheduler;
pub mod debug;
pub mod graph;
pub mod graph_gen;
pub mod graph_struct;
pub mod json_structs;
pub mod network;
pub(crate) mod network_funcs;
pub(crate) mod obj_gen;
pub(crate) mod resolution_state;
pub mod runtime;
pub mod scheduler;
pub mod time_buffer;
pub mod utils_rdtsc;

/// Node and object identifier type. `u16` supports up to 65 535 graph nodes.
pub type IdType = u16;

/// Boxed error type returned by public API boundaries (`from_json`, `init_objects`, etc.).
pub type SynError = Box<dyn std::error::Error + Send + Sync + 'static>;

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

/// Represents a range of worker thread indices [start, end)
/// Used to specify which worker threads should execute specific tasks.
/// Range is inclusive on start, exclusive on end (e.g., "0-7" → WorkerRange { start: 0, end: 8 })
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WorkerRange {
    pub start: usize, // Inclusive start
    pub end: usize,   // Exclusive end
}

impl WorkerRange {
    /// Parse from JSON string format "0-7" → WorkerRange { start: 0, end: 8 }
    /// The range is inclusive on both ends in the string (user sees "0-7"),
    /// but stored as [start, end) internally.
    pub fn parse(s: &str) -> Result<Self, String> {
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() != 2 {
            return Err(format!(
                "Invalid range format: '{}'. Expected 'start-end'",
                s
            ));
        }

        let start = parts[0]
            .trim()
            .parse::<usize>()
            .map_err(|_| format!("Invalid start value '{}' in range '{}'", parts[0], s))?;
        let end_inclusive = parts[1]
            .trim()
            .parse::<usize>()
            .map_err(|_| format!("Invalid end value '{}' in range '{}'", parts[1], s))?;

        if start > end_inclusive {
            return Err(format!(
                "Invalid range '{}': start {} > end {}",
                s, start, end_inclusive
            ));
        }

        // Convert inclusive end to exclusive [start, end)
        Ok(WorkerRange {
            start,
            end: end_inclusive + 1,
        })
    }

    /// Check if a worker index is in this range
    pub fn contains(&self, worker_idx: usize) -> bool {
        worker_idx >= self.start && worker_idx < self.end
    }

    /// Get number of workers in this range
    pub fn len(&self) -> usize {
        if self.end > self.start {
            self.end - self.start
        } else {
            0
        }
    }

    /// Check if this range is empty
    pub fn is_empty(&self) -> bool {
        self.end <= self.start
    }
}

impl std::fmt::Display for WorkerRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_empty() {
            write!(f, "[empty]")
        } else {
            write!(f, "{}-{}", self.start, self.end - 1)
        }
    }
}

/// Specification for worker allocation - supports both count-based and range-based formats
/// - Count format (backward compatible): "4" means allocate any 4 workers
/// - Range format (explicit): "0-7" means use workers 0-7 specifically
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WorkerRangeSpec {
    /// Count-based: allocate N workers dynamically at runtime
    Count(usize),
    /// Range-based: use specific worker indices
    Range(WorkerRange),
}

impl WorkerRangeSpec {
    /// Parse from JSON string supporting both formats:
    /// - "4" → Count(4) - use any 4 workers
    /// - "0-7" → Range(0-8) - use workers 0-7 specifically
    pub fn parse(s: &str) -> Result<Self, String> {
        let s = s.trim();

        // Try parsing as range first (contains '-')
        if s.contains('-') {
            // Check if it's a range like "0-7" or negative number like "-5"
            let parts: Vec<&str> = s.split('-').collect();
            if parts.len() == 2 && !parts[0].is_empty() {
                // This is a range "0-7"
                return WorkerRange::parse(s).map(WorkerRangeSpec::Range);
            }
        }

        // Try parsing as count
        match s.parse::<usize>() {
            Ok(count) => {
                if count == 0 {
                    Err("Worker count must be > 0".to_string())
                } else {
                    Ok(WorkerRangeSpec::Count(count))
                }
            }
            Err(_) => {
                // If it looks like a range but failed to parse, report range error
                if s.contains('-') {
                    WorkerRange::parse(s).map(WorkerRangeSpec::Range)
                } else {
                    Err(format!(
                        "Invalid use_workers format '{}'. Expected count (e.g., '4') or range (e.g., '0-7')",
                        s
                    ))
                }
            }
        }
    }

    /// Convert a count-based spec to a range, given a preferred start index
    /// Used at scheduler build time to allocate specific workers for count-based specs
    pub fn to_range(&self, start_idx: usize) -> WorkerRange {
        match self {
            WorkerRangeSpec::Count(count) => WorkerRange {
                start: start_idx,
                end: start_idx + count,
            },
            WorkerRangeSpec::Range(range) => range.clone(),
        }
    }

    /// Get the worker count from this spec
    pub fn count(&self) -> usize {
        match self {
            WorkerRangeSpec::Count(n) => *n,
            WorkerRangeSpec::Range(r) => r.len(),
        }
    }

    /// Check if this is a count-based spec (needs runtime allocation)
    pub fn is_count_based(&self) -> bool {
        matches!(self, WorkerRangeSpec::Count(_))
    }
}

impl std::fmt::Display for WorkerRangeSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkerRangeSpec::Count(n) => write!(f, "{} workers", n),
            WorkerRangeSpec::Range(r) => write!(f, "workers {}", r),
        }
    }
}
