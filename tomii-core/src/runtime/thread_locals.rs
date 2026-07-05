//! Thread-local storage for the Τομί runtime.
//!
//! All runtime thread-locals live here so their coupling relationships are visible
//! in one place. For each entry: what it stores, who writes it, who reads it.
//!
//! There are two classes of thread:
//! - **Worker threads** — execute tasks, resolve successors inline when
//!   `worker_resolvable` is true.
//! - **Resolution threads** — drain `batch_queue`, process completions, spawn
//!   new tasks.

use crate::buffers::NodeInfo;
use std::cell::RefCell;
use tomii_types::CmTypes;

// ---------------------------------------------------------------------------
// Supporting types
// ---------------------------------------------------------------------------

/// Reusable scratch buffers for worker-side successor resolution.
///
/// Bundled into one struct so a single `thread_local!` entry replaces several,
/// eliminating deep `.with()` nesting in `worker_resolve_successors`.
/// (Successor *enumeration* needs no buffer — the hot loop iterates
/// `SuccessorArena::edges_for` directly.)
///
/// Written and read by: worker threads in `worker_resolve_successors` only.
pub(super) struct WorkerResolutionBuffers {
    pub ready: Vec<usize>,
    pub sched: Vec<NodeInfo>,
    pub args: Vec<Option<Vec<CmTypes>>>,
}

/// Reusable scratch buffers for `process_batch_resolution` → `process_batch_inner`.
///
/// Bundled into one struct so a single `thread_local!` entry replaces several,
/// collapsing deep `.with()` nesting to one level.
///
/// Written and read by: resolution threads inside `process_batch_inner` only.
pub(super) struct BatchInnerBuffers {
    pub schedule: Vec<NodeInfo>,
    pub ready: Vec<usize>,
    pub batch_sched: Vec<NodeInfo>,
}

// ---------------------------------------------------------------------------
// Thread-local declarations
// ---------------------------------------------------------------------------

thread_local! {
    /// Temporary argument assembly buffer used by `populate_cached_args_into`.
    ///
    /// Written by: worker threads during arg resolution.
    /// Read by: worker threads immediately after writing (same call frame).
    /// Lifecycle: cleared at the start of each `populate_cached_args_into` call.
    pub(super) static ARG_BUF: RefCell<Vec<CmTypes>> =
        RefCell::new(Vec::with_capacity(16));

    /// Persistent per-node arg templates (P3 Phase A), indexed by node id.
    ///
    /// For `template_stable` nodes, the static arg template is cloned once per
    /// (thread, node) on first execution and only the dynamic slots (buffer
    /// refs, runtime indices, `$res` results) are rewritten per task — no
    /// per-task template clone, no per-task drop of the static `CmTypes`
    /// (each static `Any` arc costs an atomic inc+dec per task on the legacy
    /// path).
    ///
    /// Written and read by: worker threads in `execute_single_task` only.
    /// Lifecycle: lives for the thread's lifetime. A slot may briefly retain
    /// the previous task's `$res` value until the next task of the same node
    /// overwrites it — bounded by (nodes × threads) values. After a stale-task
    /// abort, partially patched slots are harmless: every dynamic slot is
    /// unconditionally rewritten before the next kernel call.
    pub(super) static NODE_ARG_TEMPLATES: RefCell<Vec<Option<Vec<CmTypes>>>> =
        const { RefCell::new(Vec::new()) };

    /// Worker-side dependency resolution buffers — all four in one allocation.
    ///
    /// Written and read by: worker threads in `worker_resolve_successors`.
    /// Lifecycle: cleared at the entry of each `worker_resolve_successors` call.
    pub(super) static WORKER_BUFS: RefCell<WorkerResolutionBuffers> =
        RefCell::new(WorkerResolutionBuffers {
            ready: Vec::with_capacity(32),
            sched: Vec::with_capacity(32),
            args:  Vec::with_capacity(32),
        });

    /// Batch resolution inner buffers — all four in one allocation.
    ///
    /// Written and read by: resolution threads inside `process_batch_inner`.
    /// Lifecycle: cleared at the entry of each `process_batch_inner` call.
    pub(super) static BATCH_INNER_BUFS: RefCell<BatchInnerBuffers> =
        RefCell::new(BatchInnerBuffers {
            schedule:     Vec::with_capacity(32),
            ready:        Vec::with_capacity(16),
            batch_sched:  Vec::with_capacity(256),
        });

    /// Staging buffer for task completions drained from `batch_queue`.
    ///
    /// Written by: `drain_and_process_batch_queue` (extends from raw batch_buf).
    /// Read by: `process_batch_resolution` (iterates to process completions).
    /// Lifecycle: cleared at the start of each drain call.
    ///
    /// Kept separate from `BATCH_INNER_BUFS` because `drain_and_process_batch_queue`
    /// holds this borrow while calling `process_batch_inner` (which borrows
    /// `BATCH_INNER_BUFS`). Merging them would cause a re-entrant borrow panic.
    pub(super) static TASK_COMP_BUF: RefCell<Vec<(NodeInfo, Option<CmTypes>)>> =
        RefCell::new(Vec::with_capacity(256));

    /// Reusable args buffer for `preparation()` — eliminates `vec![None; N]` heap
    /// allocation on every incremental flush (~77 flushes/stream).
    ///
    /// Written and read by: resolution threads (or worker threads, depending on
    /// scheduling mode) inside `schedule_post_nodes` → `preparation`.
    /// Lifecycle: truncated to the required length at the start of each call.
    pub(super) static PREP_ARGS_BUF: RefCell<Vec<Option<Vec<CmTypes>>>> =
        RefCell::new(Vec::with_capacity(64));
}
