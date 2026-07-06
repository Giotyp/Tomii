//! Pluggable resolution-strategy trait.
//!
//! A [`ResolutionStrategy`] drives one complete batch of dependency resolution:
//! it takes a batch of completed nodes, stores results, decrements dependency
//! counters, collects ready successors, and dispatches them to the scheduler.
//!
//! The trait also covers the worker fast path (`worker_resolve`) which runs
//! directly on a worker thread for nodes whose successors are all non-condition.
//!
//! # How the seam is wired
//! Every batch-protocol entry point routes through `ExecCtx::resolution_strategy`:
//! the batch-queue drain (`resolution_loop::drain_and_process_batch_queue`), network
//! packet ingestion (`packet_processing`), and buffered-slot activation
//! (`slot_lifecycle`) all call [`ResolutionStrategy::drive_batch`]. One indirect call
//! per *batch* — amortised to noise across the 8–256 tasks a batch carries.
//!
//! # `worker_resolve` is deliberately NOT called per task
//! The worker fast path (`task_execution::worker_resolve_successors`) is on the
//! per-task critical path in inline-continuation mode; an indirect call + vtable
//! load there would cost 2–5% of the dispatch budget on fine-grained workloads
//! (anti-diagonal wavefront). Worker-side behaviour is instead specialised
//! *data-driven* — via `NodeCacheEntry` flags and the `SuccessorArena` edge data —
//! so both paths change together without a per-task virtual call.
//! `worker_resolve` exists on the trait so a custom strategy can keep its batch
//! protocol consistent with the worker path; it is invoked only from strategy code,
//! not from the runtime's per-task hot loop.
//!
//! # v1 note
//! Only [`MultiSlotBatchStrategy`] is shipped in v1. Future strategies
//! (priority-aware, frozen-graph) register in `bin/main.rs` behind the
//! `--resolution-strategy` flag and plug in without modifying `resolution_loop.rs`.

use super::shared_data::{SchedCtx, SharedData};
use crate::buffers::NodeInfo;
use std::collections::HashMap;
use std::sync::Arc;
use tomii_types::CmTypes;

/// Outcome returned from a single batch drive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchOutcome {
    /// Batch was processed; more work may be available.
    Continue,
    /// Batch was empty; caller may yield.
    Empty,
}

/// Pluggable resolution strategy.
///
/// Implementors must be `Send + Sync` because the same instance may be called
/// from multiple resolution threads simultaneously (one call per thread per
/// batch). Each call receives independent `batch` and `stream_slot_activity`
/// buffers — no shared mutable state is required beyond `&Arc<SharedData>`.
pub trait ResolutionStrategy: Send + Sync + 'static {
    /// Drive one complete batch of dependency resolution.
    ///
    /// `batch` contains `(NodeInfo, Option<CmTypes>)` pairs:
    /// - `Some(cm)` for network-injected packets (result must be stored).
    /// - `None` for compute tasks (result pre-stored by the worker).
    ///
    /// The strategy must call `process_batch_resolution` (or equivalent) and
    /// return [`BatchOutcome::Continue`] if any work was done, [`BatchOutcome::Empty`] otherwise.
    #[allow(clippy::too_many_arguments)]
    fn drive_batch(
        &self,
        shared: &Arc<SharedData>,
        batch: &mut Vec<(NodeInfo, Option<CmTypes>)>,
        thread_core: usize,
        thread_id: usize,
        thread_slot: usize,
        cond_indexes: &[Vec<usize>],
        stream_slot_activity: &mut HashMap<usize, bool>,
        start_ns: u128,
    ) -> BatchOutcome;

    /// Worker fast path: resolve successors directly from a worker thread.
    ///
    /// Called for `worker_resolvable` nodes (all successors non-condition).
    /// Default implementation returns `None`; override to eliminate the
    /// round-trip through the batch queue.
    fn worker_resolve(
        &self,
        shared: &Arc<SharedData>,
        sctx: &SchedCtx<'_>,
        node_info: &NodeInfo,
    ) -> Option<NodeInfo>;

    /// Tasks to accumulate before flushing to the scheduler.
    ///
    /// Exposed for documentation; the actual threshold used inside
    /// `process_batch_inner` is sourced from `BatchConfig::flush_threshold`.
    /// Override in custom strategies that bypass `process_batch_inner`.
    fn flush_threshold(&self) -> usize;

    /// Human-readable name for `--help` and diagnostics.
    fn name(&self) -> &'static str;
}

// ---------------------------------------------------------------------------
// MultiSlotBatchStrategy — the v1 default
// ---------------------------------------------------------------------------

/// Default resolution strategy: multi-slot batch protocol.
///
/// Wraps the existing [`super::resolution_loop::process_batch_resolution`] and
/// [`super::task_execution::worker_resolve_successors`] implementations
/// unchanged. This is a pure code-move with no behavioural delta.
///
/// Selected by `--resolution-strategy multi-slot-batch` (the default).
pub struct MultiSlotBatchStrategy;

impl ResolutionStrategy for MultiSlotBatchStrategy {
    fn drive_batch(
        &self,
        shared: &Arc<SharedData>,
        batch: &mut Vec<(NodeInfo, Option<CmTypes>)>,
        thread_core: usize,
        thread_id: usize,
        thread_slot: usize,
        cond_indexes: &[Vec<usize>],
        stream_slot_activity: &mut HashMap<usize, bool>,
        start_ns: u128,
    ) -> BatchOutcome {
        if batch.is_empty() {
            return BatchOutcome::Empty;
        }
        super::resolution_loop::process_batch_resolution(
            shared,
            batch,
            thread_core,
            thread_id,
            thread_slot,
            cond_indexes,
            stream_slot_activity,
            start_ns,
        );
        BatchOutcome::Continue
    }

    fn worker_resolve(
        &self,
        shared: &Arc<SharedData>,
        sctx: &SchedCtx<'_>,
        node_info: &NodeInfo,
    ) -> Option<NodeInfo> {
        super::task_execution::worker_resolve_successors(shared, sctx, node_info)
    }

    fn flush_threshold(&self) -> usize {
        // Matches the hard-coded flush_threshold in batch_resolution.rs
        // (the actual constant there is used directly by process_batch_inner).
        // Exposed here for documentation; v2 strategies can override.
        8
    }

    fn name(&self) -> &'static str {
        "multi-slot-batch"
    }
}
