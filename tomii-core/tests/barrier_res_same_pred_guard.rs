//! Regression tests for the two "grouped consumer, `$barrier` + `$res` sharing
//! a predecessor's dependency-counter group" stall shapes that used to make a
//! subset of a consumer node's instances never execute.
//!
//! History: PR #29 added `Graph::validate_barrier_res_same_pred_conflict`
//! (`tomii-core/src/graph.rs`) to *reject at load time* one of these shapes
//! (shape A below). A second, structurally different shape (shape B) was
//! later found live in the MIMO benchmark, which the PR #29 guard did not
//! (and, being a different failure mode, could not) catch — and the guard's
//! own suggested fix for shape A ("widen the `$res` to one index per
//! instance") is exactly shape B once combined with a grouped barrier.
//!
//! Both are now fixed at the root instead of rejected, so the load-time guard
//! has been removed: every shape below now loads AND runs to completion.
//!
//! - **Shape A**: node `C` has `group_size` set; a `$barrier` on predecessor
//!   `P` with no `group_by` (a genuine, more-than-one-instance fan-in); and a
//!   non-barrier `$res` on the SAME `P` with exactly one index. Root cause:
//!   `fetch_res_results`'s single-index path (`single_index_dep_idx` in
//!   `tomii-core/src/runtime/arg_resolution.rs`) unconditionally multiplied
//!   the declared index by a per-symbol grouping width even when `P` isn't
//!   actually produced in lockstep with `C`'s own groups, producing an
//!   out-of-range predecessor index for every group beyond the first — the
//!   requesting worker then spins forever waiting for a result that will
//!   never be stored. Fix: only apply that multiplication when the
//!   predecessor is genuinely grouped in lockstep with `C` (a real `group_by`
//!   resolved for this exact edge); otherwise the declared index is an
//!   absolute, broadcast value, identical for every group.
//! - **Shape B**: `C` has `group_size` set; a *grouped* `$barrier` on `P`
//!   (`group_by` matching `C`'s groups — the safe pattern shape A's rejection
//!   pointed users toward); and a `$res` on `P` widened to one declared index
//!   per `C` instance (`indexes.len() == C.factor`, `P.factor` different from
//!   `C.factor`). Two compounding bugs, both now fixed:
//!   1. *Value misattribution* (`per_instance_dep_idx` in `arg_resolution.rs`):
//!      the old code trusted the dispatch-triggering predecessor instance's
//!      own index (`pred_index`) as if it always belonged to `P` — but when
//!      `C` ALSO has an unrelated `$barrier` on a DIFFERENT predecessor `Q`
//!      sharing the same per-group dependency counter, a `Q` completion can
//!      be the specific decrement that tips a group's counter to zero, and
//!      `pred_index` then holds `Q`'s index, not `P`'s. Fixed by reading the
//!      declared `indexes[node_index]` directly whenever `P.factor !=
//!      C.factor` (not the true 1:1-fanout shape `pred_index` is valid for).
//!   2. *Premature dispatch* (`Graph::barrier_group_by_for_dependency` /
//!      `has_scattered_per_instance_res` in `tomii-core/src/graph.rs`, used by
//!      both `dependency_count_vec` and `runtime::init::build_predecessor_tables`):
//!      even with (1) fixed, a *grouped* `$barrier` on `P` only guarantees its
//!      own narrow `group_by`-wide slice of `P` is done before firing a
//!      group — nothing ties an arbitrary per-instance `$res` list's declared
//!      cells to that same slice. Confirmed live on MIMO's demul/fft/beam
//!      edges: a group's 24 instances can collectively declare cells spread
//!      across most of `fft`'s 52-wide range, so firing a group dispatches up
//!      to 24 simultaneous waits for not-yet-produced `fft` cells — with only
//!      4 workers configured, this exhausts the entire pool (every worker
//!      spinning, and the very `fft` tasks that would satisfy them stuck
//!      behind them in the queue with nobody free to run them): a true
//!      deadlock, confirmed live by zero further progress over 30+ seconds of
//!      extra run time. Fixed by detecting this shape and treating the shared
//!      predecessor's barrier as global/ungrouped FOR DEPENDENCY-COUNTING AND
//!      DISPATCH PURPOSES ONLY (every group waits for ALL of `P`, same
//!      already-correct mechanism used for `Q`'s own global barrier) — so a
//!      group never fires before every cell its instances could possibly
//!      declare is already produced. This only engages for the scattered
//!      shape (`indexes.len() == C.factor` with `P.factor != C.factor`
//!      alongside a grouped barrier on the same `P`); the common "single
//!      broadcast index + grouped barrier" case (demul/fft's actual MIMO
//!      usage) is untouched and keeps its per-group pipelining. (An earlier
//!      attempt — making the spinning worker call `rayon::yield_now()` to
//!      drain the pool's own queue while "waiting" — was rejected: the arg
//!      buffer being populated is a thread-local `RefCell` already borrowed
//!      for the duration of the current task, and recursing into another
//!      task on the same thread re-borrows it and panics.)
//!
//! These integration tests use the built-in `noop` function (always returns
//! `CmTypes::None`), so they can only observe *scheduling* correctness
//! (every frame completes, quickly, under `run_until`) — not per-instance
//! VALUE correctness, since `noop` has no distinguishable output. Value
//! correctness (every instance read exactly the predecessor cell its `$res`
//! declared) is verified separately, end-to-end, against the golden MIMO
//! reference output via `construct/harness/check.py` (see the task report).

use std::time::Duration;
use tomii_core::{
    graph_gen::from_json_str,
    runtime::{BatchConfig, RuntimeConfig, SpinWaitConfig, TomiiRtBuilder},
    scheduler::{create_scheduler, SchedulerConfig, SchedulerType},
};

fn make_scheduler(num_workers: usize) -> tomii_core::scheduler::SchedulerImpl {
    create_scheduler(SchedulerConfig {
        scheduler_type: SchedulerType::WorkStealing,
        core_offset: 0,
        num_workers,
        record: false,
        external_recorder: None,
        base_instant: std::time::Instant::now(),
        system_threads: 1,
        receiver_threads: 0,
        target_batch_size: 1,
        batch_timeout_us: 10,
        worker_affinity: None,
        worker_hook: None,
    })
}

/// Load `json`, run it for `max_frames` frames (or until `budget` elapses, whichever
/// first) with `num_workers` workers, and assert it actually finished all the frames
/// well within `budget` — i.e. it did not stall.
fn assert_runs_to_completion(json: &str, num_workers: usize, max_frames: usize, budget: Duration) {
    let spec =
        from_json_str(json, 4).unwrap_or_else(|e| panic!("expected this graph to load: {e}"));
    let scheduler = make_scheduler(num_workers);
    let compiled = spec.compile(&scheduler);
    let config = RuntimeConfig {
        slots: 4,
        max_frames,
        max_runtime: None,
        system_threads: 1,
        workers: num_workers,
        spin_wait: SpinWaitConfig {
            spin_iters: 32,
            yield_iters: 64,
            park_ns: 100,
        },
        batch: BatchConfig {
            target_size: 32,
            timeout_us: 10,
            poll_spin_iters: 16,
            flush_threshold: 8,
        },
        ..RuntimeConfig::default()
    };
    let mut rt = TomiiRtBuilder::with_config(compiled, scheduler, config)
        .build()
        .expect("build failed");
    let started = std::time::Instant::now();
    rt.run_until(move |progress| {
        progress.frames_completed >= max_frames || started.elapsed() > budget
    })
    .expect("run_until failed");
    let elapsed = started.elapsed();
    assert!(
        elapsed < budget,
        "expected {max_frames} frame(s) to complete within {budget:?}, but the run was still \
         going at {elapsed:?} (frames never completed is exactly the stall this test guards \
         against)"
    );
}

fn assert_loads(json: &str) {
    from_json_str(json, 4).unwrap_or_else(|e| panic!("expected this graph to load: {e}"));
}

// --------------------------------------------------------------------------- //
// Shape A: single-index $res + ungrouped $barrier on the same predecessor,
// node has group_size. Used to be rejected at load time; now loads AND runs.
// --------------------------------------------------------------------------- //

#[test]
fn shape_a_runs_to_completion() {
    // This is the exact shape the old guard rejected (`rejects_without_an_unrelated_second_barrier`):
    // minimal, no unrelated second predecessor.
    assert_runs_to_completion(
        r#"
        {
            "nodes": [
                { "name": "p", "factor": 8, "function": "noop", "args": [] },
                {
                    "name": "c",
                    "factor": 12,
                    "group_size": 3,
                    "function": "noop",
                    "args": [
                        { "type": "$res", "predecessor": { "name": "p", "indexes": "0" } },
                        { "type": "$barrier", "predecessor": { "name": "p", "indexes": "0-7" } }
                    ]
                }
            ]
        }
        "#,
        4,
        5,
        Duration::from_secs(3),
    );
}

#[test]
fn shape_a_runs_to_completion_with_an_unrelated_second_barrier() {
    // Same conflict, with an unrelated second $barrier predecessor present
    // (mirrors the shape of the real MIMO graph, which always has >1
    // predecessor feeding the consumer).
    assert_runs_to_completion(
        r#"
        {
            "nodes": [
                { "name": "p", "factor": 8, "function": "noop", "args": [] },
                { "name": "beam", "factor": 2, "function": "noop", "args": [] },
                {
                    "name": "c",
                    "factor": 12,
                    "group_size": 3,
                    "function": "noop",
                    "args": [
                        { "type": "$res", "predecessor": { "name": "p", "indexes": "0" } },
                        { "type": "$barrier", "predecessor": { "name": "p", "indexes": "0-7" } },
                        { "type": "$barrier", "predecessor": { "name": "beam", "indexes": "0-1" } }
                    ]
                }
            ]
        }
        "#,
        4,
        5,
        Duration::from_secs(3),
    );
}

// --------------------------------------------------------------------------- //
// Shape B: per-instance $res list on predecessor P (grouped $barrier, matching
// C's own groups) + a SEPARATE predecessor Q with an ungrouped/global $barrier
// sharing the same dependency-counter group. A deterministic single-worker
// schedule (serial dispatch: all of P's instances run, then all of Q's) makes
// Q's lone completion the specific decrement that tips every one of C's group
// counters to zero, so `pred_index` deterministically carries Q's own index,
// not P's, exercising the exact misattribution mechanism behind the live MIMO
// stall. With `noop` (always `CmTypes::None`, and here Q's index happens to
// coincide with an already-produced P instance) this does not reproduce an
// actual hang or a detectably wrong value — both of those require the live
// MIMO reproduction in the task report (demul reads real per-antenna FFT
// buffers, and a mis-dispatch there can starve every worker at once). This
// test's job is to prove the fixed dep_idx computation is correct regardless
// of which predecessor tipped the counter, which the `per_instance_dep_idx`
// unit tests above (`arg_resolution.rs`) already show directly; this is the
// scheduling-level companion, confirming nothing upstream of that function
// breaks under the real counter-sharing arithmetic. Here p's own factor (8)
// equals c's group width * num_groups incidentally but not p.factor itself
// (8 != 12), so the "premature dispatch" fix (`barrier_group_by_for_dependency`,
// `graph.rs`) also applies: p's barrier is counted as global here too (every
// group waits for all 8 of p), which is exercised directly by the
// `scattered_per_instance_res_forces_global_barrier_dependency` unit test in
// `graph.rs`.
// --------------------------------------------------------------------------- //

#[test]
fn shape_b_runs_to_completion_when_an_unrelated_global_barrier_is_the_tipping_decrement() {
    // p: factor 8, grouped barrier (group_by=2) matching c's 4 groups of 3 —
    //    contributes 2 decrements per group (8 total).
    // q: factor 1, ungrouped/global barrier — contributes 1 decrement to
    //    EVERY group at once (4 total, 1 per group). With p dispatched (and,
    //    single-worker, fully executed) before q, p's 2-per-group share is
    //    always exhausted first, so q's one global completion is always the
    //    decrement that zeroes every group's counter — pred_index at that
    //    moment is q's own index (0), not a p index.
    // c: factor 12, group_size 3. $res on p is a per-instance list (length 12,
    //    cycling p's 8 indices) — pred_factor(8) != node_factor(12), so this
    //    is NOT the "pred_factor == node_factor" true 1:1 edge; it must read
    //    its own declared index, not pred_index.
    assert_runs_to_completion(
        r#"
        {
            "nodes": [
                { "name": "p", "factor": 8, "function": "noop", "args": [] },
                { "name": "q", "factor": 1, "function": "noop", "args": [] },
                {
                    "name": "c",
                    "factor": 12,
                    "group_size": 3,
                    "function": "noop",
                    "args": [
                        { "type": "$res", "predecessor": { "name": "p", "indexes": "0,1,2,3,4,5,6,7,0,1,2,3" } },
                        { "type": "$barrier", "predecessor": { "name": "p", "indexes": "0-7", "group_by": 2 } },
                        { "type": "$barrier", "predecessor": { "name": "q", "indexes": "0" } }
                    ]
                }
            ]
        }
        "#,
        1,
        50,
        Duration::from_secs(5),
    );
}

#[test]
fn shape_b_runs_to_completion_with_multiple_workers() {
    // Same shape as above but at the actual MIMO demul/fft/beam scale (52 fft
    // outputs, 312 demul instances, 24 per group, grouped by 4 antennas, plus
    // an unrelated global "beam" barrier) and the SAME worker count the real
    // benchmark's run.sh uses (4) — the exact configuration that deadlocked
    // live before this fix (see the task report for the `check.py` run).
    let indexes: Vec<String> = (0..312usize).map(|i| (i % 52).to_string()).collect();
    let json = format!(
        r#"
        {{
            "nodes": [
                {{ "name": "fft", "factor": 52, "function": "noop", "args": [] }},
                {{ "name": "beam", "factor": 13, "function": "noop", "args": [] }},
                {{
                    "name": "demul",
                    "factor": 312,
                    "group_size": 24,
                    "function": "noop",
                    "args": [
                        {{ "type": "$res", "predecessor": {{ "name": "fft", "indexes": "{indexes}" }} }},
                        {{ "type": "$barrier", "predecessor": {{ "name": "fft", "indexes": "0-51", "group_by": 4 }} }},
                        {{ "type": "$barrier", "predecessor": {{ "name": "beam", "indexes": "0-12" }} }}
                    ]
                }}
            ]
        }}
        "#,
        indexes = indexes.join(",")
    );
    assert_runs_to_completion(&json, 4, 50, Duration::from_secs(5));
}

// --------------------------------------------------------------------------- //
// Working siblings: each ingredient of shape A/B, removed or altered one at a
// time, already worked before this fix and must keep working identically.
// --------------------------------------------------------------------------- //

#[test]
fn accepts_and_runs_grouped_barrier_matching_consumer_groups() {
    // group_by on the barrier (matching the MIMO reference graph's beam/csi and
    // demul/fft edges) — the pre-existing correct path, untouched by this fix.
    assert_runs_to_completion(
        r#"
        {
            "nodes": [
                { "name": "p", "factor": 8, "function": "noop", "args": [] },
                {
                    "name": "c",
                    "factor": 12,
                    "group_size": 3,
                    "function": "noop",
                    "args": [
                        { "type": "$res", "predecessor": { "name": "p", "indexes": "0" } },
                        { "type": "$barrier", "predecessor": { "name": "p", "indexes": "0-7", "group_by": 2 } }
                    ]
                }
            ]
        }
        "#,
        4,
        5,
        Duration::from_secs(3),
    );
}

#[test]
fn accepts_and_runs_no_group_size() {
    assert_runs_to_completion(
        r#"
        {
            "nodes": [
                { "name": "p", "factor": 8, "function": "noop", "args": [] },
                {
                    "name": "c",
                    "factor": 12,
                    "function": "noop",
                    "args": [
                        { "type": "$res", "predecessor": { "name": "p", "indexes": "0" } },
                        { "type": "$barrier", "predecessor": { "name": "p", "indexes": "0-7" } }
                    ]
                }
            ]
        }
        "#,
        4,
        5,
        Duration::from_secs(3),
    );
}

#[test]
fn accepts_and_runs_dep_instead_of_res() {
    assert_runs_to_completion(
        r#"
        {
            "nodes": [
                { "name": "p", "factor": 8, "function": "noop", "args": [] },
                {
                    "name": "c",
                    "factor": 12,
                    "group_size": 3,
                    "function": "noop",
                    "args": [
                        { "type": "$dep", "predecessor": { "name": "p", "indexes": "0" } },
                        { "type": "$barrier", "predecessor": { "name": "p", "indexes": "0-7" } }
                    ]
                }
            ]
        }
        "#,
        4,
        5,
        Duration::from_secs(3),
    );
}

#[test]
fn accepts_res_index_count_neither_one_nor_factor() {
    // n=2: neither the "single broadcast value" nor the "one per instance" shape.
    // Collect-all path, untouched by this fix — still just loads/runs fine.
    assert_runs_to_completion(
        r#"
        {
            "nodes": [
                { "name": "p", "factor": 8, "function": "noop", "args": [] },
                {
                    "name": "c",
                    "factor": 12,
                    "group_size": 3,
                    "function": "noop",
                    "args": [
                        { "type": "$barrier", "predecessor": { "name": "p", "indexes": "0-7" } },
                        { "type": "$res", "predecessor": { "name": "p", "indexes": "0,1" } }
                    ]
                }
            ]
        }
        "#,
        4,
        5,
        Duration::from_secs(3),
    );
}

#[test]
fn accepts_trivial_single_instance_barrier() {
    assert_loads(
        r#"
        {
            "nodes": [
                { "name": "p", "factor": 1, "function": "noop", "args": [] },
                {
                    "name": "c",
                    "factor": 12,
                    "group_size": 3,
                    "function": "noop",
                    "args": [
                        { "type": "$res", "predecessor": { "name": "p", "indexes": "0" } },
                        { "type": "$barrier", "predecessor": { "name": "p", "indexes": "0" } }
                    ]
                }
            ]
        }
        "#,
    );
}
