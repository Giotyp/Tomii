//! Regression tests for the "same predecessor referenced through more than one
//! arg, with DIFFERENT index sets" dependency-counting/routing bug.
//!
//! # Root cause
//!
//! A consumer node can read a single predecessor through more than one arg —
//! e.g. a `$res` on predecessor instance 0 AND a `$barrier` on instance 1 of a
//! factor-2 predecessor (the QEC leaf/fuse shape this was found in: `fuse_2`
//! reads `leaf[0]`'s *value* via `$res` and only needs `leaf[1]` to have *run*,
//! via `$barrier`). Both `Graph::dependency_count_vec` (`tomii-core/src/graph.rs`)
//! and `runtime::init::build_predecessor_tables` (`tomii-core/src/runtime/init.rs`)
//! used a `preds_seen`-style "first/last arg wins" dedup keyed only by
//! predecessor ID: whichever arg was visited first (counting side) or last
//! (routing side) had its own, narrower index set used for BOTH the
//! dependency-counter threshold AND the predecessor→successor routing filter —
//! silently dropping any instance that only a DIFFERENT arg on the same
//! predecessor referenced. In the leaf/fuse shape, the `$barrier` (processed
//! first in the counting loop, and visited last in the routing loop) declares
//! only index 1, so the consumer's threshold dropped to 1 (needing only
//! `leaf[1]`) and its routing filter narrowed to `[1, 2)` (so `leaf[0]`'s
//! completion was never even routed to it). The consumer could then become
//! ready, and read its `$res` on `leaf[0]`, before `leaf[0]` had run at all —
//! at `workers=1` this is a true deadlock (the single worker spins forever in
//! the consumer's `fetch_res_results` waiting for a result only a worker it
//! is occupying could ever produce); at `workers>1` it is a live race that
//! merely usually goes unnoticed.
//!
//! The same dedup-by-predecessor-id pattern applies identically to a plain
//! `$res` + `$res` (or `$res` + `$dep`) pair on the same predecessor with
//! different index sets and no barrier at all — there is nothing barrier-
//! specific about the bug.
//!
//! # Fix
//!
//! Both files now compute, for every predecessor a node reads through more
//! than one arg, the UNION of index sets declared across ALL of that
//! predecessor's args (see `Graph::union_pred_indexes`) and use that union for
//! both the dependency count and the routing filter — so a consumer is only
//! ever ready once every instance ANY of its args needs has completed. The
//! common single-arg-per-predecessor path (by far the most frequent shape) is
//! left byte-for-byte on its original computation.
//!
//! # What these tests check (and what they deliberately don't)
//!
//! Like `barrier_res_same_pred_guard.rs`, these tests use the built-in `noop`
//! function and `run_until`'s elapsed-time budget, so they can only observe
//! *scheduling* completion directly — not per-instance VALUE correctness
//! (`noop` always returns `CmTypes::None`). `run_until`'s predicate closure
//! runs on the calling thread, independent of the worker pool, so a stuck
//! run is observed as "elapsed exceeded budget without reaching `max_frames`"
//! rather than an actual test-process hang.
//!
//! This is still a strong, *empirically confirmed* regression signal: with
//! the pre-fix code checked out, `res_barrier_overlapping_index_on_same_pred_completes`
//! reliably times out (a true, deterministic hang — not a transient race) at
//! `workers=2`, because the routing side's bug (`filter[succ][pred]` only
//! ever gets OVERWRITTEN when an arg's own `index_filter` call returns
//! `Some(..)`, so a later unfiltered/full-range arg on the same predecessor
//! — which legitimately returns `None`, "no filter" — silently leaves the
//! EARLIER, narrower arg's filter in place) permanently starves the
//! consumer's dependency counter: only predecessor instance 0 ever passes
//! the (wrongly narrow) filter and decrements it, so a 3-decrement threshold
//! is never reached. This was confirmed by re-running this exact test suite
//! against the pre-fix `graph.rs`/`init.rs` (via a temporary `git stash`) —
//! 8 of these 9 tests happen to still pass pre-fix (this minimal 2-3 node
//! topology's deterministic single/few-worker dispatch order coincidentally
//! still runs `leaf[0]` before `leaf[1]`/`fuse_2` in those particular
//! shapes), but `res_barrier_overlapping_index_on_same_pred_completes` does
//! not, directly proving the routing-side union fix is necessary, not just
//! defensive.
//!
//! True per-instance VALUE-level ordering proof (the consumer's `$res` read
//! for `leaf[0]` never observing a result produced before `leaf[0]`'s own run)
//! needs real kernels, real compute time (to open the race window), and
//! enough topology for scheduling order to not coincidentally save a wrong
//! threshold — exactly what `noop` plus a 2-3 node graph cannot provide. That
//! is covered separately, end-to-end, by the plugin-independent repro at
//! `qec/p2/minrepro2/` (see the task report), which prints an `eprintln!`
//! from each real kernel invocation; rerun at `workers=1` before/after this
//! fix, it directly shows the pre-fix hang (the sole worker spinning in
//! `fetch_res_results` for a `leaf` instance that will never run) and,
//! post-fix, `leaf_solve` for every instance completing before the `fuse_unit`
//! that reads it.

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
/// first) with `num_workers` workers, and assert it actually finished ALL the frames,
/// with no stale-task drops, well within `budget` — i.e. it did not stall, and no
/// instance was silently skipped. This is exactly
/// `barrier_res_same_pred_guard.rs::assert_runs_to_completion`, plus the stale-drop
/// check (a non-zero count would mean some instance's work was discarded rather than
/// run, which "ran exactly once per frame" must rule out).
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
    let final_stale_drops = std::cell::Cell::new(0usize);
    rt.run_until(|progress| {
        final_stale_drops.set(progress.stale_drops);
        progress.frames_completed >= max_frames || started.elapsed() > budget
    })
    .expect("run_until failed");
    let elapsed = started.elapsed();
    assert!(
        elapsed < budget,
        "expected {max_frames} frame(s) to complete within {budget:?}, but the run was still \
         going at {elapsed:?} — this is exactly the deadlock the same-predecessor \
         dependency-counting bug caused (a consumer became ready, and spun waiting for a \
         result, before its producer instance had run)"
    );
    assert_eq!(
        final_stale_drops.get(),
        0,
        "expected every instance to run exactly once per frame — a stale-task drop means \
         work was discarded instead of executed"
    );
}

// --------------------------------------------------------------------------- //
// The reported bug, in its minimal plugin-independent shape: a factor-1
// consumer ("fuse_2") reads a factor-2 predecessor ("leaf") through a `$res`
// on instance 0 AND a `$barrier` on instance 1 — exactly the QEC leaf/fuse
// shape from the task report (and the standalone repro at
// qec/p2/minrepro2/graph_p2.json).
// --------------------------------------------------------------------------- //

fn leaf_fuse_root_json() -> &'static str {
    r#"
    {
        "nodes": [
            { "name": "leaf", "factor": 2, "function": "noop", "args": [] },
            {
                "name": "fuse_2",
                "factor": 1,
                "function": "noop",
                "args": [
                    { "type": "$res", "predecessor": { "name": "leaf", "indexes": "0" } },
                    { "type": "$barrier", "predecessor": { "name": "leaf", "indexes": "1" } }
                ]
            },
            {
                "name": "root_extract",
                "factor": 1,
                "function": "noop",
                "args": [
                    { "type": "$res", "predecessor": { "name": "fuse_2", "indexes": "0" } }
                ]
            }
        ]
    }
    "#
}

#[test]
fn leaf_fuse_repro_completes_workers_1() {
    // The deadlocking shape from the task report: if fuse_2 becomes ready
    // before leaf[0] has run, a single worker spins forever in
    // fetch_res_results waiting for a result only a free worker — of which
    // there are none — could ever produce. This exact minimal topology
    // happens to still complete pre-fix at workers=1 (this graph's
    // deterministic single-worker FIFO dispatch runs leaf[0] before leaf[1]
    // regardless of the wrong threshold); the real QEC graph's larger
    // topology does not get that coincidence and hangs for real (see the
    // module doc and the task report's `qec/p2/minrepro2/` rerun). This test
    // still matters post-fix: workers=1,2,4 must all keep completing exactly
    // once per frame with the corrected, union-based counting/routing.
    assert_runs_to_completion(leaf_fuse_root_json(), 1, 20, Duration::from_secs(5));
}

#[test]
fn leaf_fuse_repro_completes_workers_2() {
    assert_runs_to_completion(leaf_fuse_root_json(), 2, 20, Duration::from_secs(5));
}

#[test]
fn leaf_fuse_repro_completes_workers_4() {
    assert_runs_to_completion(leaf_fuse_root_json(), 4, 20, Duration::from_secs(5));
}

// --------------------------------------------------------------------------- //
// Generalizing beyond $res + $barrier: the same predecessor read through two
// $res args with DIFFERENT index sets and no barrier at all. A pure
// `preds_seen` dedup (as both files had) drops one of the two reads from the
// dependency count/routing filter just the same — there is nothing
// barrier-specific about this bug.
// --------------------------------------------------------------------------- //

#[test]
fn res_res_differing_indexes_on_same_pred_completes() {
    assert_runs_to_completion(
        r#"
        {
            "nodes": [
                { "name": "p", "factor": 2, "function": "noop", "args": [] },
                {
                    "name": "c",
                    "factor": 1,
                    "function": "noop",
                    "args": [
                        { "type": "$res", "predecessor": { "name": "p", "indexes": "0" } },
                        { "type": "$res", "predecessor": { "name": "p", "indexes": "1" } }
                    ]
                }
            ]
        }
        "#,
        1,
        20,
        Duration::from_secs(5),
    );
}

// --------------------------------------------------------------------------- //
// $res + $dep on the same predecessor with different indexes: $dep carries no
// value (pure ordering edge) but must still contribute its own instance to
// the union.
// --------------------------------------------------------------------------- //

#[test]
fn res_dep_differing_indexes_on_same_pred_completes() {
    assert_runs_to_completion(
        r#"
        {
            "nodes": [
                { "name": "p", "factor": 2, "function": "noop", "args": [] },
                {
                    "name": "c",
                    "factor": 1,
                    "function": "noop",
                    "args": [
                        { "type": "$res", "predecessor": { "name": "p", "indexes": "0" } },
                        { "type": "$dep", "predecessor": { "name": "p", "indexes": "1" } }
                    ]
                }
            ]
        }
        "#,
        1,
        20,
        Duration::from_secs(5),
    );
}

// --------------------------------------------------------------------------- //
// Three distinct args ($res, $res, $barrier), three distinct indexes, no
// overlap, pred_factor > 2 — the union must still cover every one of them,
// not just whichever two the old per-predecessor dedup happened to keep.
// --------------------------------------------------------------------------- //

#[test]
fn three_args_three_distinct_indexes_on_same_pred_completes() {
    assert_runs_to_completion(
        r#"
        {
            "nodes": [
                { "name": "p", "factor": 3, "function": "noop", "args": [] },
                {
                    "name": "c",
                    "factor": 1,
                    "function": "noop",
                    "args": [
                        { "type": "$res", "predecessor": { "name": "p", "indexes": "0" } },
                        { "type": "$res", "predecessor": { "name": "p", "indexes": "1" } },
                        { "type": "$barrier", "predecessor": { "name": "p", "indexes": "2" } }
                    ]
                }
            ]
        }
        "#,
        1,
        20,
        Duration::from_secs(5),
    );
}

// --------------------------------------------------------------------------- //
// Overlapping (not merely differing) index sets on the same predecessor must
// still be deduplicated by the union, not double-counted (which would make
// the consumer wait forever for more decrements than it can ever receive) —
// the integration-level companion to the `test_dep_count_same_pred_twice_deduped`
// unit test.
// --------------------------------------------------------------------------- //

#[test]
fn res_barrier_overlapping_index_on_same_pred_completes() {
    assert_runs_to_completion(
        r#"
        {
            "nodes": [
                { "name": "p", "factor": 3, "function": "noop", "args": [] },
                {
                    "name": "c",
                    "factor": 1,
                    "function": "noop",
                    "args": [
                        { "type": "$res", "predecessor": { "name": "p", "indexes": "0" } },
                        { "type": "$barrier", "predecessor": { "name": "p", "indexes": "0-2" } }
                    ]
                }
            ]
        }
        "#,
        2,
        20,
        Duration::from_secs(5),
    );
}

// --------------------------------------------------------------------------- //
// The same shape, but interacting with group_size (PR #33 territory): a
// grouped consumer whose per-group barrier is NOT itself scattered, plus an
// unrelated second arg on the same predecessor with a different index. Must
// still wait for the union; the existing grouped-barrier pipelining for the
// untouched shapes (`barrier_res_same_pred_guard.rs`) must keep passing
// unmodified (checked by the full suite, not duplicated here).
// --------------------------------------------------------------------------- //

#[test]
fn res_barrier_differing_indexes_with_group_size_completes() {
    assert_runs_to_completion(
        r#"
        {
            "nodes": [
                { "name": "p", "factor": 4, "function": "noop", "args": [] },
                {
                    "name": "c",
                    "factor": 4,
                    "group_size": 2,
                    "function": "noop",
                    "args": [
                        { "type": "$res", "predecessor": { "name": "p", "indexes": "0" } },
                        { "type": "$barrier", "predecessor": { "name": "p", "indexes": "0-3" } }
                    ]
                }
            ]
        }
        "#,
        2,
        20,
        Duration::from_secs(5),
    );
}

// --------------------------------------------------------------------------- //
// Working siblings: a single arg per predecessor (by far the most common
// shape — the 1:1/collect-all/grouped-barrier paths this fix leaves
// untouched) must keep working identically. `barrier_res_same_pred_guard.rs`
// already covers the grouped-barrier variants in detail; this just adds the
// plain single-$res sibling of the leaf/fuse shape above (no second arg on
// the predecessor at all).
// --------------------------------------------------------------------------- //

#[test]
fn single_res_arg_per_predecessor_completes() {
    assert_runs_to_completion(
        r#"
        {
            "nodes": [
                { "name": "leaf", "factor": 2, "function": "noop", "args": [] },
                {
                    "name": "fuse_2",
                    "factor": 1,
                    "function": "noop",
                    "args": [
                        { "type": "$barrier", "predecessor": { "name": "leaf", "indexes": "0-1" } }
                    ]
                }
            ]
        }
        "#,
        1,
        20,
        Duration::from_secs(5),
    );
}

// --------------------------------------------------------------------------- //
// Follow-up regression: the fix above (union the index sets across a
// predecessor's args) initially disabled the 1:1 exact-dispatch offset for
// EVERY predecessor referenced by more than one arg — including two
// IDENTICAL args (`mat_mul`'s `[vec_mat.out(), vec_mat.out()]` in
// `examples/matrix-compute`, found by that example's CI check, not by this
// suite: `cargo test` resolves every plugin function to `noop`, so it cannot
// observe a wrong-VALUE regression — only a hang/non-completion, and this
// specific regression's usual symptom at the example's actual scale (200
// instances, 2 workers) was a real deadlock, confirmed by reproducing it
// locally). Identical duplicate args impose exactly what a single copy would
// and must take the exact single-arg path, 1:1 offset included — see
// `pred_is_ambiguous_multi_arg` / `node_has_ambiguous_multi_res_args` in
// `graph_struct.rs`. VALUE-level proof that this is actually fixed (every
// instance reads the predecessor cell it declared, not merely that the run
// completes) lives in `same_pred_multi_arg_values.rs`, which builds and runs
// a real plugin instead of `noop`.
// --------------------------------------------------------------------------- //

#[test]
fn identical_duplicate_res_args_equal_factor_completes() {
    // Mirrors mat_mul: two IDENTICAL $res args on an equal-factor predecessor,
    // factor large enough (and workers small enough) to matter if the 1:1
    // exact-dispatch offset were disabled for this shape.
    assert_runs_to_completion(
        r#"
        {
            "nodes": [
                { "name": "p", "factor": 64, "function": "noop", "args": [] },
                {
                    "name": "c",
                    "factor": 64,
                    "function": "noop",
                    "args": [
                        { "type": "$res", "predecessor": { "name": "p", "indexes": "0" } },
                        { "type": "$res", "predecessor": { "name": "p", "indexes": "0" } }
                    ]
                }
            ]
        }
        "#,
        2,
        20,
        Duration::from_secs(5),
    );
}

// --------------------------------------------------------------------------- //
// The OTHER new-regression risk flagged alongside the identical-args fix:
// two DIFFERING, barrier-free $res args on the same equal-factor predecessor
// (e.g. offsets "0" and "1" — c[j] needs BOTH p[j] and p[(j+1) % factor]).
// With no literal $barrier, this node's dependency counter would otherwise
// fall through to the 1:1-offset (which can only ever encode ONE of the two
// offsets) or ordinal-threshold (arrival-order, not identity-matched) paths —
// neither safe for a genuine two-cell-per-instance requirement. This must
// force the same "wait for the full predecessor factor, release every
// instance together" treatment a literal barrier already gets
// (`node_has_ambiguous_multi_res_args`, consumed by
// `buffers::node_dep::NodeDepMap::new`).
//
// (Not "0" and "-1": the JSON predecessor-index grammar
// (`graph_gen.rs::parse_predecessor`) treats any `-`-containing index string
// as a `start-end` range, so a bare single negative offset can't currently be
// expressed in JSON — a separate, pre-existing limitation unrelated to this
// fix. "0"/"1" exercises the identical mechanism without depending on it.)
// --------------------------------------------------------------------------- //

#[test]
fn differing_nonidentical_res_args_equal_factor_completes() {
    assert_runs_to_completion(
        r#"
        {
            "nodes": [
                { "name": "p", "factor": 64, "function": "noop", "args": [] },
                {
                    "name": "c",
                    "factor": 64,
                    "function": "noop",
                    "args": [
                        { "type": "$res", "predecessor": { "name": "p", "indexes": "0" } },
                        { "type": "$res", "predecessor": { "name": "p", "indexes": "1" } }
                    ]
                }
            ]
        }
        "#,
        2,
        20,
        Duration::from_secs(5),
    );
}
