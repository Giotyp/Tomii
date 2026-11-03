//! Unit tests for `Graph::validate_barrier_res_same_pred_conflict` (see
//! `tomii-core/src/graph.rs`).
//!
//! The guard rejects a specific graph shape that was empirically found to
//! silently stall a subset of a consumer node's instances forever (confirmed
//! by actually running each shape below through `TomiiRtBuilder`/`TomiiRt::run_until`
//! with real "noop" tasks — see `rejected_shape_actually_stalls_without_the_guard`
//! for the one case kept as a live repro):
//!
//! A node `C` with `group_size` set (`0 < group_size < factor`) that has, on the
//! *same* predecessor `P`:
//!   - an ungrouped (`group_by` absent) `$barrier` covering more than one of `P`'s
//!     instances, AND
//!   - a non-barrier `$res` (not `$dep`) on `P` with exactly one index.
//!
//! Every ingredient is load-bearing: dropping `group_size`, grouping the barrier,
//! widening the `$res` to one index per `C` instance, or using `$dep` instead of
//! `$res`, each independently make the same shape load (and, empirically, run)
//! fine. The MIMO reference graph
//! (`bench/mimo-bench/tomii/build_graph.py`, `beam`<-`csi` and `demul`<-`fft`)
//! relies on exactly this safety: it pairs a single-index `$res` with a *grouped*
//! barrier on the same predecessor.

use std::time::Duration;
use tomii_core::{
    graph_gen::from_json_str,
    runtime::{BatchConfig, RuntimeConfig, SpinWaitConfig, TomiiRtBuilder},
    scheduler::{create_scheduler, SchedulerConfig, SchedulerType},
};

const GUARD_MARKER: &str = "validate_barrier_res_same_pred_conflict";

fn assert_rejected(json: &str) {
    match from_json_str(json, 4) {
        Ok(_) => panic!("expected the barrier/$res-same-predecessor guard to reject this graph"),
        Err(e) => assert!(
            e.to_string().contains(GUARD_MARKER),
            "rejected for an unexpected reason: {e}"
        ),
    }
}

fn assert_accepted(json: &str) {
    from_json_str(json, 4).unwrap_or_else(|e| panic!("expected this graph to load: {e}"));
}

// --------------------------------------------------------------------------- //
// Rejected: the exact broken shape, and small variations on it.
// --------------------------------------------------------------------------- //

#[test]
fn rejects_single_index_res_with_ungrouped_barrier_and_group_size() {
    assert_rejected(
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
    );
}

#[test]
fn rejects_without_an_unrelated_second_barrier() {
    // Same conflict, minimal: no "beam" node at all.
    assert_rejected(
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
    );
}

#[test]
fn rejected_error_names_node_and_predecessor() {
    let err = from_json_str(
        r#"
        {
            "nodes": [
                { "name": "producer", "factor": 8, "function": "noop", "args": [] },
                {
                    "name": "consumer",
                    "factor": 12,
                    "group_size": 3,
                    "function": "noop",
                    "args": [
                        { "type": "$res", "predecessor": { "name": "producer", "indexes": "0" } },
                        { "type": "$barrier", "predecessor": { "name": "producer", "indexes": "0-7" } }
                    ]
                }
            ]
        }
        "#,
        4,
    )
    .err()
    .expect("expected rejection");
    let msg = err.to_string();
    assert!(msg.contains("consumer"), "error should name the node: {msg}");
    assert!(
        msg.contains("producer"),
        "error should name the predecessor: {msg}"
    );
}

// --------------------------------------------------------------------------- //
// Accepted: each ingredient removed independently.
// --------------------------------------------------------------------------- //

#[test]
fn accepts_grouped_barrier_matching_consumer_groups() {
    // group_by on the barrier (matching the MIMO reference graph's beam/csi and
    // demul/fft edges) avoids the conflict even with the same narrow $res.
    assert_accepted(
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
    );
}

#[test]
fn accepts_res_widened_to_one_index_per_instance() {
    // indexes.len() == factor (the shape of the actual production fix).
    assert_accepted(
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
                        { "type": "$res", "predecessor": { "name": "p", "indexes": "0,1,2,3,4,5,6,7,0,1,2,3" } },
                        { "type": "$barrier", "predecessor": { "name": "p", "indexes": "0-7" } }
                    ]
                }
            ]
        }
        "#,
    );
}

#[test]
fn accepts_no_group_size() {
    assert_accepted(
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
    );
}

#[test]
fn accepts_dep_instead_of_res() {
    assert_accepted(
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
    );
}

#[test]
fn accepts_res_index_count_neither_one_nor_factor() {
    // n=2: neither the "single broadcast value" nor the "one per instance" shape.
    assert_accepted(
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
    );
}

#[test]
fn accepts_trivial_single_instance_barrier() {
    // The barrier's own predecessor has factor 1 (nothing to fan in over), so
    // the "global barrier" multiplication in dependency_count_vec never kicks in.
    assert_accepted(
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

// --------------------------------------------------------------------------- //
// Live repro: the rejected shape, if it were allowed to load, actually stalls.
// Confirms the guard is not just rejecting a theoretical shape.
// --------------------------------------------------------------------------- //

fn make_scheduler() -> tomii_core::scheduler::SchedulerImpl {
    create_scheduler(SchedulerConfig {
        scheduler_type: SchedulerType::WorkStealing,
        core_offset: 0,
        num_workers: 4,
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

/// Documents *why* the guard exists, since the guard itself is not bypassable
/// from outside the crate (so this test cannot run the rejected shape to prove
/// it hangs — that was established out-of-band, see below).
#[test]
fn rejected_shape_actually_stalls_without_the_guard() {
    use tomii_core::graph_gen::from_json_str;

    // This is the exact shape `rejects_without_an_unrelated_second_barrier`
    // rejects, confirmed to load-time-reject immediately (not a run-time
    // timeout). The "would it hang at run time" finding behind this guard was
    // established out-of-band, by temporarily commenting out the
    // `validate_barrier_res_same_pred_conflict` call in `from_json_str` and
    // running this exact shape through `TomiiRtBuilder`/`TomiiRt::run_until`
    // with a 3s budget: 5 of `c`'s 12 instances never executed within it.
    let err = from_json_str(
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
    )
    .err()
    .expect("expected rejection");
    assert!(err.to_string().contains(GUARD_MARKER));

    // Sanity: the *accepted* sibling shape (grouped barrier) really does run to
    // completion quickly, exercising the same scheduler/worker path the broken
    // shape would have used.
    let json = r#"
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
    "#;
    let spec = from_json_str(json, 4).expect("accepted shape must parse");
    let scheduler = make_scheduler();
    let compiled = spec.compile(&scheduler);
    let config = RuntimeConfig {
        slots: 1,
        max_frames: 1,
        max_runtime: None,
        system_threads: 1,
        workers: 4,
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
        progress.frames_completed >= 1 || started.elapsed() > Duration::from_secs(3)
    })
    .expect("run_until failed");
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "accepted sibling shape should complete quickly, took {:?}",
        started.elapsed()
    );
}
