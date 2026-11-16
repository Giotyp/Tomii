//! End-to-end runtime tests for explicit per-instance dependency maps
//! (`instance_map` on predecessor edges — see `tomii_core::instance_map`).
//!
//! # Methodology
//!
//! These tests use the built-in `noop` kernel (every node function resolves to
//! a no-op in `tomii-core`'s own test builds — see `runtime::node_cache::resolve_func`),
//! exactly like the established precedent for this problem class,
//! `same_pred_multi_arg_dep_count.rs` (same-predecessor multi-arg routing) and
//! `barrier_res_same_pred_guard.rs`. `noop` cannot record per-instance execution
//! order itself, so correctness is observed the same way those suites observe
//! it: the run must complete every declared frame, with zero stale-task drops,
//! well within a generous time budget — a wrong threshold (too high) or a
//! dropped routing entry manifests as a deterministic HANG (a consumer never
//! reaches its threshold, starving the worker pool spinning for a result that
//! will never arrive — see `same_pred_multi_arg_dep_count.rs`'s module docs for
//! the exact mechanism), and a wrong (too low) threshold or a double-routed
//! decrement manifests as an extra/duplicate dispatch, caught by the
//! stale-drop/generation-mismatch counters. True per-instance kernel-recorded
//! ordering needs a real plugin (out of scope for `tomii-core`'s in-crate test
//! harness) — see the same precedent's module docs for why.

use std::time::Duration;
use tomii_core::{
    graph_gen::from_json_str,
    runtime::{BatchConfig, RuntimeConfig, SpinWaitConfig, TomiiRtBuilder},
    scheduler::{create_scheduler, SchedulerConfig, SchedulerType},
};

fn make_scheduler(
    scheduler_type: SchedulerType,
    num_workers: usize,
) -> tomii_core::scheduler::SchedulerImpl {
    create_scheduler(SchedulerConfig {
        scheduler_type,
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

/// Load `json`, run it for `max_frames` frames (or until `budget` elapses) with
/// `slots` concurrent slots and `num_workers` workers under `scheduler_type`,
/// and assert it actually completed all the frames within budget with zero
/// stale-task drops. Mirrors `same_pred_multi_arg_dep_count.rs::assert_runs_to_completion`,
/// generalized over scheduler type and slot count.
fn assert_runs_to_completion(
    json: &str,
    scheduler_type: SchedulerType,
    num_workers: usize,
    slots: usize,
    max_frames: usize,
    budget: Duration,
) {
    assert_runs_to_completion_cfg(
        json,
        scheduler_type,
        num_workers,
        slots,
        max_frames,
        budget,
        false,
    );
}

/// As [`assert_runs_to_completion`], with an explicit `inline_continuation` toggle
/// (the completing worker resolving a ready successor directly instead of queuing
/// it through the batch_queue) — orthogonal to scheduling correctness but exercised
/// explicitly for `instance_map` coverage (zero-threshold instances in particular).
#[allow(clippy::too_many_arguments)]
fn assert_runs_to_completion_cfg(
    json: &str,
    scheduler_type: SchedulerType,
    num_workers: usize,
    slots: usize,
    max_frames: usize,
    budget: Duration,
    inline_continuation: bool,
) {
    let spec =
        from_json_str(json, 4).unwrap_or_else(|e| panic!("expected this graph to load: {e}"));
    let scheduler = make_scheduler(scheduler_type, num_workers);
    let compiled = spec.compile(&scheduler);
    let config = RuntimeConfig {
        slots,
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
        inline_continuation,
        ..RuntimeConfig::default()
    };
    let mut rt = TomiiRtBuilder::with_config(compiled, scheduler, config)
        .build()
        .unwrap_or_else(|e| {
            panic!("build failed ({scheduler_type:?}, workers={num_workers}, slots={slots}): {e:?}")
        });
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
        "expected {max_frames} frame(s) to complete within {budget:?} \
         (scheduler={scheduler_type:?}, workers={num_workers}, slots={slots}), but the run was \
         still going at {elapsed:?} — a wrong instance_map threshold or a dropped routing entry \
         deadlocks exactly like this"
    );
    assert_eq!(
        final_stale_drops.get(),
        0,
        "expected every instance to run exactly once per frame \
         (scheduler={scheduler_type:?}, workers={num_workers}, slots={slots}) — a stale-task \
         drop means work was discarded instead of executed"
    );
}

// ---------------------------------------------------------------------------
// (a) Irregular reduction tree: an FB-style shape — a factor-8 leaf node, a
// level with UNEVEN group sizes (3 leaves, then 2), and a fuse node that joins
// a LEAF directly with an INTERNAL unit (two separate instance_map predecessors
// on two different producers, on the same consumer instance).
// ---------------------------------------------------------------------------

fn irregular_tree_json() -> &'static str {
    r#"
    {
        "nodes": [
            { "name": "leaves", "factor": 8, "function": "noop", "args": [] },
            {
                "name": "level1",
                "factor": 2,
                "function": "noop",
                "args": [
                    {
                        "type": "$barrier",
                        "predecessor": { "name": "leaves", "instance_map": [[0, 1, 2], [3, 4]] }
                    }
                ]
            },
            {
                "name": "fuse_mixed",
                "factor": 1,
                "function": "noop",
                "args": [
                    {
                        "type": "$dep",
                        "predecessor": { "name": "leaves", "instance_map": [[5]] }
                    },
                    {
                        "type": "$barrier",
                        "predecessor": { "name": "level1", "instance_map": [[0]] }
                    }
                ]
            },
            {
                "name": "root",
                "factor": 1,
                "function": "noop",
                "args": [
                    {
                        "type": "$dep",
                        "predecessor": { "name": "level1", "instance_map": [[1]] }
                    },
                    {
                        "type": "$barrier",
                        "predecessor": { "name": "fuse_mixed", "instance_map": [[0]] }
                    }
                ]
            }
        ]
    }
    "#
}

#[test]
fn irregular_tree_default_scheduler_w1() {
    assert_runs_to_completion(
        irregular_tree_json(),
        SchedulerType::WorkStealing,
        1,
        4,
        30,
        Duration::from_secs(5),
    );
}

#[test]
fn irregular_tree_default_scheduler_w_gt_1() {
    assert_runs_to_completion(
        irregular_tree_json(),
        SchedulerType::WorkStealing,
        4,
        4,
        30,
        Duration::from_secs(5),
    );
}

#[test]
fn irregular_tree_custom_scheduler_w1() {
    assert_runs_to_completion(
        irregular_tree_json(),
        SchedulerType::Custom,
        1,
        4,
        30,
        Duration::from_secs(5),
    );
}

#[test]
fn irregular_tree_custom_scheduler_w_gt_1() {
    assert_runs_to_completion(
        irregular_tree_json(),
        SchedulerType::Custom,
        4,
        4,
        30,
        Duration::from_secs(5),
    );
}

#[test]
fn irregular_tree_many_slots_in_flight() {
    // S > 1 slots concurrently in flight, many frames, both at W=1 (tightest
    // scheduling — a wrong threshold starves the single worker fastest) and a
    // wider worker pool.
    assert_runs_to_completion(
        irregular_tree_json(),
        SchedulerType::WorkStealing,
        1,
        8,
        100,
        Duration::from_secs(10),
    );
    assert_runs_to_completion(
        irregular_tree_json(),
        SchedulerType::WorkStealing,
        8,
        8,
        100,
        Duration::from_secs(10),
    );
}

// ---------------------------------------------------------------------------
// (b) Halo (neighbourhood) stencil with wrap: n=8 tiles, radius 1 — tile j
// depends on tiles j-1, j, j+1 (mod n). `tomii.halo(1, wrap=True)` on the
// Python side generates exactly this map.
// ---------------------------------------------------------------------------

fn halo_map_json(n: usize, radius: usize) -> String {
    let map: Vec<String> = (0..n)
        .map(|j| {
            let list: Vec<String> = (0..=2 * radius)
                .map(|k| ((j + n - radius + k) % n).to_string())
                .collect();
            format!("[{}]", list.join(", "))
        })
        .collect();
    format!(
        r#"
    {{
        "nodes": [
            {{ "name": "tile", "factor": {n}, "function": "noop", "args": [] }},
            {{
                "name": "halo_step",
                "factor": {n},
                "function": "noop",
                "args": [
                    {{
                        "type": "$barrier",
                        "predecessor": {{ "name": "tile", "instance_map": [{maps}] }}
                    }}
                ]
            }}
        ]
    }}
    "#,
        n = n,
        maps = map.join(", ")
    )
}

#[test]
fn halo_wrap_n8_radius1_default_scheduler() {
    let json = halo_map_json(8, 1);
    assert_runs_to_completion(
        &json,
        SchedulerType::WorkStealing,
        1,
        4,
        30,
        Duration::from_secs(5),
    );
    assert_runs_to_completion(
        &json,
        SchedulerType::WorkStealing,
        4,
        4,
        30,
        Duration::from_secs(5),
    );
}

#[test]
fn halo_wrap_n8_radius1_custom_scheduler() {
    let json = halo_map_json(8, 1);
    assert_runs_to_completion(
        &json,
        SchedulerType::Custom,
        1,
        4,
        30,
        Duration::from_secs(5),
    );
    assert_runs_to_completion(
        &json,
        SchedulerType::Custom,
        4,
        4,
        30,
        Duration::from_secs(5),
    );
}

#[test]
fn halo_wrap_n8_radius1_many_slots_many_frames() {
    let json = halo_map_json(8, 1);
    assert_runs_to_completion(
        &json,
        SchedulerType::WorkStealing,
        2,
        8,
        150,
        Duration::from_secs(10),
    );
}

// ---------------------------------------------------------------------------
// (c) A mapped consumer coexisting with an ordinary 1:1 edge on the same node:
// `combined` has an instance_map predecessor ("marks") AND a plain 1:1 $res
// edge ("vals", same factor, single offset) on a DIFFERENT predecessor.
// ---------------------------------------------------------------------------

fn mapped_plus_1to1_json() -> &'static str {
    r#"
    {
        "nodes": [
            { "name": "marks", "factor": 4, "function": "noop", "args": [] },
            { "name": "vals", "factor": 4, "function": "noop", "args": [] },
            {
                "name": "combined",
                "factor": 4,
                "function": "noop",
                "args": [
                    {
                        "type": "$dep",
                        "predecessor": { "name": "marks", "instance_map": [[0], [1], [2], [3]] }
                    },
                    { "type": "$res", "predecessor": { "name": "vals", "indexes": "0" } }
                ]
            }
        ]
    }
    "#
}

#[test]
fn mapped_consumer_coexists_with_ordinary_1to1_edge_default_scheduler() {
    assert_runs_to_completion(
        mapped_plus_1to1_json(),
        SchedulerType::WorkStealing,
        1,
        4,
        40,
        Duration::from_secs(5),
    );
    assert_runs_to_completion(
        mapped_plus_1to1_json(),
        SchedulerType::WorkStealing,
        4,
        4,
        40,
        Duration::from_secs(5),
    );
}

#[test]
fn mapped_consumer_coexists_with_ordinary_1to1_edge_custom_scheduler() {
    assert_runs_to_completion(
        mapped_plus_1to1_json(),
        SchedulerType::Custom,
        1,
        4,
        40,
        Duration::from_secs(5),
    );
    assert_runs_to_completion(
        mapped_plus_1to1_json(),
        SchedulerType::Custom,
        4,
        4,
        40,
        Duration::from_secs(5),
    );
}

// ---------------------------------------------------------------------------
// (d) Zero-threshold instances: a consumer instance whose instance_map entry
// is empty (and no coexisting predecessor contributes) has a total
// dependency threshold of 0 — dispatched at frame admission, exactly once per
// frame, like a root (see `crate::instance_map`'s module docs). Two shapes:
// a node where SOME instances are zero-threshold and others are not (the
// general case), and a node where EVERY instance is zero-threshold (fully
// root-like despite declaring a predecessor).
// ---------------------------------------------------------------------------

/// `cons[1]` has an empty map entry (zero threshold, dispatched at
/// admission); `cons[0]` and `cons[2]` have ordinary nonzero thresholds
/// (dispatched through the normal per-instance counter path). `fan_in`
/// barrier-waits on the full `cons` range, so it can only complete if EVERY
/// `cons` instance — zero-threshold or not — actually ran exactly once.
fn mixed_zero_threshold_json() -> &'static str {
    r#"
    {
        "nodes": [
            { "name": "leaves", "factor": 4, "function": "noop", "args": [] },
            {
                "name": "cons",
                "factor": 3,
                "function": "noop",
                "args": [
                    {
                        "type": "$barrier",
                        "predecessor": { "name": "leaves", "instance_map": [[0, 1], [], [2, 3]] }
                    }
                ]
            },
            {
                "name": "fan_in",
                "factor": 1,
                "function": "noop",
                "args": [
                    { "type": "$barrier", "predecessor": { "name": "cons", "indexes": "0-2" } }
                ]
            }
        ]
    }
    "#
}

/// `orphan` declares an `instance_map` predecessor, but EVERY entry is empty
/// — every instance has threshold 0, so the whole node behaves exactly like
/// an ordinary root/initial node despite not being one structurally (it does
/// have a predecessor arg; it just never routes anything to it).
fn all_zero_threshold_json() -> &'static str {
    r#"
    {
        "nodes": [
            { "name": "leaves2", "factor": 1, "function": "noop", "args": [] },
            {
                "name": "orphan",
                "factor": 2,
                "function": "noop",
                "args": [
                    {
                        "type": "$dep",
                        "predecessor": { "name": "leaves2", "instance_map": [[], []] }
                    }
                ]
            },
            {
                "name": "fan_in2",
                "factor": 1,
                "function": "noop",
                "args": [
                    { "type": "$barrier", "predecessor": { "name": "orphan", "indexes": "0-1" } }
                ]
            }
        ]
    }
    "#
}

#[test]
fn mixed_zero_threshold_default_scheduler_w1() {
    assert_runs_to_completion(
        mixed_zero_threshold_json(),
        SchedulerType::WorkStealing,
        1,
        4,
        30,
        Duration::from_secs(5),
    );
}

#[test]
fn mixed_zero_threshold_default_scheduler_w_gt_1() {
    assert_runs_to_completion(
        mixed_zero_threshold_json(),
        SchedulerType::WorkStealing,
        4,
        4,
        30,
        Duration::from_secs(5),
    );
}

#[test]
fn mixed_zero_threshold_custom_scheduler_w1() {
    assert_runs_to_completion(
        mixed_zero_threshold_json(),
        SchedulerType::Custom,
        1,
        4,
        30,
        Duration::from_secs(5),
    );
}

#[test]
fn mixed_zero_threshold_custom_scheduler_w_gt_1() {
    assert_runs_to_completion(
        mixed_zero_threshold_json(),
        SchedulerType::Custom,
        4,
        4,
        30,
        Duration::from_secs(5),
    );
}

#[test]
fn mixed_zero_threshold_many_slots_many_frames() {
    assert_runs_to_completion(
        mixed_zero_threshold_json(),
        SchedulerType::WorkStealing,
        1,
        8,
        100,
        Duration::from_secs(10),
    );
    assert_runs_to_completion(
        mixed_zero_threshold_json(),
        SchedulerType::WorkStealing,
        8,
        8,
        100,
        Duration::from_secs(10),
    );
}

#[test]
fn mixed_zero_threshold_inline_continuation_on_and_off() {
    for inline in [false, true] {
        assert_runs_to_completion_cfg(
            mixed_zero_threshold_json(),
            SchedulerType::WorkStealing,
            4,
            4,
            30,
            Duration::from_secs(5),
            inline,
        );
    }
}

#[test]
fn all_zero_threshold_default_scheduler_w1() {
    assert_runs_to_completion(
        all_zero_threshold_json(),
        SchedulerType::WorkStealing,
        1,
        4,
        30,
        Duration::from_secs(5),
    );
}

#[test]
fn all_zero_threshold_default_scheduler_w_gt_1() {
    assert_runs_to_completion(
        all_zero_threshold_json(),
        SchedulerType::WorkStealing,
        4,
        4,
        30,
        Duration::from_secs(5),
    );
}

#[test]
fn all_zero_threshold_custom_scheduler_w1() {
    assert_runs_to_completion(
        all_zero_threshold_json(),
        SchedulerType::Custom,
        1,
        4,
        30,
        Duration::from_secs(5),
    );
}

#[test]
fn all_zero_threshold_custom_scheduler_w_gt_1() {
    assert_runs_to_completion(
        all_zero_threshold_json(),
        SchedulerType::Custom,
        4,
        4,
        30,
        Duration::from_secs(5),
    );
}

#[test]
fn all_zero_threshold_many_slots_many_frames() {
    assert_runs_to_completion(
        all_zero_threshold_json(),
        SchedulerType::WorkStealing,
        1,
        8,
        100,
        Duration::from_secs(10),
    );
    assert_runs_to_completion(
        all_zero_threshold_json(),
        SchedulerType::WorkStealing,
        8,
        8,
        100,
        Duration::from_secs(10),
    );
}

// ---------------------------------------------------------------------------
// (e) Item 2: mixed mapped + plain args on the SAME predecessor. `level`
// reads `leaves` through BOTH a mapped `$barrier` (an irregular, QEC-tree-
// style grouping: level[0] fuses leaves {0,1,2}, level[1] fuses leaves
// {3,4}) AND a plain `$dep` on the SAME `leaves` predecessor — leaf 5 is a
// "halo"/boundary leaf every level instance also needs (filtered broadcast),
// exactly the shape `bench/qec-bench`'s real `frame_tag` broadcast node was
// invented to work around before this item landed. `root` barrier-waits the
// full `level` range, so completion is only possible if every leaf AND every
// level instance ran exactly once (a wrong union, a double-routed decrement,
// or the predecessor leaking into the ordinary N×N tables would each hang or
// double-dispatch).
// ---------------------------------------------------------------------------

fn mixed_same_pred_broadcast_json() -> &'static str {
    r#"
    {
        "nodes": [
            { "name": "leaves", "factor": 6, "function": "noop", "args": [] },
            {
                "name": "level",
                "factor": 2,
                "function": "noop",
                "args": [
                    {
                        "type": "$barrier",
                        "predecessor": { "name": "leaves", "instance_map": [[0, 1, 2], [3, 4]] }
                    },
                    { "type": "$dep", "predecessor": { "name": "leaves", "indexes": "5" } }
                ]
            },
            {
                "name": "root",
                "factor": 1,
                "function": "noop",
                "args": [
                    {
                        "type": "$barrier",
                        "predecessor": { "name": "level", "instance_map": [[0, 1]] }
                    }
                ]
            }
        ]
    }
    "#
}

/// Same QEC-like tree, but the plain coexisting arg is a 1:1 same-factor
/// `$res` edge instead of a broadcast `$dep` — `level2` reads a VALUE from
/// `leaves2` at its own offset (`level2[j]` reads `leaves2[j]`) while ALSO
/// barrier-waiting an irregular mapped subset. Exercises value delivery
/// (unaffected by routing this predecessor through `instance_map`'s
/// per-instance mechanism — see `runtime::init::build_predecessor_tables`)
/// alongside the mixed gating.
fn mixed_same_pred_1to1_json() -> &'static str {
    r#"
    {
        "nodes": [
            { "name": "leaves2", "factor": 4, "function": "noop", "args": [] },
            {
                "name": "level2",
                "factor": 4,
                "function": "noop",
                "args": [
                    {
                        "type": "$barrier",
                        "predecessor": { "name": "leaves2", "instance_map": [[1], [], [0], [3]] }
                    },
                    { "type": "$res", "predecessor": { "name": "leaves2", "indexes": "0" } }
                ]
            },
            {
                "name": "root2",
                "factor": 1,
                "function": "noop",
                "args": [
                    {
                        "type": "$barrier",
                        "predecessor": { "name": "level2", "instance_map": [[0, 1, 2, 3]] }
                    }
                ]
            }
        ]
    }
    "#
}

#[test]
fn mixed_same_pred_broadcast_default_scheduler_w1() {
    assert_runs_to_completion(
        mixed_same_pred_broadcast_json(),
        SchedulerType::WorkStealing,
        1,
        4,
        30,
        Duration::from_secs(5),
    );
}

#[test]
fn mixed_same_pred_broadcast_default_scheduler_w_gt_1() {
    assert_runs_to_completion(
        mixed_same_pred_broadcast_json(),
        SchedulerType::WorkStealing,
        4,
        4,
        30,
        Duration::from_secs(5),
    );
}

#[test]
fn mixed_same_pred_broadcast_custom_scheduler_w1() {
    assert_runs_to_completion(
        mixed_same_pred_broadcast_json(),
        SchedulerType::Custom,
        1,
        4,
        30,
        Duration::from_secs(5),
    );
}

#[test]
fn mixed_same_pred_broadcast_custom_scheduler_w_gt_1() {
    assert_runs_to_completion(
        mixed_same_pred_broadcast_json(),
        SchedulerType::Custom,
        4,
        4,
        30,
        Duration::from_secs(5),
    );
}

#[test]
fn mixed_same_pred_broadcast_many_slots_many_frames() {
    assert_runs_to_completion(
        mixed_same_pred_broadcast_json(),
        SchedulerType::WorkStealing,
        1,
        8,
        100,
        Duration::from_secs(10),
    );
    assert_runs_to_completion(
        mixed_same_pred_broadcast_json(),
        SchedulerType::WorkStealing,
        4,
        8,
        100,
        Duration::from_secs(10),
    );
}

#[test]
fn mixed_same_pred_broadcast_inline_continuation_on_and_off() {
    for inline_continuation in [false, true] {
        assert_runs_to_completion_cfg(
            mixed_same_pred_broadcast_json(),
            SchedulerType::WorkStealing,
            4,
            4,
            30,
            Duration::from_secs(5),
            inline_continuation,
        );
    }
}

#[test]
fn mixed_same_pred_1to1_default_scheduler_w1() {
    assert_runs_to_completion(
        mixed_same_pred_1to1_json(),
        SchedulerType::WorkStealing,
        1,
        4,
        30,
        Duration::from_secs(5),
    );
}

#[test]
fn mixed_same_pred_1to1_default_scheduler_w_gt_1() {
    assert_runs_to_completion(
        mixed_same_pred_1to1_json(),
        SchedulerType::WorkStealing,
        4,
        4,
        30,
        Duration::from_secs(5),
    );
}

#[test]
fn mixed_same_pred_1to1_custom_scheduler_w1() {
    assert_runs_to_completion(
        mixed_same_pred_1to1_json(),
        SchedulerType::Custom,
        1,
        4,
        30,
        Duration::from_secs(5),
    );
}

#[test]
fn mixed_same_pred_1to1_custom_scheduler_w_gt_1() {
    assert_runs_to_completion(
        mixed_same_pred_1to1_json(),
        SchedulerType::Custom,
        4,
        4,
        30,
        Duration::from_secs(5),
    );
}

#[test]
fn mixed_same_pred_1to1_many_slots_many_frames() {
    assert_runs_to_completion(
        mixed_same_pred_1to1_json(),
        SchedulerType::WorkStealing,
        1,
        8,
        100,
        Duration::from_secs(10),
    );
    assert_runs_to_completion(
        mixed_same_pred_1to1_json(),
        SchedulerType::WorkStealing,
        4,
        8,
        100,
        Duration::from_secs(10),
    );
}

#[test]
fn mixed_same_pred_1to1_inline_continuation_on_and_off() {
    for inline_continuation in [false, true] {
        assert_runs_to_completion_cfg(
            mixed_same_pred_1to1_json(),
            SchedulerType::WorkStealing,
            4,
            4,
            30,
            Duration::from_secs(5),
            inline_continuation,
        );
    }
}

// ---------------------------------------------------------------------------
// Validation errors surface through `from_json_str`.
// ---------------------------------------------------------------------------

#[test]
fn instance_map_and_indexes_mutually_exclusive_at_parse_time() {
    let json = r#"
    {
        "nodes": [
            { "name": "p", "factor": 4, "function": "noop", "args": [] },
            {
                "name": "c",
                "factor": 1,
                "function": "noop",
                "args": [
                    {
                        "type": "$dep",
                        "predecessor": {
                            "name": "p",
                            "indexes": "0",
                            "instance_map": [[0, 1]]
                        }
                    }
                ]
            }
        ]
    }
    "#;
    let err = from_json_str(json, 4).err().unwrap();
    assert!(err.to_string().contains("mutually exclusive"), "{err}");
}

#[test]
fn instance_map_rejects_out_of_range_producer_index() {
    let json = r#"
    {
        "nodes": [
            { "name": "p", "factor": 4, "function": "noop", "args": [] },
            {
                "name": "c",
                "factor": 1,
                "function": "noop",
                "args": [
                    {
                        "type": "$dep",
                        "predecessor": { "name": "p", "instance_map": [[9]] }
                    }
                ]
            }
        ]
    }
    "#;
    let err = from_json_str(json, 4).err().unwrap();
    assert!(err.to_string().contains("out of range"), "{err}");
}

#[test]
fn instance_map_accepts_res_type() {
    // $res + instance_map is now supported (value-carrying mapped edges) — loads
    // cleanly where it used to be rejected. Value-delivery correctness is
    // exercised by the real-plugin e2e tests in `same_pred_multi_arg_values.rs`
    // and the dedicated mapped-$res value tests elsewhere in this module.
    let json = r#"
    {
        "nodes": [
            { "name": "p", "factor": 4, "function": "noop", "args": [] },
            {
                "name": "c",
                "factor": 1,
                "function": "noop",
                "args": [
                    {
                        "type": "$res",
                        "predecessor": { "name": "p", "instance_map": [[0]] }
                    }
                ]
            }
        ]
    }
    "#;
    from_json_str(json, 4).expect("$res on a mapped predecessor should load cleanly");
}
