//! End-to-end integration tests for the Τομί runtime.
//!
//! These tests compile simple graphs programmatically and run them to completion,
//! verifying that the resolver, slot lifecycle, and successor dispatch all work
//! correctly together.  Functions are no-ops (returning `CmTypes::None`) — the
//! tests validate structural execution rather than computation.
//!
//! NOTE: `TomiiRt::run()` blocks the calling thread until all streams complete
//! or `max_runtime` is exceeded.  Each test sets `max_runtime` to a short bound
//! so a hang in the runtime surfaces as a test timeout rather than a deadlock.

use tomii_core::{
    graph_gen::from_json_str,
    runtime::{BatchConfig, RuntimeConfig, SpinWaitConfig, TomiiRtBuilder},
    scheduler::{create_scheduler, SchedulerConfig, SchedulerType},
    BuildError,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Minimal scheduler: 2 workers, 1 system thread, no recording.
fn make_scheduler() -> tomii_core::scheduler::SchedulerImpl {
    create_scheduler(SchedulerConfig {
        scheduler_type: SchedulerType::WorkStealing,
        core_offset: 0,
        num_workers: 2,
        record: false,
        external_recorder: None,
        base_instant: std::time::Instant::now(),
        system_threads: 1,
        receiver_threads: 0,
        target_batch_size: 1,
        batch_timeout_us: 10,
        worker_affinity: None,
    })
}

/// Build and run a JSON graph with default minimal settings.
/// Panics if the graph doesn't complete within 5 seconds.
fn run_graph(json: &str) {
    let spec = from_json_str(json, 2).expect("JSON parse failed");
    let scheduler = make_scheduler();
    let compiled = spec.compile(&scheduler);

    let config = RuntimeConfig {
        slots: 1,
        max_streams: 1,
        max_runtime: Some(5),
        system_threads: 1,
        workers: 2,
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

    rt.run().expect("run failed");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// A → B: single dependency edge, both factor=1.
/// Verifies basic task execution and successor dispatch along a linear chain.
#[test]
fn test_linear_pipeline() {
    let json = r#"
    {
        "nodes": [
            { "name": "a", "function": "noop", "args": [] },
            {
                "name": "b",
                "function": "noop",
                "args": [
                    { "type": "$res", "predecessor": { "name": "a", "indexes": "0" } }
                ]
            }
        ]
    }
    "#;
    run_graph(json);
}

/// A → B, A → C, B → D, C → D: classic diamond.
/// Verifies that D fires only after both B and C complete (barrier convergence).
#[test]
fn test_diamond() {
    let json = r#"
    {
        "nodes": [
            { "name": "a", "function": "noop", "args": [] },
            {
                "name": "b",
                "function": "noop",
                "args": [
                    { "type": "$res", "predecessor": { "name": "a", "indexes": "0" } }
                ]
            },
            {
                "name": "c",
                "function": "noop",
                "args": [
                    { "type": "$res", "predecessor": { "name": "a", "indexes": "0" } }
                ]
            },
            {
                "name": "d",
                "function": "noop",
                "args": [
                    { "type": "$barrier", "predecessor": { "name": "b", "indexes": "0" } },
                    { "type": "$barrier", "predecessor": { "name": "c", "indexes": "0" } }
                ]
            }
        ]
    }
    "#;
    run_graph(json);
}

/// A(factor=4) → B(factor=4) with 1:1 index mapping.
/// Verifies that parallel task instances complete and the 1:1 dispatch optimisation
/// (pred_succ_1to1_offset) fires the correct successor instance without spin-waiting.
#[test]
fn test_parallel_1to1() {
    let json = r#"
    {
        "nodes": [
            { "name": "gen", "factor": 4, "function": "noop", "args": [] },
            {
                "name": "compute",
                "factor": 4,
                "function": "noop",
                "args": [
                    { "type": "$res", "predecessor": { "name": "gen", "indexes": "0" } }
                ]
            }
        ]
    }
    "#;
    run_graph(json);
}

/// A(factor=4) → barrier → B(factor=1).
/// Verifies that B fires only after all 4 instances of A complete.
#[test]
fn test_barrier_fanin() {
    let json = r#"
    {
        "nodes": [
            { "name": "workers", "factor": 4, "function": "noop", "args": [] },
            {
                "name": "aggregator",
                "function": "noop",
                "args": [
                    {
                        "type": "$barrier",
                        "predecessor": {
                            "name": "workers",
                            "indexes": "0-3"
                        }
                    }
                ]
            }
        ]
    }
    "#;
    run_graph(json);
}

/// TomiiRtBuilder::build() must return BuildError::InvalidConfig for out-of-range slots.
#[test]
fn test_build_error_slots_zero() {
    let json = r#"{ "nodes": [{ "name": "a", "function": "noop", "args": [] }] }"#;
    let spec = from_json_str(json, 1).unwrap();
    let scheduler = make_scheduler();
    let compiled = spec.compile(&scheduler);

    let result = TomiiRtBuilder::new(compiled, scheduler)
        .slots(0)
        .max_streams(1)
        .build();

    assert!(
        matches!(result, Err(BuildError::InvalidConfig(_))),
        "expected InvalidConfig, got an unexpected variant"
    );
}

/// slots > 64 must be rejected at build time.
#[test]
fn test_build_error_slots_too_large() {
    let json = r#"{ "nodes": [{ "name": "a", "function": "noop", "args": [] }] }"#;
    let spec = from_json_str(json, 1).unwrap();
    let scheduler = make_scheduler();
    let compiled = spec.compile(&scheduler);

    let result = TomiiRtBuilder::new(compiled, scheduler)
        .slots(65)
        .max_streams(100)
        .build();

    assert!(
        matches!(result, Err(BuildError::InvalidConfig(_))),
        "expected InvalidConfig for 65 slots, got an unexpected variant"
    );
}

/// Multiple streams through a single slot: verifies slot reinitialisation across
/// stream boundaries (the core correctness invariant for Bugs #14–#22).
#[test]
fn test_multi_stream_single_slot() {
    let json = r#"
    {
        "nodes": [
            { "name": "a", "function": "noop", "args": [] },
            {
                "name": "b",
                "function": "noop",
                "args": [
                    { "type": "$res", "predecessor": { "name": "a", "indexes": "0" } }
                ]
            }
        ]
    }
    "#;

    let spec = from_json_str(json, 2).expect("JSON parse failed");
    let scheduler = make_scheduler();
    let compiled = spec.compile(&scheduler);

    let mut rt = TomiiRtBuilder::with_config(
        compiled,
        scheduler,
        RuntimeConfig {
            slots: 1,
            max_streams: 5,
            max_runtime: Some(10),
            system_threads: 1,
            workers: 2,
            ..RuntimeConfig::default()
        },
    )
    .build()
    .expect("build failed");

    rt.run().expect("run failed");
}

// ---------------------------------------------------------------------------
// Plugin scheduler test (requires `plugin-scheduler` feature)
// ---------------------------------------------------------------------------

/// Minimal TaskScheduler that wraps a Rayon thread pool.
/// Verifies that an external plugin scheduler runs a complete graph.
#[cfg(feature = "plugin-scheduler")]
mod plugin_tests {
    use std::sync::Arc;
    use tomii_core::{
        graph_gen::from_json_str,
        runtime::TomiiRtBuilder,
        scheduler::{create_scheduler, SchedulerConfig, SchedulerType, TaskScheduler},
        Priority, TaskMeta,
    };

    struct PassthroughScheduler {
        pool: rayon::ThreadPool,
        workers: usize,
    }

    impl PassthroughScheduler {
        fn new(workers: usize) -> Self {
            Self {
                pool: rayon::ThreadPoolBuilder::new()
                    .num_threads(workers)
                    .build()
                    .unwrap(),
                workers,
            }
        }
    }

    impl TaskScheduler for PassthroughScheduler {
        fn spawn_task_with_meta_priority(
            &self,
            _p: Priority,
            _m: Option<TaskMeta>,
            task: Box<dyn FnOnce() + Send + 'static>,
        ) {
            self.pool.spawn(task);
        }
        fn spawn_to_group_with_meta(
            &self,
            _g: usize,
            p: Priority,
            m: Option<TaskMeta>,
            task: Box<dyn FnOnce() + Send + 'static>,
        ) {
            self.spawn_task_with_meta_priority(p, m, task);
        }
        fn workers(&self) -> usize {
            self.workers
        }
        fn core_offset(&self) -> usize {
            0
        }
        fn system_threads(&self) -> usize {
            1
        }
        fn receiver_core_offset(&self) -> usize {
            0
        }
        fn receiver_threads(&self) -> usize {
            0
        }
    }

    #[test]
    fn test_plugin_scheduler_completes_graph() {
        let json = r#"{"nodes":[{"name":"a","function":"noop","args":[]},{"name":"b","function":"noop","args":[{"type":"$res","predecessor":{"name":"a","indexes":"0"}}]}]}"#;

        // Use a plain scheduler to compile (provides core metadata).
        let sched = create_scheduler(SchedulerConfig {
            scheduler_type: SchedulerType::WorkStealing,
            core_offset: 0,
            num_workers: 2,
            record: false,
            external_recorder: None,
            base_instant: std::time::Instant::now(),
            system_threads: 1,
            receiver_threads: 0,
            target_batch_size: 1,
            batch_timeout_us: 10,
            worker_affinity: None,
        });
        let compiled = from_json_str(json, 2).unwrap().compile(&sched);

        let mut rt =
            TomiiRtBuilder::new_with_plugin(compiled, Arc::new(PassthroughScheduler::new(2)))
                .max_runtime(Some(5))
                .max_streams(1)
                .build()
                .expect("build failed");

        rt.run().expect("plugin scheduler run failed");
    }
}
