//! End-to-end integration tests for the Τομί runtime.
//!
//! These tests compile simple graphs programmatically and run them to completion,
//! verifying that the resolver, slot lifecycle, and successor dispatch all work
//! correctly together.  Functions are no-ops (returning `CmTypes::None`) — the
//! tests validate structural execution rather than computation.
//!
//! NOTE: `TomiiRt::run()` blocks the calling thread until all frames complete
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
        worker_hook: None,
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
        max_frames: 1,
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
        .max_frames(1)
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
        .max_frames(100)
        .build();

    assert!(
        matches!(result, Err(BuildError::InvalidConfig(_))),
        "expected InvalidConfig for 65 slots, got an unexpected variant"
    );
}

/// Multiple frames through a single slot: verifies slot reinitialisation across
/// frame boundaries (the core correctness invariant for Bugs #14–#22).
#[test]
fn test_multi_frame_single_slot() {
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
            max_frames: 5,
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

/// Regression test for findings #14/#19: `--slot-priority` on a **non-network**
/// (compute-only) graph with a single slot used to hang forever. Slot-priority
/// suppressed the in-place non-network restart and instead waited to *promote* a
/// Buffering slot — but a slot only ever becomes Buffering in the network
/// packet-admission path, so with one slot and no network the run stalled after
/// frame 0 (observed at ~22 GB RSS, spinning indefinitely).
///
/// The test drives several frames through one slot with slot-priority +
/// inline-continuation on the custom scheduler (the exact repro flag set) and
/// asserts that frames keep completing. Before the fix `frames_completed` sticks
/// at 1 and the run only ends when `max_runtime` fires, failing the assertion
/// rather than deadlocking the test binary.
#[test]
fn test_slot_priority_single_slot_nonnetwork_restarts() {
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
                    { "type": "$res", "predecessor": { "name": "b", "indexes": "0" } }
                ]
            }
        ]
    }
    "#;

    // Custom (lock-free priority) scheduler to match the `--custom` repro.
    let scheduler = create_scheduler(SchedulerConfig {
        scheduler_type: SchedulerType::Custom,
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
        worker_hook: None,
    });
    let compiled = from_json_str(json, 2)
        .expect("JSON parse failed")
        .compile(&scheduler);

    // max_frames must be far above the success target AND large enough that the run
    // cannot finish the whole budget within the first ~10 ms poll tick — otherwise the
    // wait loop sees `completed == max_frames` on its first check and returns before the
    // predicate is ever called, leaving `observed` at 0. This tiny 3-task/frame graph
    // runs thousands of frames per 10 ms, so 1000 was too small (flaky on fast/idle
    // machines); 10M matches `test_run_until_predicate_terminates_run`. `max_runtime`
    // still bounds a regressed (stalling) run so a real hang fails within 5 s.
    const TARGET_FRAMES: usize = 8;
    let mut rt = TomiiRtBuilder::new(compiled, scheduler)
        .slots(1)
        .max_frames(10_000_000)
        .max_runtime(Some(5))
        .slot_priority_enabled(true)
        .inline_continuation(true)
        .build()
        .expect("build failed");

    let mut observed = 0usize;
    rt.run_until(|p| {
        observed = observed.max(p.frames_completed);
        p.frames_completed >= TARGET_FRAMES
    })
    .expect("run_until failed");

    assert!(
        observed >= TARGET_FRAMES,
        "single-slot slot-priority run stalled: only {observed} frame(s) completed \
         (expected >= {TARGET_FRAMES}); the non-network restart path did not fire"
    );
}

/// Regression test for findings #21 defect B: a bulk (fanout-bulk) completion must
/// decrement a filtered successor edge over `|chunk ∩ filter|`, and must NOT skip a
/// chunk that overlaps the filter without starting inside it.
///
/// Graph: `a`(factor=16) → `b`(factor=16, 1:1 $res) → `c`(reads `b.out(5)`). With
/// `inline_continuation` off and W=4, `b` dispatches as 4 bulk chunks of 4; instance
/// 5 lives in chunk `[4,8)`, which starts at 4 — OUTSIDE `c`'s filter `[5,6)`. The old
/// start-only filter check skipped that chunk, so `c`'s dependency was never
/// decremented and the frame never completed (hang, bounded here by `max_runtime`).
/// The fix intersects the chunk with the filter and decrements instance 5, so the
/// frames complete.
#[test]
fn test_fanout_bulk_filtered_successor_completes() {
    let json = r#"
    {
        "nodes": [
            { "name": "a", "factor": 16, "function": "noop", "args": [] },
            {
                "name": "b",
                "factor": 16,
                "function": "noop",
                "args": [
                    { "type": "$res", "predecessor": { "name": "a", "indexes": "0" } }
                ]
            },
            {
                "name": "c",
                "function": "noop",
                "args": [
                    { "type": "$res", "predecessor": { "name": "b", "indexes": "5" } }
                ]
            }
        ]
    }
    "#;

    let spec = from_json_str(json, 2).expect("JSON parse failed");
    let scheduler = create_scheduler(SchedulerConfig {
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
    });
    let compiled = spec.compile(&scheduler);

    const TARGET_FRAMES: usize = 3;
    let mut rt = TomiiRtBuilder::with_config(
        compiled,
        scheduler,
        RuntimeConfig {
            slots: 1,
            // Large enough that the run cannot finish before the predicate observes
            // TARGET_FRAMES (see the note in the slot-priority test above).
            max_frames: 10_000_000,
            max_runtime: Some(5),
            system_threads: 1,
            workers: 4,
            inline_continuation: false, // force the fanout-bulk dispatch path
            ..RuntimeConfig::default()
        },
    )
    .build()
    .expect("build failed");

    let mut observed = 0usize;
    rt.run_until(|p| {
        observed = observed.max(p.frames_completed);
        p.frames_completed >= TARGET_FRAMES
    })
    .expect("run_until failed");

    assert!(
        observed >= TARGET_FRAMES,
        "fanout-bulk run stalled: only {observed} frame(s) completed (expected >= \
         {TARGET_FRAMES}); a bulk chunk overlapping c's filter [5,6) without starting \
         inside it failed to decrement its dependency"
    );
}

/// Run `json` for at least `target_frames` frames and return
/// `(frames_completed, max_stale_drops_seen)`. A healthy run drops nothing; the
/// stale-task guard only fires when dispatched work is discarded.
fn run_capturing_stale_drops(
    json: &str,
    workers: usize,
    inline_continuation: bool,
    target_frames: usize,
) -> (usize, usize) {
    let spec = from_json_str(json, 2).expect("JSON parse failed");
    let scheduler = create_scheduler(SchedulerConfig {
        scheduler_type: SchedulerType::WorkStealing,
        core_offset: 0,
        num_workers: workers,
        record: false,
        external_recorder: None,
        base_instant: std::time::Instant::now(),
        system_threads: 1,
        receiver_threads: 0,
        target_batch_size: 1,
        batch_timeout_us: 10,
        worker_affinity: None,
        worker_hook: None,
    });
    let compiled = spec.compile(&scheduler);
    let mut rt = TomiiRtBuilder::with_config(
        compiled,
        scheduler,
        RuntimeConfig {
            slots: 1,
            // Large budget so the run outlives the predicate's first observation of
            // `target_frames` regardless of how fast the graph is (avoids front-run).
            max_frames: 10_000_000,
            max_runtime: Some(5),
            system_threads: 1,
            workers,
            inline_continuation,
            ..RuntimeConfig::default()
        },
    )
    .build()
    .expect("build failed");

    let mut frames = 0usize;
    let mut drops = 0usize;
    rt.run_until(|p| {
        frames = frames.max(p.frames_completed);
        drops = drops.max(p.stale_drops);
        p.frames_completed >= target_frames
    })
    .expect("run_until failed");
    (frames, drops)
}

/// Regression test for the successor-less-root work-loss bug (2026-09 eval item 5):
/// a graph whose only fan-out is a root with no successor had `total_tasks = 0`, so
/// completion fired before the root's instances ran and the generation bump silently
/// dropped them. Counting initial nodes in `pending_tasks` makes completion wait for
/// every root instance — asserted here via zero stale drops across the flag matrix.
#[test]
fn test_successor_less_root_runs_all_instances() {
    let json = r#"
    { "nodes": [ { "name": "root", "factor": 8, "function": "noop", "args": [] } ] }
    "#;
    const TARGET: usize = 5;
    for workers in [1usize, 4] {
        for inline in [true, false] {
            let (frames, drops) = run_capturing_stale_drops(json, workers, inline, TARGET);
            assert!(
                frames >= TARGET,
                "successor-less root stalled at {frames} frames (W={workers}, inline={inline})"
            );
            assert_eq!(
                drops, 0,
                "successor-less root dropped {drops} instance(s) as stale (W={workers}, \
                 inline={inline}) — its instances were not counted in completion"
            );
        }
    }
}

/// Regression test for the partial-coverage variant: a root feeding a FILTERED
/// successor (`a.out(3)`) leaves a's other instances consumed by nothing. Before the
/// fix those uncovered instances gated nothing and were dropped once the covered path
/// completed the frame. Counting the root's instances closes it.
#[test]
fn test_root_with_filtered_successor_runs_all_instances() {
    let json = r#"
    {
        "nodes": [
            { "name": "a", "factor": 8, "function": "noop", "args": [] },
            {
                "name": "b",
                "function": "noop",
                "args": [
                    { "type": "$res", "predecessor": { "name": "a", "indexes": "3" } }
                ]
            }
        ]
    }
    "#;
    const TARGET: usize = 5;
    for workers in [1usize, 4] {
        for inline in [true, false] {
            let (frames, drops) = run_capturing_stale_drops(json, workers, inline, TARGET);
            assert!(
                frames >= TARGET,
                "filtered-successor root stalled at {frames} frames (W={workers}, inline={inline})"
            );
            assert_eq!(
                drops, 0,
                "filtered-successor root dropped {drops} uncovered instance(s) (W={workers}, \
                 inline={inline})"
            );
        }
    }
}

/// Correctness guard for the custom scheduler's channel sharding: at W > shard_size
/// the default config splits workers into multiple shard channels (dispatch
/// round-robins across them, idle workers steal across them). This exercises W=16
/// (2 shards) with a 128-task/frame fan-out graph over many frames and asserts every
/// task runs — no lost or stranded tasks (zero stale drops) and all frames complete.
#[test]
fn test_custom_scheduler_sharded_high_w_no_lost_tasks() {
    let json = r#"
    {
        "nodes": [
            { "name": "a", "factor": 64, "function": "noop", "args": [] },
            {
                "name": "b",
                "factor": 64,
                "function": "noop",
                "args": [
                    { "type": "$res", "predecessor": { "name": "a", "indexes": "0" } }
                ]
            }
        ]
    }
    "#;
    let workers = 16; // > CUSTOM_SHARD_SIZE (8) → multiple shards
    let scheduler = create_scheduler(SchedulerConfig {
        scheduler_type: SchedulerType::Custom,
        core_offset: 0,
        num_workers: workers,
        record: false,
        external_recorder: None,
        base_instant: std::time::Instant::now(),
        system_threads: 1,
        receiver_threads: 0,
        target_batch_size: 1,
        batch_timeout_us: 10,
        worker_affinity: None,
        worker_hook: None,
    });
    let compiled = from_json_str(json, 2)
        .expect("JSON parse failed")
        .compile(&scheduler);
    let mut rt = TomiiRtBuilder::with_config(
        compiled,
        scheduler,
        RuntimeConfig {
            slots: 1,
            max_frames: 10_000_000,
            max_runtime: Some(5),
            system_threads: 1,
            workers,
            ..RuntimeConfig::default()
        },
    )
    .build()
    .expect("build failed");

    const TARGET: usize = 50;
    let mut frames = 0usize;
    let mut drops = 0usize;
    rt.run_until(|p| {
        frames = frames.max(p.frames_completed);
        drops = drops.max(p.stale_drops);
        p.frames_completed >= TARGET
    })
    .expect("run_until failed");

    assert!(
        frames >= TARGET,
        "sharded custom run stalled at {frames} frames (W={workers})"
    );
    assert_eq!(
        drops, 0,
        "sharded custom scheduler lost/stranded {drops} task instance(s) (W={workers})"
    );
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
            worker_hook: None,
        });
        let compiled = from_json_str(json, 2).unwrap().compile(&sched);

        let mut rt =
            TomiiRtBuilder::new_with_plugin(compiled, Arc::new(PassthroughScheduler::new(2)))
                .max_runtime(Some(5))
                .max_frames(1)
                .build()
                .expect("build failed");

        rt.run().expect("plugin scheduler run failed");
    }
}

// ---------------------------------------------------------------------------
// P5: WorkerHook + run_until
// ---------------------------------------------------------------------------

mod worker_hook_and_run_until {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    /// Counts start/exit callbacks; both must run on the worker threads.
    struct CountingHook {
        starts: AtomicUsize,
        exits: AtomicUsize,
    }

    impl tomii_core::WorkerHook for CountingHook {
        fn on_worker_start(&self, _worker_index: usize) {
            self.starts.fetch_add(1, Ordering::SeqCst);
        }
        fn on_worker_exit(&self, _worker_index: usize) {
            self.exits.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn make_scheduler_with_hook(
        scheduler_type: SchedulerType,
        hook: Arc<CountingHook>,
    ) -> tomii_core::scheduler::SchedulerImpl {
        create_scheduler(SchedulerConfig {
            scheduler_type,
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
            worker_hook: Some(hook),
        })
    }

    /// Wait (bounded) for exits to converge with starts — rayon detaches its
    /// worker threads, so a worker can fire its start callback after pool drop
    /// and both handlers can lag it. Re-reads both counters each poll: comparing
    /// exits against a stale starts snapshot races with late-starting workers.
    fn wait_for_exits(hook: &CountingHook) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let starts = hook.starts.load(Ordering::SeqCst);
            let exits = hook.exits.load(Ordering::SeqCst);
            if starts >= 1 && exits == starts {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn hook_lifecycle(scheduler_type: SchedulerType) {
        let hook = Arc::new(CountingHook {
            starts: AtomicUsize::new(0),
            exits: AtomicUsize::new(0),
        });
        {
            let scheduler = make_scheduler_with_hook(scheduler_type, Arc::clone(&hook));
            // Rayon calls start handlers as threads spawn; give them a moment.
            let deadline = Instant::now() + Duration::from_secs(5);
            while hook.starts.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(10));
            }
            drop(scheduler);
        }
        assert!(
            hook.starts.load(Ordering::SeqCst) >= 1,
            "no worker start callbacks ran"
        );
        assert!(
            wait_for_exits(&hook),
            "exit callbacks ({}) never converged with start callbacks ({})",
            hook.exits.load(Ordering::SeqCst),
            hook.starts.load(Ordering::SeqCst)
        );
    }

    #[test]
    fn test_worker_hook_rayon_lifecycle() {
        hook_lifecycle(SchedulerType::WorkStealing);
    }

    #[test]
    fn test_worker_hook_custom_lifecycle() {
        hook_lifecycle(SchedulerType::Custom);
    }

    /// run_until: the predicate terminates a run that would otherwise churn
    /// through a huge frame budget, and it observes sane progress snapshots.
    #[test]
    fn test_run_until_predicate_terminates_run() {
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
        let scheduler = create_scheduler(SchedulerConfig {
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
            worker_hook: None,
        });
        let compiled = spec.compile(&scheduler);

        let config = RuntimeConfig {
            slots: 1,
            // Budget far beyond what completes before the predicate fires
            // (a few thousand frames at most in the ~30ms this test runs).
            // Not usize::MAX: the network build sizes a per-frame drop bitmap
            // to max_frames + slots, which must stay allocatable.
            max_frames: 10_000_000,
            max_runtime: None,
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

        let started = Instant::now();
        let mut ticks = 0usize;
        rt.run_until(|progress| {
            assert_eq!(progress.max_frames, 10_000_000);
            ticks += 1;
            ticks >= 3
        })
        .expect("run_until failed");

        assert!(ticks >= 3, "predicate saw only {} ticks", ticks);
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "run_until did not terminate promptly ({:?})",
            started.elapsed()
        );
    }
}
