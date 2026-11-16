//! Black-box regression test: a panicking plugin kernel must fail the run
//! fast and nonzero, not idle to `--max-runtime` and exit **rc=0**.
//!
//! Before the worker-panic fail-fast fix (see
//! `runtime::shared_data::TaskPanicState` and
//! `runtime::task_execution::execute_task`'s `catch_unwind` wrapper), that was
//! exactly what the `--custom` scheduler did: `CustomScheduler::shutdown`'s
//! `let _ = handle.join();` silently swallowed the panicking worker thread's
//! `Err`, so `TomiiRt::run_impl`'s wait loop had no way to notice and idled
//! all the way to `--max-runtime`.
//!
//! **Important caveat for a `--dylib`-loaded kernel specifically** (this
//! test's shape): a panic that tries to unwind out of a dynamically loaded
//! `.so` hits Rust's own cross-dylib "foreign exception" abort guard
//! (`fatal runtime error: Rust cannot catch foreign exceptions, aborting`)
//! *before* it ever reaches `execute_task`'s `catch_unwind` — this is a
//! pre-existing Rust runtime behavior, unrelated to and unaffected by the
//! fail-fast fix, and it already yields a nonzero exit (SIGABRT) with the
//! message printed to stderr by the default panic hook before the abort.
//! So THIS test's assertions hold identically with or without the fail-fast
//! fix for a `--dylib` kernel panic — it is a genuine, valuable black-box
//! safety net (directly matching the bug report's "a kernel that panics"
//! scenario), but it is NOT a sanity check of the fix's own code path. The
//! fix's actual `catch_unwind` mechanism — needed for a panic that does NOT
//! cross an FFI boundary, such as the original bug report's own panic site
//! (`instance_map`'s `$res` arity guard, pure Rust code inside `tomii-core`
//! itself) — is unit-tested directly in
//! `runtime::task_execution::panic_fail_fast_tests`, which patches a
//! same-binary panicking kernel into a built `NodeCacheEntry` (no dylib, no
//! FFI boundary) and is the one that was sanity-checked by disabling the fix.
//!
//! This test builds one real plugin whose only kernel panics unconditionally,
//! runs the `main` binary against a generous `--max-runtime` (60s), and
//! asserts:
//! - the process exits with a NONZERO status,
//! - well before `--max-runtime` elapses (bounded by a much shorter wall-clock
//!   budget — proving "fail fast", not "idle then fail"),
//! - with the panic message surfaced on stderr,
//! - under BOTH the default (Rayon) and `--custom` schedulers.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const PANIC_MESSAGE: &str = "intentional test panic from boom kernel";
/// Generous upper bound passed to `--max-runtime`. The test's actual pass/fail
/// signal is the wall-clock budget below, which must be far smaller than this.
const MAX_RUNTIME_SECS: u64 = 60;
/// The run must exit within this wall-clock budget — proof that it failed
/// fast rather than idling out to `MAX_RUNTIME_SECS`. Generous enough to
/// absorb CI/build-machine scheduling noise while still being more than an
/// order of magnitude below `MAX_RUNTIME_SECS`.
const FAIL_FAST_BUDGET: Duration = Duration::from_secs(10);

fn unique_dir(tag: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    env::temp_dir().join(format!(
        "tomii_worker_panic_fail_fast_{tag}_{}_{}",
        std::process::id(),
        n
    ))
}

struct PluginFixture {
    dylib: PathBuf,
    main_bin: PathBuf,
    _plugin_dir: PathBuf,
    _main_target: PathBuf,
}

fn run(cmd: &mut Command, what: &str) -> std::process::Output {
    cmd.output()
        .unwrap_or_else(|e| panic!("failed to spawn {what}: {e}"))
}

fn build_fixture() -> PluginFixture {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let tomii_types = PathBuf::from(&manifest_dir).join("../tomii-types");
    let tomii_macro = PathBuf::from(&manifest_dir).join("../tomii-macro");

    let plugin_dir = unique_dir("plugin");
    fs::create_dir_all(plugin_dir.join("src")).unwrap();

    fs::write(
        plugin_dir.join("Cargo.toml"),
        format!(
            r#"
[package]
name = "workerpanicfailfast"
version = "0.1.0"
edition = "2021"

[lib]
name = "workerpanicfailfast"
crate-type = ["dylib", "rlib"]

[dependencies]
tomii-types = {{ path = {tomii_types:?} }}
tomii-macro = {{ path = {tomii_macro:?} }}
"#
        ),
    )
    .unwrap();

    fs::write(
        plugin_dir.join("src/lib.rs"),
        format!(
            r#"
use tomii_macro::tomii_export;

/// Panics unconditionally on every call — the only kernel in this plugin.
#[tomii_export]
pub fn boom(idx: usize) -> i64 {{
    let _ = idx;
    panic!("{PANIC_MESSAGE}");
}}
"#
        ),
    )
    .unwrap();

    let plugin_target = plugin_dir.join("target");
    let func_path = plugin_dir.join("src/lib.rs");

    let out = run(
        Command::new("cargo")
            .arg("build")
            .arg("--release")
            .arg("--manifest-path")
            .arg(plugin_dir.join("Cargo.toml"))
            .env("FUNC_PATH", &func_path)
            .env("CARGO_TARGET_DIR", &plugin_target),
        "cargo build (plugin)",
    );
    assert!(
        out.status.success(),
        "plugin build failed:\n--- stdout ---\n{}\n--- stderr ---\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let dylib = plugin_target.join("release/libworkerpanicfailfast.so");
    assert!(dylib.exists(), "expected plugin dylib at {dylib:?}");

    let main_target = unique_dir("main_target");
    let out = run(
        Command::new("cargo")
            .arg("build")
            .arg("--release")
            .arg("-p")
            .arg("tomii-core")
            .arg("--bin")
            .arg("main")
            .env("FUNC_PATH", &func_path)
            .env("CARGO_TARGET_DIR", &main_target)
            .current_dir(&manifest_dir),
        "cargo build (main)",
    );
    assert!(
        out.status.success(),
        "main build failed:\n--- stdout ---\n{}\n--- stderr ---\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let main_bin = main_target.join("release/main");
    assert!(main_bin.exists(), "expected runtime binary at {main_bin:?}");

    PluginFixture {
        dylib,
        main_bin,
        _plugin_dir: plugin_dir,
        _main_target: main_target,
    }
}

fn graph_json(plugin_dir: &std::path::Path) -> PathBuf {
    let graph = r#"
    {
        "nodes": [
            { "name": "producer", "factor": 1, "function": "boom",
              "args": [
                  { "type": "$ref", "value": "$index" }
              ] }
        ]
    }
    "#;
    let path = plugin_dir.join("graph.json");
    fs::write(&path, graph).unwrap();
    path
}

/// Runs `main` against the panicking graph under one scheduler flag set, and
/// asserts the fail-fast contract: nonzero exit, well within budget, panic
/// message surfaced.
fn assert_fails_fast(fixture: &PluginFixture, graph_path: &std::path::Path, custom: bool) {
    let tag = if custom { "custom" } else { "default" };

    let mut cmd = Command::new(&fixture.main_bin);
    cmd.arg("--json")
        .arg(graph_path)
        .arg("--dylib")
        .arg(&fixture.dylib)
        .arg("--workers")
        .arg("1")
        .arg("--core-offset")
        .arg("1")
        .arg("--slots")
        .arg("2")
        .arg("--max-frames")
        .arg("1")
        .arg("--max-runtime")
        .arg(MAX_RUNTIME_SECS.to_string());
    if custom {
        cmd.arg("--custom");
    }

    let start = Instant::now();
    let out = run(&mut cmd, &format!("main ({tag} scheduler)"));
    let elapsed = start.elapsed();

    let stderr = String::from_utf8_lossy(&out.stderr);
    let stdout = String::from_utf8_lossy(&out.stdout);

    assert!(
        !out.status.success(),
        "{tag}: expected a nonzero exit after a worker panic, got success.\n\
         --- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
    );
    assert!(
        elapsed < FAIL_FAST_BUDGET,
        "{tag}: expected the run to fail fast (within {FAIL_FAST_BUDGET:?}), but it took \
         {elapsed:?} — close to or at --max-runtime ({MAX_RUNTIME_SECS}s) suggests the panic \
         was not detected promptly.\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
    );
    assert!(
        stderr.contains(PANIC_MESSAGE),
        "{tag}: expected the panic message to be surfaced on stderr.\n\
         --- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
    );
}

#[test]
fn worker_panic_fails_the_run_fast_under_both_schedulers() {
    let fixture = build_fixture();
    let graph_path = graph_json(&fixture._plugin_dir);

    assert_fails_fast(&fixture, &graph_path, false);
    assert_fails_fast(&fixture, &graph_path, true);

    let _ = fs::remove_dir_all(&fixture._plugin_dir);
    let _ = fs::remove_dir_all(&fixture._main_target);
}
