//! VALUE-level regression test for the `$frame_id` built-in (see
//! `tomii_core::runtime::arg_resolution`'s `process_runtime_refs` and
//! `crate::obj_gen::init_objects`'s reserved obj_id 2).
//!
//! `tomii-core`'s own `cargo test` build resolves every plugin function to a
//! `noop` stub (see `runtime/node_cache.rs::resolve_func`'s `#[cfg(test)]`
//! fallback), so no in-crate unit/integration test can observe a REAL kernel
//! reading `$frame_id` — only the arg-buffer injection itself, which
//! `arg_resolution::frame_id_tests` already covers directly (calling
//! `populate_cached_args_into` and inspecting the resulting buffer). This test
//! goes one step further and follows the SAME precedent as
//! `same_pred_multi_arg_values.rs`: build a small real plugin and the `main`
//! runtime binary as a one-off nested `cargo build`, run it for several
//! frames in non-network mode, and assert on the ACTUAL per-frame values a
//! real kernel received — proving `$frame_id` survives the full JSON ->
//! graph-compile -> node-cache -> hot-path-injection pipeline, across
//! generational slot reuse, end to end.
//!
//! Non-network frame numbering (see `slot_lifecycle.rs::restart_slot_nonnetwork`):
//! `next_frame_id = completed + currently_active`, monotonically increasing
//! from 0. With one slot and `max-frames N`, the kernel must observe exactly
//! `0, 1, .., N-1` in order.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

const MAX_FRAMES: usize = 5;

fn unique_dir(tag: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    env::temp_dir().join(format!(
        "tomii_frame_id_value_{tag}_{}_{}",
        std::process::id(),
        n
    ))
}

fn run(cmd: &mut Command, what: &str) -> String {
    let out = cmd
        .output()
        .unwrap_or_else(|e| panic!("failed to spawn {what}: {e}"));
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        out.status.success(),
        "{what} failed (status {:?}):\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}",
        out.status
    );
    stdout + &stderr
}

#[test]
fn frame_id_arg_delivers_the_actual_admitted_frame_number_across_frames() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let tomii_types = PathBuf::from(&manifest_dir).join("../tomii-types");
    let tomii_macro = PathBuf::from(&manifest_dir).join("../tomii-macro");

    let plugin_dir = unique_dir("plugin");
    fs::create_dir_all(plugin_dir.join("src")).unwrap();
    let log = plugin_dir.join("frame_id.log");

    fs::write(
        plugin_dir.join("Cargo.toml"),
        format!(
            r#"
[package]
name = "frameidvalue"
version = "0.1.0"
edition = "2021"

[lib]
name = "frameidvalue"
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
        r#"
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Mutex;
use tomii_macro::tomii_export;

static LOCK: Mutex<()> = Mutex::new(());

fn append_line(path: &str, line: &str) {
    let _g = LOCK.lock().unwrap();
    let mut f = OpenOptions::new().create(true).append(true).open(path).unwrap();
    writeln!(f, "{line}").unwrap();
}

#[tomii_export]
pub fn record_frame_id(out_path: String, frame_id: usize) -> usize {
    append_line(&out_path, &format!("{frame_id}"));
    frame_id
}
"#,
    )
    .unwrap();

    let plugin_target = plugin_dir.join("target");
    let func_path = plugin_dir.join("src/lib.rs");

    run(
        Command::new("cargo")
            .arg("build")
            .arg("--release")
            .arg("--manifest-path")
            .arg(plugin_dir.join("Cargo.toml"))
            .env("FUNC_PATH", &func_path)
            .env("CARGO_TARGET_DIR", &plugin_target),
        "cargo build (plugin)",
    );
    let dylib = plugin_target.join("release/libframeidvalue.so");
    assert!(dylib.exists(), "expected plugin dylib at {dylib:?}");

    let main_target = unique_dir("main_target");
    run(
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
    let main_bin = main_target.join("release/main");
    assert!(main_bin.exists(), "expected runtime binary at {main_bin:?}");

    let graph_json = format!(
        r#"
        {{
            "nodes": [
                {{ "name": "tagged", "factor": 1, "function": "record_frame_id",
                   "args": [
                       {{ "type": "String", "value": {log:?} }},
                       {{ "type": "$ref", "value": "$frame_id" }}
                   ] }}
            ]
        }}
        "#,
        log = log.to_str().unwrap(),
    );
    let graph_path = plugin_dir.join("graph.json");
    fs::write(&graph_path, graph_json).unwrap();

    let _ = fs::remove_file(&log);
    run(
        Command::new(&main_bin)
            .arg("--json")
            .arg(&graph_path)
            .arg("--dylib")
            .arg(&dylib)
            .arg("--workers")
            .arg("1")
            .arg("--core-offset")
            .arg("1")
            .arg("--slots")
            .arg("1")
            .arg("--max-frames")
            .arg(MAX_FRAMES.to_string())
            .arg("--max-runtime")
            .arg("15"),
        "main (single slot, multi-frame)",
    );

    let contents = fs::read_to_string(&log).unwrap_or_else(|e| panic!("missing {log:?}: {e}"));
    let observed: Vec<usize> = contents
        .lines()
        .map(|l| l.trim().parse().unwrap())
        .collect();
    let expected: Vec<usize> = (0..MAX_FRAMES).collect();
    assert_eq!(
        observed, expected,
        "with one slot, $frame_id must deliver the monotonically increasing sequence \
         0..MAX_FRAMES in admission order (non-network: completed + currently_active) — \
         any repeat, skip, or stale value here is a $frame_id delivery/reset bug"
    );

    let _ = fs::remove_dir_all(&plugin_dir);
    let _ = fs::remove_dir_all(&main_target);
}
