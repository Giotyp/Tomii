//! VALUE-level regression test for the same-predecessor-multiple-args fix.
//!
//! `tomii-core`'s own `cargo test` build resolves every plugin function to a
//! `noop` stub (see `runtime/node_cache.rs::resolve_func`'s `#[cfg(test)]`
//! fallback) — so no in-crate unit/integration test can observe an actual
//! per-instance VALUE, only scheduling completion (that's what
//! `same_pred_multi_arg_dep_count.rs` and `barrier_res_same_pred_guard.rs`
//! check). This test instead builds a small, real plugin and the `main`
//! runtime binary as a one-off nested `cargo build` (isolated to a temp
//! `CARGO_TARGET_DIR`, so it cannot race the outer test build), runs it, and
//! asserts on the ACTUAL values each consumer instance computed from a real
//! kernel — the thing a routing/dependency-count regression can get wrong
//! that `noop`-based tests structurally cannot catch (this is exactly how the
//! `mat_mul` regression in `examples/matrix-compute` was found: CI's example
//! verification runs real kernels; the in-crate test suite did not, and so
//! did not catch it).
//!
//! Three predecessor-sharing shapes, one producer/plugin, one build:
//! - `consume_dup`: TWO IDENTICAL `$res` args on an equal-factor predecessor
//!   (`produce.out()` twice) — mirrors `mat_mul`'s
//!   `[vec_mat.out(), vec_mat.out()]` exactly. Must dedupe to the single-arg
//!   path (1:1 exact dispatch preserved) — every instance `j` must read
//!   `produce[j]` for BOTH args.
//! - `consume_diff`: TWO DIFFERING, barrier-free `$res` args on the same
//!   equal-factor predecessor (`produce.out(0)` and `produce.out(1)`) — the
//!   shape the coordinator flagged as needing the forced full-barrier
//!   treatment (`Node`'s ambiguous-multi-arg detection in
//!   `graph_struct.rs`). Every instance `j` must read `produce[j]` (first
//!   arg) AND `produce[(j+1) % factor]` (second arg) — never the same cell
//!   for both, never a cell neither arg declared.
//! - `fuse`: the original QEC leaf/fuse shape (`$res` on `leaf[0]`, `$barrier`
//!   on `leaf[1]`, differing factors) — must read `leaf[0]`'s value
//!   specifically.
//!
//! (Offsets 0/1, not 0/-1: the JSON predecessor-index grammar
//! (`graph_gen.rs::parse_predecessor`) treats any `-`-containing index string
//! as a range (`start-end`), so a bare single negative offset like `"-1"`
//! cannot currently be expressed in JSON at all — a separate, pre-existing
//! parser limitation, not something this fix introduces or needs to work
//! around. 0/1 exercises the identical mechanism — two different single-cell
//! reads of one equal-factor predecessor, no barrier — without depending on
//! that limitation.)

use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

const PRODUCE_FACTOR: usize = 200;

fn unique_dir(tag: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    env::temp_dir().join(format!(
        "tomii_same_pred_values_{tag}_{}_{}",
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
fn same_predecessor_multi_arg_shapes_read_the_correct_declared_instances() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    // Absolute, so the plugin (built from an unrelated temp directory) can
    // path-depend on these sibling crates regardless of cwd.
    let tomii_types = PathBuf::from(&manifest_dir).join("../tomii-types");
    let tomii_macro = PathBuf::from(&manifest_dir).join("../tomii-macro");

    let plugin_dir = unique_dir("plugin");
    fs::create_dir_all(plugin_dir.join("src")).unwrap();

    let dup_log = plugin_dir.join("dup.log");
    let diff_log = plugin_dir.join("diff.log");
    let fuse_log = plugin_dir.join("fuse.log");

    fs::write(
        plugin_dir.join("Cargo.toml"),
        format!(
            r#"
[package]
name = "samepredvalues"
version = "0.1.0"
edition = "2021"

[lib]
name = "samepredvalues"
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
#![allow(clippy::too_many_arguments)]
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
pub fn produce(idx: usize) -> i64 {
    // Deliberately reverse the completion order: lower indexes sleep LONGER,
    // so higher indexes reliably finish first. Without this, every instance
    // might finish in index order anyway (nothing to race against) and a
    // wrong-dispatch bug could go unobserved even without a fix.
    std::thread::sleep(std::time::Duration::from_micros((200 - idx) as u64 * 30));
    idx as i64
}

#[tomii_export]
pub fn leaf_produce(idx: usize) -> i64 {
    idx as i64
}

#[tomii_export]
pub fn consume_dup(out_path: String, idx: usize, a: i64, b: i64) -> i64 {
    let val = if a == b { a } else { -1 };
    append_line(&out_path, &format!("{idx} {a} {b} {val}"));
    val
}

#[tomii_export]
pub fn consume_diff(out_path: String, idx: usize, a: i64, b: i64) -> i64 {
    append_line(&out_path, &format!("{idx} {a} {b}"));
    a * 1000 + b
}

#[tomii_export]
pub fn fuse_fn(out_path: String, a: i64) -> i64 {
    append_line(&out_path, &format!("{a}"));
    a
}
"#,
    )
    .unwrap();

    let plugin_target = plugin_dir.join("target");
    let func_path = plugin_dir.join("src/lib.rs");

    // Build the plugin dylib.
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
    let dylib = plugin_target.join("release/libsamepredvalues.so");
    assert!(dylib.exists(), "expected plugin dylib at {dylib:?}");

    // Build `main` wired to this plugin's exported functions, isolated to its
    // own CARGO_TARGET_DIR so this nested build cannot race the outer test
    // binary's own in-progress build of the SAME tomii-core crate.
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
                {{ "name": "produce", "factor": {n}, "function": "produce",
                   "args": [ {{ "type": "$ref", "value": "$index" }} ] }},
                {{ "name": "consume_dup", "factor": {n}, "function": "consume_dup",
                   "args": [
                       {{ "type": "String", "value": {dup_log:?} }},
                       {{ "type": "$ref", "value": "$index" }},
                       {{ "type": "$res", "predecessor": {{ "name": "produce", "indexes": "0" }} }},
                       {{ "type": "$res", "predecessor": {{ "name": "produce", "indexes": "0" }} }}
                   ] }},
                {{ "name": "consume_diff", "factor": {n}, "function": "consume_diff",
                   "args": [
                       {{ "type": "String", "value": {diff_log:?} }},
                       {{ "type": "$ref", "value": "$index" }},
                       {{ "type": "$res", "predecessor": {{ "name": "produce", "indexes": "0" }} }},
                       {{ "type": "$res", "predecessor": {{ "name": "produce", "indexes": "1" }} }}
                   ] }},
                {{ "name": "leaf", "factor": 2, "function": "leaf_produce",
                   "args": [ {{ "type": "$ref", "value": "$index" }} ] }},
                {{ "name": "fuse", "factor": 1, "function": "fuse_fn",
                   "args": [
                       {{ "type": "String", "value": {fuse_log:?} }},
                       {{ "type": "$res", "predecessor": {{ "name": "leaf", "indexes": "0" }} }},
                       {{ "type": "$barrier", "predecessor": {{ "name": "leaf", "indexes": "1" }} }}
                   ] }}
            ]
        }}
        "#,
        n = PRODUCE_FACTOR,
        dup_log = dup_log.to_str().unwrap(),
        diff_log = diff_log.to_str().unwrap(),
        fuse_log = fuse_log.to_str().unwrap(),
    );
    let graph_path = plugin_dir.join("graph.json");
    fs::write(&graph_path, graph_json).unwrap();

    for workers in [1usize, 2] {
        let _ = fs::remove_file(&dup_log);
        let _ = fs::remove_file(&diff_log);
        let _ = fs::remove_file(&fuse_log);

        run(
            Command::new(&main_bin)
                .arg("--json")
                .arg(&graph_path)
                .arg("--dylib")
                .arg(&dylib)
                .arg("--workers")
                .arg(workers.to_string())
                .arg("--core-offset")
                .arg("1")
                .arg("--slots")
                .arg("1")
                .arg("--max-frames")
                .arg("1")
                .arg("--max-runtime")
                .arg("15"),
            &format!("main (workers={workers})"),
        );

        // consume_dup: both args are the SAME declared cell — every instance
        // must see a == b == its own index (never -1, the mismatch sentinel).
        let dup_contents = fs::read_to_string(&dup_log)
            .unwrap_or_else(|e| panic!("workers={workers}: missing {dup_log:?}: {e}"));
        let mut dup_seen = vec![false; PRODUCE_FACTOR];
        for line in dup_contents.lines() {
            let f: Vec<i64> = line
                .split_whitespace()
                .map(|s| s.parse().unwrap())
                .collect();
            let (idx, a, b, val) = (f[0] as usize, f[1], f[2], f[3]);
            assert_eq!(
                a, idx as i64,
                "workers={workers}: consume_dup[{idx}] first $res read {a}, expected its own index {idx}"
            );
            assert_eq!(
                b, idx as i64,
                "workers={workers}: consume_dup[{idx}] second $res read {b}, expected its own index {idx}"
            );
            assert_eq!(
                val, idx as i64,
                "workers={workers}: consume_dup[{idx}] mismatched a/b (val=-1 sentinel)"
            );
            assert!(
                !dup_seen[idx],
                "workers={workers}: consume_dup[{idx}] ran more than once"
            );
            dup_seen[idx] = true;
        }
        assert!(
            dup_seen.iter().all(|&s| s),
            "workers={workers}: not every consume_dup instance ran: {dup_seen:?}"
        );

        // consume_diff: first arg reads produce[idx] (offset 0), second reads
        // produce[(idx+1) % N] (offset 1) — two DIFFERENT declared cells.
        let diff_contents = fs::read_to_string(&diff_log)
            .unwrap_or_else(|e| panic!("workers={workers}: missing {diff_log:?}: {e}"));
        let mut diff_seen = vec![false; PRODUCE_FACTOR];
        for line in diff_contents.lines() {
            let f: Vec<i64> = line
                .split_whitespace()
                .map(|s| s.parse().unwrap())
                .collect();
            let (idx, a, b) = (f[0] as usize, f[1], f[2]);
            let expected_b = ((idx + 1) % PRODUCE_FACTOR) as i64;
            assert_eq!(
                a, idx as i64,
                "workers={workers}: consume_diff[{idx}] first $res (offset 0) read {a}, expected {idx}"
            );
            assert_eq!(
                b, expected_b,
                "workers={workers}: consume_diff[{idx}] second $res (offset 1) read {b}, expected {expected_b} \
                 — reading the wrong predecessor instance is exactly the ambiguous-multi-arg bug this guards against"
            );
            assert!(
                !diff_seen[idx],
                "workers={workers}: consume_diff[{idx}] ran more than once"
            );
            diff_seen[idx] = true;
        }
        assert!(
            diff_seen.iter().all(|&s| s),
            "workers={workers}: not every consume_diff instance ran: {diff_seen:?}"
        );

        // fuse: must read leaf[0]'s value (0) — the original QEC leaf/fuse shape.
        let fuse_contents = fs::read_to_string(&fuse_log)
            .unwrap_or_else(|e| panic!("workers={workers}: missing {fuse_log:?}: {e}"));
        let fuse_lines: Vec<&str> = fuse_contents.lines().collect();
        assert_eq!(
            fuse_lines.len(),
            1,
            "workers={workers}: fuse should run exactly once per frame, got {fuse_lines:?}"
        );
        assert_eq!(
            fuse_lines[0].trim().parse::<i64>().unwrap(),
            0,
            "workers={workers}: fuse read leaf[{}], expected leaf[0]",
            fuse_lines[0].trim()
        );
    }

    let _ = fs::remove_dir_all(&plugin_dir);
    let _ = fs::remove_dir_all(&main_target);
}
