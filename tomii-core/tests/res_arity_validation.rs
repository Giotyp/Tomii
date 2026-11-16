//! Load-time regression test for `$res` arity validation
//! (`crate::instance_map::validate_res_arity`) — see
//! `tomii-core/src/instance_map.rs`'s module docs' "Arity validation"
//! section.
//!
//! `tomii-core`'s own `cargo test` build resolves every plugin function via
//! the `func_reg` stub, which always returns `None` from `get_func_argspec`
//! (see `instance_map::tests::validate_res_arity_is_a_noop_without_published_kernel_metadata`
//! for that unit-level fallback case) — so the actual REJECTION behaviour,
//! which needs a real plugin's published per-argument arity, can only be
//! observed by building a real plugin + the `main` runtime binary, exactly
//! like `tomii-core/tests/same_pred_multi_arg_values.rs` and
//! `tomii-core/tests/mapped_res_value_delivery.rs` do.
//!
//! Two shapes, one plugin:
//! - `scalar_bad`: a scalar (non-variadic) kernel parameter fed by an
//!   ordinary multi-index `$res` edge (`"indexes": "0,1"`, 2 values) — must
//!   be REJECTED at load with a clear error naming the node and explaining
//!   the scalar/variadic mismatch.
//! - `scalar_good`: the SAME kernel, fed by a single-index `$res` edge (1
//!   value) — must load and run to completion normally, confirming the
//!   check does not false-positive on the ordinary, overwhelmingly common
//!   shape.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

fn unique_dir(tag: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    env::temp_dir().join(format!(
        "tomii_res_arity_{tag}_{}_{}",
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
name = "resarity"
version = "0.1.0"
edition = "2021"

[lib]
name = "resarity"
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
use tomii_macro::tomii_export;

#[tomii_export]
pub fn produce(idx: usize) -> i64 {
    idx as i64
}

/// Deliberately NOT `#[tomii_export(variadic)]` — `val` is a plain scalar
/// `i64` parameter. A `$res` edge delivering more than one value to it must
/// be rejected at load.
#[tomii_export]
pub fn scalar_sink(idx: usize, val: i64) -> i64 {
    let _ = idx;
    val
}
"#,
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
    let dylib = plugin_target.join("release/libresarity.so");
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

fn graph_json(plugin_dir: &std::path::Path, indexes: &str) -> PathBuf {
    let graph = format!(
        r#"
        {{
            "nodes": [
                {{ "name": "producer", "factor": 4, "function": "produce",
                   "args": [
                       {{ "type": "$ref", "value": "$index" }}
                   ] }},
                {{ "name": "consumer", "factor": 1, "function": "scalar_sink",
                   "args": [
                       {{ "type": "$ref", "value": "$index" }},
                       {{ "type": "$res", "predecessor": {{ "name": "producer", "indexes": {indexes:?} }} }}
                   ] }}
            ]
        }}
        "#
    );
    let path = plugin_dir.join(format!("graph_{}.json", indexes.replace([',', '-'], "_")));
    fs::write(&path, graph).unwrap();
    path
}

#[test]
fn scalar_param_fed_by_a_multi_index_res_edge_is_rejected_at_load() {
    let fixture = build_fixture();
    let graph_path = graph_json(&fixture._plugin_dir, "0,1");

    let out = run(
        Command::new(&fixture.main_bin)
            .arg("--json")
            .arg(&graph_path)
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
            .arg("10"),
        "main (bad graph)",
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success(),
        "expected the runtime to reject the bad graph at load, but it exited successfully.\n\
         --- stdout ---\n{}\n--- stderr ---\n{stderr}",
        String::from_utf8_lossy(&out.stdout)
    );
    assert!(
        stderr.contains("consumer") && stderr.contains("scalar"),
        "expected the load error to name node 'consumer' and explain the scalar/variadic \
         mismatch; got:\n{stderr}"
    );
}

#[test]
fn scalar_param_fed_by_a_single_index_res_edge_loads_and_runs() {
    let fixture = build_fixture();
    let graph_path = graph_json(&fixture._plugin_dir, "0");

    let out = run(
        Command::new(&fixture.main_bin)
            .arg("--json")
            .arg(&graph_path)
            .arg("--dylib")
            .arg(&fixture.dylib)
            .arg("--workers")
            .arg("1")
            .arg("--core-offset")
            .arg("1")
            .arg("--slots")
            .arg("2")
            .arg("--max-frames")
            .arg("2")
            .arg("--max-runtime")
            .arg("10"),
        "main (good graph)",
    );
    assert!(
        out.status.success(),
        "expected the ordinary single-index shape to load and run cleanly.\n\
         --- stdout ---\n{}\n--- stderr ---\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}
