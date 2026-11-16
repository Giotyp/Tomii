//! VALUE-level end-to-end regression tests for `$res` (value-carrying)
//! `instance_map` edges — the last item of the `instance_map` feature (see
//! `tomii-core/src/instance_map.rs`'s module docs).
//!
//! `tomii-core`'s own `cargo test` build resolves every plugin function to a
//! `noop` stub, so no in-crate unit/integration test (including every e2e
//! test in `tests/instance_map.rs`) can observe an actual delivered VALUE —
//! only scheduling completion. A wrong (under-counted) per-instance threshold
//! is exactly the kind of bug a noop-kernel test structurally cannot catch:
//! dispatching an instance_map consumer ONE decrement early doesn't hang or
//! error, it just runs with whatever placeholder/partial state happens to be
//! in the arg buffer — completion-only tests see a graph that still "runs to
//! completion" (see this file's own sanity-check section, and the note in
//! `same_pred_multi_arg_values.rs`'s module doc making the same point for the
//! unmapped case). This test follows that exact precedent: build one real
//! plugin + the `main` runtime binary as a one-off nested `cargo build`, and
//! assert on ACTUAL values a real kernel received, across:
//! - (a) an IRREGULAR mapped-`$res` reduction tree (leaves → `level1` → `root`),
//!   each internal instance summing a variadic `Vec<i64>` of its mapped
//!   children — instance widths differ (3 children vs 1), and the tree is
//!   two levels deep, composing two independent mapped edges;
//! - (b) a wrap-around halo: `instance_map` neighbour sets include the
//!   high→low wraparound index, and the module's union convention delivers
//!   them in ASCENDING ABSOLUTE INDEX order (not "spatial" `j-1, j, j+1`
//!   declaration order) — the kernel asserts the exact neighbour values in
//!   that exact order;
//! - (c) a non-variadic, length-1-per-instance mapped `$res` permutation —
//!   the "ordinary single value" shape, delivered as a plain (non-`Vec`)
//!   argument.
//!
//! All three coexist with `$frame_id` on the same nodes (M3's other
//! requirement) and are run across many frames with `slots > 1` (forcing
//! generational slot reuse — M4), and across the default and `--custom`
//! schedulers, `workers` 1 and 4, and `--inline-continuation` on/off.

use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

const NUM_FRAMES: i64 = 12;
const SLOTS: usize = 3;

fn unique_dir(tag: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    env::temp_dir().join(format!(
        "tomii_mapped_res_value_{tag}_{}_{}",
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

/// `leaves[idx]` for a given frame — the independently-computed oracle this
/// test's expectations are built from.
fn leaf_val(frame: i64, idx: i64) -> i64 {
    frame * 100 + idx
}

/// `level1`'s two (irregular) children sets: instance 0 has THREE children
/// (`leaves` 0,1,2), instance 1 has ONE (`leaves` 3) — matches the
/// `"instance_map": [[0,1,2],[3]]` declared on the `leaves` predecessor below.
const LEVEL1_CHILDREN: [&[i64]; 2] = [&[0, 1, 2], &[3]];

fn level1_val(frame: i64, level_idx: usize) -> i64 {
    LEVEL1_CHILDREN[level_idx]
        .iter()
        .map(|&i| leaf_val(frame, i))
        .sum()
}

fn root_val(frame: i64) -> i64 {
    (0..2).map(|i| level1_val(frame, i)).sum()
}

fn src_halo_val(frame: i64, idx: i64) -> i64 {
    frame * 1000 + idx
}

/// `halo`'s 5 wrap-around neighbour sets (radius 1 over a 5-element ring),
/// matching the literal `"instance_map"` declared on `src_halo` below —
/// ALREADY sorted ascending, because `instance_map`'s union convention
/// (`crate::instance_map::union_instance_maps`) always normalises every
/// consumer instance's forward list to ascending absolute-producer-index
/// order, never the "spatial" `j-1, j, j+1` declaration order. Tile 0's
/// neighbours are declared as `{4, 0, 1}` (the wraparound) but delivered as
/// `[0, 1, 4]`; tile 4's as `{3, 4, 0}` delivered as `[0, 3, 4]`.
const HALO_NEIGHBOURS: [&[i64]; 5] = [&[0, 1, 4], &[0, 1, 2], &[1, 2, 3], &[2, 3, 4], &[0, 3, 4]];

fn src_perm_val(frame: i64, idx: i64) -> i64 {
    frame * 10 + idx
}

/// `perm[j]` reads `src_perm[(j + 2) % 4]` — a fixed-shift permutation, one
/// producer instance per consumer instance (length-1 `instance_map` entries).
fn perm_source(j: usize) -> i64 {
    ((j + 2) % 4) as i64
}

#[test]
fn mapped_res_value_delivery_matrix() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let tomii_types = PathBuf::from(&manifest_dir).join("../tomii-types");
    let tomii_macro = PathBuf::from(&manifest_dir).join("../tomii-macro");

    let plugin_dir = unique_dir("plugin");
    fs::create_dir_all(plugin_dir.join("src")).unwrap();

    let level1_log = plugin_dir.join("level1.log");
    let root_log = plugin_dir.join("root.log");
    let halo_log = plugin_dir.join("halo.log");
    let perm_log = plugin_dir.join("perm.log");

    fs::write(
        plugin_dir.join("Cargo.toml"),
        format!(
            r#"
[package]
name = "mappedresvalue"
version = "0.1.0"
edition = "2021"

[lib]
name = "mappedresvalue"
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
pub fn leaf_fn(frame_id: usize, idx: usize) -> i64 {
    frame_id as i64 * 100 + idx as i64
}

#[tomii_export(variadic)]
pub fn level1_fn(out_path: String, frame_id: usize, level_idx: usize, children: Vec<i64>) -> i64 {
    let sum: i64 = children.iter().sum();
    append_line(&out_path, &format!("{frame_id} {level_idx} {sum}"));
    sum
}

#[tomii_export(variadic)]
pub fn root_fn(out_path: String, frame_id: usize, children: Vec<i64>) -> i64 {
    let sum: i64 = children.iter().sum();
    append_line(&out_path, &format!("{frame_id} {sum}"));
    sum
}

#[tomii_export]
pub fn src_halo_fn(frame_id: usize, idx: usize) -> i64 {
    frame_id as i64 * 1000 + idx as i64
}

#[tomii_export(variadic)]
pub fn halo_fn(out_path: String, frame_id: usize, tile_idx: usize, neighbours: Vec<i64>) -> i64 {
    let joined: Vec<String> = neighbours.iter().map(|v| v.to_string()).collect();
    append_line(&out_path, &format!("{frame_id} {tile_idx} {}", joined.join(" ")));
    neighbours.iter().sum()
}

#[tomii_export]
pub fn src_perm_fn(frame_id: usize, idx: usize) -> i64 {
    frame_id as i64 * 10 + idx as i64
}

#[tomii_export]
pub fn perm_fn(out_path: String, frame_id: usize, idx: usize, val: i64) -> i64 {
    append_line(&out_path, &format!("{frame_id} {idx} {val}"));
    val
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
    let dylib = plugin_target.join("release/libmappedresvalue.so");
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
                {{ "name": "leaves", "factor": 4, "function": "leaf_fn",
                   "args": [
                       {{ "type": "$ref", "value": "$frame_id" }},
                       {{ "type": "$ref", "value": "$index" }}
                   ] }},
                {{ "name": "level1", "factor": 2, "function": "level1_fn",
                   "args": [
                       {{ "type": "String", "value": {level1_log:?} }},
                       {{ "type": "$ref", "value": "$frame_id" }},
                       {{ "type": "$ref", "value": "$index" }},
                       {{ "type": "$res", "predecessor": {{ "name": "leaves", "instance_map": [[0,1,2],[3]] }} }}
                   ] }},
                {{ "name": "root", "factor": 1, "function": "root_fn",
                   "args": [
                       {{ "type": "String", "value": {root_log:?} }},
                       {{ "type": "$ref", "value": "$frame_id" }},
                       {{ "type": "$res", "predecessor": {{ "name": "level1", "instance_map": [[0,1]] }} }}
                   ] }},
                {{ "name": "src_halo", "factor": 5, "function": "src_halo_fn",
                   "args": [
                       {{ "type": "$ref", "value": "$frame_id" }},
                       {{ "type": "$ref", "value": "$index" }}
                   ] }},
                {{ "name": "halo", "factor": 5, "function": "halo_fn",
                   "args": [
                       {{ "type": "String", "value": {halo_log:?} }},
                       {{ "type": "$ref", "value": "$frame_id" }},
                       {{ "type": "$ref", "value": "$index" }},
                       {{ "type": "$res", "predecessor": {{ "name": "src_halo",
                           "instance_map": [[4,0,1],[0,1,2],[1,2,3],[2,3,4],[3,4,0]] }} }}
                   ] }},
                {{ "name": "src_perm", "factor": 4, "function": "src_perm_fn",
                   "args": [
                       {{ "type": "$ref", "value": "$frame_id" }},
                       {{ "type": "$ref", "value": "$index" }}
                   ] }},
                {{ "name": "perm", "factor": 4, "function": "perm_fn",
                   "args": [
                       {{ "type": "String", "value": {perm_log:?} }},
                       {{ "type": "$ref", "value": "$frame_id" }},
                       {{ "type": "$ref", "value": "$index" }},
                       {{ "type": "$res", "predecessor": {{ "name": "src_perm", "instance_map": [[2],[3],[0],[1]] }} }}
                   ] }}
            ]
        }}
        "#,
        level1_log = level1_log.to_str().unwrap(),
        root_log = root_log.to_str().unwrap(),
        halo_log = halo_log.to_str().unwrap(),
        perm_log = perm_log.to_str().unwrap(),
    );
    let graph_path = plugin_dir.join("graph.json");
    fs::write(&graph_path, graph_json).unwrap();

    for workers in [1usize, 4] {
        for &custom in &[false, true] {
            for &inline in &[false, true] {
                let _ = fs::remove_file(&level1_log);
                let _ = fs::remove_file(&root_log);
                let _ = fs::remove_file(&halo_log);
                let _ = fs::remove_file(&perm_log);

                let tag = format!("workers={workers} custom={custom} inline_continuation={inline}");

                let mut cmd = Command::new(&main_bin);
                cmd.arg("--json")
                    .arg(&graph_path)
                    .arg("--dylib")
                    .arg(&dylib)
                    .arg("--workers")
                    .arg(workers.to_string())
                    .arg("--core-offset")
                    .arg("1")
                    .arg("--slots")
                    .arg(SLOTS.to_string())
                    .arg("--max-frames")
                    .arg(NUM_FRAMES.to_string())
                    .arg("--max-runtime")
                    .arg("30");
                if custom {
                    cmd.arg("--custom");
                }
                if inline {
                    cmd.arg("--inline-continuation");
                }
                run(&mut cmd, &format!("main ({tag})"));

                // --- level1: irregular 3-child / 1-child reduction, variadic + $frame_id ---
                let mut level1_seen: HashMap<(i64, usize), i64> = HashMap::new();
                for line in fs::read_to_string(&level1_log)
                    .unwrap_or_else(|e| panic!("{tag}: missing {level1_log:?}: {e}"))
                    .lines()
                {
                    let f: Vec<i64> = line
                        .split_whitespace()
                        .map(|s| s.parse().unwrap())
                        .collect();
                    level1_seen.insert((f[0], f[1] as usize), f[2]);
                }
                for frame in 0..NUM_FRAMES {
                    for level_idx in 0..2usize {
                        let expected = level1_val(frame, level_idx);
                        let actual = level1_seen.get(&(frame, level_idx)).unwrap_or_else(|| {
                            panic!("{tag}: level1[frame={frame}, idx={level_idx}] never ran")
                        });
                        assert_eq!(
                            *actual, expected,
                            "{tag}: level1[frame={frame}, idx={level_idx}] = {actual}, expected {expected}"
                        );
                    }
                }

                // --- root: second mapped-$res level, composing level1's output ---
                let mut root_seen: HashMap<i64, i64> = HashMap::new();
                for line in fs::read_to_string(&root_log)
                    .unwrap_or_else(|e| panic!("{tag}: missing {root_log:?}: {e}"))
                    .lines()
                {
                    let f: Vec<i64> = line
                        .split_whitespace()
                        .map(|s| s.parse().unwrap())
                        .collect();
                    root_seen.insert(f[0], f[1]);
                }
                for frame in 0..NUM_FRAMES {
                    let expected = root_val(frame);
                    let actual = root_seen
                        .get(&frame)
                        .unwrap_or_else(|| panic!("{tag}: root[frame={frame}] never ran"));
                    assert_eq!(
                        *actual, expected,
                        "{tag}: root[frame={frame}] = {actual}, expected {expected} \
                         (sum of all 4 leaves across both level1 instances)"
                    );
                }

                // --- halo: wrap-around neighbours, exact ascending order ---
                let mut halo_seen: HashMap<(i64, usize), Vec<i64>> = HashMap::new();
                for line in fs::read_to_string(&halo_log)
                    .unwrap_or_else(|e| panic!("{tag}: missing {halo_log:?}: {e}"))
                    .lines()
                {
                    let f: Vec<i64> = line
                        .split_whitespace()
                        .map(|s| s.parse().unwrap())
                        .collect();
                    halo_seen.insert((f[0], f[1] as usize), f[2..].to_vec());
                }
                for frame in 0..NUM_FRAMES {
                    for tile_idx in 0..5usize {
                        let expected: Vec<i64> = HALO_NEIGHBOURS[tile_idx]
                            .iter()
                            .map(|&i| src_halo_val(frame, i))
                            .collect();
                        let actual = halo_seen.get(&(frame, tile_idx)).unwrap_or_else(|| {
                            panic!("{tag}: halo[frame={frame}, tile={tile_idx}] never ran")
                        });
                        assert_eq!(
                            *actual, expected,
                            "{tag}: halo[frame={frame}, tile={tile_idx}] neighbours = {actual:?}, \
                             expected {expected:?} in ASCENDING absolute-index order"
                        );
                    }
                }

                // --- perm: non-variadic, length-1 mapped $res permutation ---
                let mut perm_seen: HashMap<(i64, usize), i64> = HashMap::new();
                for line in fs::read_to_string(&perm_log)
                    .unwrap_or_else(|e| panic!("{tag}: missing {perm_log:?}: {e}"))
                    .lines()
                {
                    let f: Vec<i64> = line
                        .split_whitespace()
                        .map(|s| s.parse().unwrap())
                        .collect();
                    perm_seen.insert((f[0], f[1] as usize), f[2]);
                }
                for frame in 0..NUM_FRAMES {
                    for idx in 0..4usize {
                        let expected = src_perm_val(frame, perm_source(idx));
                        let actual = perm_seen.get(&(frame, idx)).unwrap_or_else(|| {
                            panic!("{tag}: perm[frame={frame}, idx={idx}] never ran")
                        });
                        assert_eq!(
                            *actual, expected,
                            "{tag}: perm[frame={frame}, idx={idx}] = {actual}, expected {expected}"
                        );
                    }
                }
            }
        }
    }

    let _ = fs::remove_dir_all(&plugin_dir);
    let _ = fs::remove_dir_all(&main_target);
}
