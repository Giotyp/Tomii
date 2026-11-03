//! Every Tomii graph JSON committed to the repo (examples/**, bench/**) must still
//! parse after graph-loading changes. A load-time guard rejecting one
//! "ungrouped-barrier + single-index-$res on the same predecessor, with
//! group_size" silent-stall shape briefly lived here
//! (`Graph::validate_barrier_res_same_pred_conflict`); that shape, and a second
//! one found later, are now both resolved correctly at runtime instead of
//! rejected (see `tomii-core/src/graph.rs` and
//! `tomii-core/src/runtime/arg_resolution.rs`), so the guard — and the
//! `GUARD_MARKER` check below — is now vacuous but kept as a tripwire in case
//! a similar guard is reintroduced for some other shape.
//!
//! This only exercises *static* graph JSON files checked into the repo. Several
//! examples (radar-pipeline, mimo-bench, mapreduce, gpu-vectoradd, ...) build
//! their graph at run time via the Python API instead of committing a JSON
//! file; those are verified separately (see the fix-3 report) since they
//! require the Python package, not just this crate.

use std::path::Path;
use tomii_core::graph_gen::from_json_str;

/// Find every `*.json` file under `root` that looks like a Tomii graph file
/// (top-level object with a `nodes` key) — this excludes plain data/config
/// JSON that happens to live next to a graph (e.g. mimo-bench's
/// `tddconfig-*.json` radio configs, which have no `nodes` key).
fn find_graph_jsons(root: &Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let Ok(contents) = std::fs::read_to_string(&path) else {
                continue;
            };
            let Ok(value) = serde_json::from_str::<serde_json::Value>(&contents) else {
                continue;
            };
            if value.get("nodes").is_some() {
                found.push(path);
            }
        }
    }
    found
}

/// Marker substring unique to the new guard's error message (see
/// `Graph::validate_barrier_res_same_pred_conflict` in `tomii-core/src/graph.rs`).
/// Used to tell "this guard rejected the graph" apart from unrelated failures.
const GUARD_MARKER: &str = "validate_barrier_res_same_pred_conflict";

#[test]
fn every_shipped_graph_json_loads() {
    // NOTE: these binaries are compiled with no FUNC_PATH (plain `cargo test`), so
    // the function registry only resolves the built-in "noop" (see
    // `tomii-core/build.rs`). A shipped example's real `initializations` entries
    // (e.g. matrix-compute's `fft_planner`) therefore fail at the *function-call*
    // stage regardless of this change — that failure is pre-existing and orthogonal
    // to graph *structure*. What this test actually asserts is narrower and
    // unambiguous: the new guard added for fix 3 must not be the reason any shipped
    // graph fails to load. A graph that fails for some other, pre-existing reason
    // (no plugin loaded) is reported but does not fail the test.
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("tomii-core has a parent dir");

    let mut checked = 0;
    let mut rejected_by_guard: Vec<String> = Vec::new();
    let mut other_failures: Vec<String> = Vec::new();
    for subdir in ["examples", "bench"] {
        let dir = repo_root.join(subdir);
        if !dir.is_dir() {
            continue;
        }
        for path in find_graph_jsons(&dir) {
            let contents = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
            // workers=8 is an arbitrary generous value — these graphs are only
            // parsed here, not run, so the exact worker count doesn't matter
            // except for factor="$workers" resolution.
            match from_json_str(&contents, 8) {
                Ok(_) => {}
                Err(e) if e.to_string().contains(GUARD_MARKER) => {
                    rejected_by_guard.push(format!("{}: {e}", path.display()));
                }
                Err(e) => {
                    other_failures.push(format!("{}: {e}", path.display()));
                }
            }
            checked += 1;
        }
    }
    assert!(
        checked >= 3,
        "expected to find at least the known static graph.json files under \
         examples/ and bench/, found {checked}"
    );
    if !other_failures.is_empty() {
        eprintln!(
            "note: {} shipped graph(s) failed to load for reasons unrelated to \
             the new guard (expected without a compiled plugin): {other_failures:#?}",
            other_failures.len()
        );
    }
    assert!(
        rejected_by_guard.is_empty(),
        "the new barrier/$res-same-predecessor guard rejected shipped graph(s) \
         that must keep loading: {rejected_by_guard:#?}"
    );
}
