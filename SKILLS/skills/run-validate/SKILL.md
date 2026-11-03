---
name: run-validate
description: "Use after editing the graph or plugin to re-measure performance. Builds the plugin, verifies correctness with a single worker, then scales up to establish a baseline and writes report.json."
---

# Skill: run-validate

Execute a Τομί graph safely: build the plugin, verify correctness with a single worker,
then scale up to establish a stable performance baseline. Produces a baseline record used by
[diagnose](../diagnose/SKILL.md) and [knob-search](../knob-search/SKILL.md).

## Trigger

- After building or modifying a graph (via [graph-build](../graph-build/SKILL.md))
- After modifying the plugin (via [plugin-author](../plugin-author/SKILL.md))
- After applying graph coarsening or knob changes, to measure the new configuration

## Steps

### 1. Build the plugin

Call `app.build(...)` from the Python entry point, or directly via:

```python
build_result = app.build(
    func_path=str(HERE / "src" / "lib.rs"),   # or C header path
    plugin_manifest=str(HERE / "Cargo.toml"),
    env={"CARGO_TARGET_DIR": _TARGET_DIR},
    release=True,
    clean=False,
)
```

If the build fails, read the compiler output carefully:
- Type mismatch errors → check parameter kinds (`&T`, owned `T`, variadic `Vec<T>` last) against the graph args
- `#[tomii_export]` not found → verify `tomii-macro` is in `Cargo.toml` dependencies
- Linker errors → check `crate-type = ["dylib", "rlib"]` in `[lib]` section

### 2. Correctness run (single worker)

Run with `workers=1, slots=1, max_frames=3` to isolate correctness issues from concurrency:

```python
proc = app.run(
    dylib=build_result.dylib,
    workers=1,
    slots=1,
    max_frames=3,
    max_runtime=60,            # watchdog: a stalled graph otherwise runs forever
    dump_state="state.json",   # per-slot runtime state at shutdown
)
print(proc.returncode)
```

Using `workers=1` eliminates data races as a source of incorrect output.
Using `max_frames=3` keeps the run short (the CLI default is 1 frame).

Check for:
- Non-zero `proc.returncode` → read the panic message in stderr (look for `panicked`).
  `run()` does not raise on failure.
- Run hits `max_runtime` without finishing → a dependency count that never reaches zero
  (see the traps table in [graph-build](../graph-build/SKILL.md)). Inspect `state.json`; for a live
  process, `kill -USR1 <pid>` writes numbered snapshots `state.json.1`, `.2`, …
- Index-out-of-bounds panics → check factor values vs. actual data sizes in plugin
- Wrong output values → debug the plugin function logic

### 3. Verify output correctness

If the graph writes to an output file (check `--inits` in the run, or look for file-writing
functions in the plugin), read the output and verify it is numerically/logically correct.

```bash
cat result.txt   # whatever file the graph's output function writes
```

For network-input graphs, check that every frame the sender sent completed: compare
`summary.total_frames` in `report.json` (plus excluded warm-up frames) with the sender's
count, and use the application's own output check. `debug=True` prints per-frame
completions but slows the run; use it only while debugging. Start the receiver before the
sender (see [network-ingest](../network-ingest/SKILL.md)).

### 4. Scale up for baseline measurement

Once correctness is confirmed, run with target configuration:

```python
import os
cpus = len(os.sched_getaffinity(0))   # CPUs this process may use (logical)
# Leave core_offset cores free and count physical cores if SMT is on.

proc = app.run(
    dylib=build_result.dylib,
    workers=cpus - 1,
    core_offset=1,
    slots=1,
    max_frames=20,
    exclude_frames=5,       # skip warm-up frames
    report="report.json",
    timing="timing.txt",    # required: the report is only written when timing is collected
)
assert proc.returncode == 0, proc.stderr
```

Remove an old `report.json` before the run, so a run that wrote none is noticed.

### 5. Read the baseline

Check and summarize the report with the bundled script (in this skill's directory):

```bash
touch .run_start            # before the run
# ... run ...
python3 <skill-dir>/scripts/check_report.py report.json --newer-than .run_start \
        --expect-frames <max_frames - exclude_frames>
```

It exits 1 if the report is missing (no `timing=`), stale, reports dropped tasks or covers
too few frames, and otherwise prints the fields below. Record them as the baseline:

```json
{
  "summary": {
    "avg_latency_us": ...,
    "p50_latency_us": ...,
    "p99_latency_us": ...,
    "p999_latency_us": ...,
    "throughput_frames_per_sec": ...,
    "total_frames": ...,
    "total_tasks_per_frame": ...,
    "stale_task_drops": ...,
    "scheduling_overhead_diagnostic": {
      "overhead_pct": ...,
      "interpretation": "..."
    }
  },
  "critical_path": {
    "max_node_factor": ...,
    "length_nodes": ...,
    "estimated_latency_us": ...
  }
}
```

`stale_task_drops` must be 0: a non-zero value means tasks were dropped and the run is
not valid, whatever its latency.

### 6. Check variance

If `p99_latency_us > 2 * avg_latency_us`, the measurements have high variance.
Increase `max_frames` to 50 or `exclude_frames` to 10 for a more stable sample.

For network-input graphs, also compare mean and tail across 3 repeated runs: one run is
not a baseline.

### 7. Optional: scheduling recording

With `record=True` (plus `timing=`), the runtime also writes `timing_sched.csv`:
```bash
python3 scripts/analyze_sched.py timing_sched.csv --system-threads 1 --slots 1
python3 scripts/scheduler_visualize.py timing_sched.csv -o gantt.png
```

## Output

Baseline record:
```
BASELINE
========
Config: workers=N, slots=1, batching_size=1, coalesce_barriers=False, inline_continuation=False
avg_latency_us: X   p50: X   p99: X   p999: X
throughput_frames_per_sec: X
frames completed: X / X   stale_task_drops: 0
overhead_pct: X% (interpretation: ...)
total_tasks_per_frame: X
critical_path: N nodes, max_factor=N, estimated=Xus

Next: run [diagnose](../diagnose/SKILL.md) to classify the bottleneck.
```

## See also

- [diagnose](../diagnose/SKILL.md) — classify the bottleneck from the baseline report
- [plugin-author](../plugin-author/SKILL.md) — if the build fails due to plugin issues
- `python -m tomii --list-knobs` — every `graph.run()` option
