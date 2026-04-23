---
name: run-validate
description: Use after editing the graph or plugin to re-measure performance. Builds the plugin, verifies correctness with a single worker, then scales up to establish a baseline and writes report.json.
---

# Skill: run-validate

Execute a Τομί graph safely: build the plugin, verify correctness with a single worker,
then scale up to establish a stable performance baseline. Produces a baseline record used by
[diagnose](diagnose.md) and [knob-search](knob-search.md).

## Trigger

- After building or modifying a graph (via [graph-build](graph-build.md))
- After modifying the plugin (via [plugin-author](plugin-author.md))
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
- Type mismatch errors → check that return types match `CmTypes` variants
- `#[tomii_export]` not found → verify `tomii-macro` is in `Cargo.toml` dependencies
- Linker errors → check `crate-type = ["dylib", "rlib"]` in `[lib]` section

### 2. Correctness run (single worker)

Run with `workers=1, slots=1, max_streams=3` to isolate correctness issues from concurrency:

```python
app.run(
    dylib=build_result.dylib,
    workers=1,
    slots=1,
    max_streams=3,
)
```

Using `workers=1` eliminates data races as a source of incorrect output.
Using `max_streams=3` keeps the run short.

Check for:
- Non-zero exit code → read panic message in stderr (look for `thread 'main' panicked`)
- Index-out-of-bounds panics → check factor values vs. actual data sizes in plugin
- Wrong output values → debug the plugin function logic

### 3. Verify output correctness

If the graph writes to an output file (check `--inits` in the run, or look for file-writing
functions in the plugin), read the output and verify it is numerically/logically correct.

```bash
# Example for matrix-compute
cat result.txt
```

For network-input graphs, check that the expected number of streams completed by reading
stdout (the runtime prints "Completed iteration N").

### 4. Scale up for baseline measurement

Once correctness is confirmed, run with target configuration:

```python
import os
physical_cores = os.cpu_count()  # or read from /proc/cpuinfo

app.run(
    dylib=build_result.dylib,
    workers=physical_cores,
    core_offset=1,
    slots=1,
    max_streams=20,
    exclude_streams=5,       # skip first 5 streams (JIT warm-up)
    report="report.json",
    timing="timing.txt",
)
```

### 5. Read the baseline

Read `report.json` and record these fields as the baseline:

```json
{
  "summary": {
    "avg_latency_us": ...,
    "p99_latency_us": ...,
    "throughput_streams_per_sec": ...,
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

### 6. Check variance

If `p99_latency_us > 2 * avg_latency_us`, the measurements have high variance.
Increase `max_streams` to 50 or `exclude_streams` to 10 for a more stable sample.

### 7. Optional: validate scheduling recording

If `--record` was enabled (adds `timing_sched.csv`), run:
```bash
python3 scripts/validate_recording.py timing_sched.csv
```

Check for "overlapping tasks" warnings (indicates a scheduling bug, not a user issue).

## Output

Baseline record:
```
BASELINE
========
Config: workers=N, slots=1, batching_size=1, coalesce_barriers=False, inline_continuation=False
avg_latency_us: X
p99_latency_us: X
throughput_streams_per_sec: X
overhead_pct: X% (interpretation: ...)
total_tasks_per_stream: X
critical_path: N nodes, max_factor=N, estimated=Xus

Next: run [diagnose](diagnose.md) to classify the bottleneck.
```

## See also

- [diagnose](diagnose.md) — classify the bottleneck from the baseline report
- [plugin-author](plugin-author.md) — if the build fails due to plugin issues
- [AGENT.md](../AGENT.md) — key `graph.run()` flags reference
