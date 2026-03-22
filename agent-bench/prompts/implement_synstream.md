# Task: Implement Wavefront in SynStream

## Problem Description

Implement an N×N anti-diagonal wavefront sweep.

**Grid initialization:**
- `grid[0][j] = j + 1` for all j (top row)
- `grid[i][0] = i + 1` for all i (left column)
- All other cells initialized to 0

**Recurrence:**
- `grid[i][j] = 0.5 * (grid[i-1][j] + grid[i][j-1])` for all interior cells (i > 0, j > 0)

**Parallelism:**
- Anti-diagonal d contains all (i, j) with i + j = d
- All cells within one diagonal are independent and can be computed in parallel
- Diagonal d+1 depends on diagonal d

## Framework

SynStream is a Rust task-graph framework with a Python API. The package is at:

```
<REPO_ROOT>/synstream/
```

Read `AGENT.md` at `<REPO_ROOT>/AGENT.md` for a quick-reference on the plugin API, then read the package source for full details.

## What to create

In `<WORKSPACE>`, create from scratch:

- `Cargo.toml` — Cargo manifest for the plugin dylib
- `src/lib.rs` — Rust kernel implementation
- `run_wavefront.py` — Python script that builds the graph, runs the benchmark, and writes `report.json`

No other files are required, but you may create additional source files if needed.

## Output requirements

- Write `report.json` containing at minimum `{"summary": {"avg_latency_us": <float>}}`
- Call the verifier at `<REPO_ROOT>/agent-bench/tools/verify_wavefront.py --n N`
  and confirm it prints `PASS`.

## Verification

After completing your implementation, run it and confirm it prints `PASS`.
