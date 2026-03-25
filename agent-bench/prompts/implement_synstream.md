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

SynStream is a Rust task-graph framework with a Python API.
The `synstream` package is installed in the environment — `import synstream as ss`.

Read `AGENT.md` in this workspace for a quick-reference on the plugin API,
Cargo.toml template, and performance optimization guide.

Run these discovery commands to understand the full API before writing code:

```bash
python -m synstream --schema          # graph construction: node options, arg types
python -m synstream --list-knobs-json # all graph.run() runtime flags with search hints
```

## What to create

In `<WORKSPACE>`, create from scratch:

- `Cargo.toml` — Cargo manifest for the plugin dylib
- `src/lib.rs` — Rust kernel implementation
- `run_wavefront.py` — Python script that builds the graph, runs the benchmark, and writes `report.json`

No other files are required, but you may create additional source files if needed.

## Output requirements

- Write `report.json` by passing `report="report.json"` to `graph.run()`. This produces
  structured diagnostics (`optimization_suggestions`, `scheduling_overhead_diagnostic`)
  needed for the optimize phase. The report must contain at minimum
  `{"summary": {"avg_latency_us": <float>}}`.
- After running, expose `grid[N-1][N-1]` (the bottom-right corner cell) and call:
  ```
  python verify_wavefront.py --n N --corner VALUE
  ```
  where `VALUE` is your computed `grid[N-1][N-1]`. Confirm it prints `PASS`.

## Verification

After completing your implementation, run it and confirm the verifier prints `PASS`.
