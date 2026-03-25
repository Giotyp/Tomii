# Task: Implement Wavefront in Taskflow

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

Taskflow is a header-only C++17 library. The headers are in `taskflow-lib/` in this workspace.

Read the headers to learn the API. Compile with:

```
g++ -O3 -std=c++17 -Itaskflow-lib -lpthread
```

## What to create

In `<WORKSPACE>`, create from scratch:

- `wavefront.cpp` — C++ implementation
- Optionally a `Makefile`

## Output requirements

- Write `report.json` containing at minimum `{"summary": {"avg_latency_us": <float>}}`
- Print per-iteration timing to stdout
- After running, expose `grid[N-1][N-1]` (the bottom-right corner cell) and call:
  ```
  python verify_wavefront.py --n N --corner VALUE
  ```
  where `VALUE` is your computed `grid[N-1][N-1]`. Confirm it prints `PASS`.

## Verification

After completing your implementation, build and run it.
Confirm the verifier prints `PASS`.
