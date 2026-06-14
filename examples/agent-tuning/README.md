# agent-tuning: Four-arm search MVP for stream-analytics

This example demonstrates **agent-native graph tuning** — Tomii's key differentiator.
Because graph topology is pure data (JSON), an AI agent can edit it and tune runtime
knobs without recompilation. This directory benchmarks four search strategies head-to-head
over the same knob space with the same budget.

## What this demonstrates

Tomii's JSON-defined computation graphs make the tuning surface machine-readable. Any
optimizer — random, Bayesian, grid, or a language model — can enumerate candidate
configurations, run the verifier, and iterate. The four arms here share:

- the same `KnobConfig` search space (`knob_space.json`)
- the same evaluation budget (`--iterations N`)
- the same correctness gate (`verify.py` from stream-analytics)
- the same metric (`ms_per_stream` from the runtime report)

## Prerequisites

```bash
# Python deps (Bayesian arm requires Optuna; agent arm uses the claude CLI)
pip install -e ".[agent-tuning]"   # from the repo root — installs optuna>=3

# Agent arm (arm 4): requires the claude CLI on PATH
# Install Claude Code: https://claude.ai/code
# Verify with: claude --version
# No API key needed — uses your active Claude Code subscription.
```

The stream-analytics dylib and the `tomii-core` binary are built automatically
if not already present, **provided** `FUNC_PATH` is set to the stream-analytics lib:

```bash
# One-time build (required before the first run)
FUNC_PATH=$(pwd)/examples/stream-analytics/src/lib.rs \
    cargo build --release -p tomii-core --bin main
FUNC_PATH=$(pwd)/examples/stream-analytics/src/lib.rs \
    cargo build --release --manifest-path examples/stream-analytics/Cargo.toml
```

## Quick start

```bash
cd examples/agent-tuning
bash run_all.sh 50   # run all 4 arms with 50 iterations each
```

Each arm writes a `.jsonl` trial log to the results directory.

## Methodology

| Property | Value |
|---|---|
| Budget per arm | `--iterations` (default 50) |
| Streams per eval | 500 (50 warmup excluded) |
| Correctness gate | `examples/stream-analytics/verify.py` |
| Metric | `ms_per_stream` (from `report.json` summary) |
| Rejected trial | `verifier_ok=False` — not counted toward best |

All four arms use identical budget, threshold, and verifier. A trial is valid only if
the verifier passes for the full stream count.

## Arms

| File | Strategy |
|---|---|
| `arms/random_search.py` | Uniform random sampling (seed=42) |
| `arms/bayesian.py` | Optuna TPE (Tree-structured Parzen Estimator, seed=42) |
| `arms/grid.py` | Bounded cross-product grid (first N cells of 2048-cell full grid) |
| `arms/agent.py` | Claude (`claude-sonnet-4-6`) via `claude` CLI, prompted with trial history |

## Aggregate + plot

After a run, generate `summary.csv` and `comparison.png`:

```bash
python aggregate.py --results-dir results/run_<ts>
python plot.py --results-dir results/run_<ts>
```

`aggregate.py` computes per-arm best/mean ms, passing trial count, and improvement over
baseline. `plot.py` draws convergence curves (best-so-far ms vs iteration) for all arms.

## Results format

Each arm writes `results/<run_dir>/<arm>_trials.jsonl`. Each line is a JSON object:

```json
{
  "iteration": 3,
  "arm": "random",
  "notes": "",
  "knobs": {"workers": 4, "slots": 4, ...},
  "verifier_ok": true,
  "ms_per_stream": 0.1823,
  "rejection_reason": null,
  "wall_seconds": 2.41
}
```

A `baseline.json` file is written at the start of each run.

## Allowed edits

See `knob_space.json` for the full list of tunable CLI flags and what is forbidden.
The forbidden list protects correctness: `$barrier`, `$dep`, and `$res` arguments
encode data-dependency semantics that must not be removed.

## Results

Running `run_all.sh` writes per-arm `*_trials.jsonl` logs; `aggregate.py` and `plot.py`
then produce `summary.csv` and `comparison.png` (best-so-far convergence per arm) under
the run directory. Measured comparison numbers across the four arms are published in the
project documentation, not in this README.

## Honest framing

The value of this example is the **structured tuning surface**: a reproducible benchmark
showing that a language model can reliably stay in the valid region (verifier rejections
at zero) and converge efficiently, without access to source code or documentation beyond
the knob descriptions. On a small discrete knob space all adaptive strategies converge
quickly; the differentiator is the machine-readable graph, not a single-best win.
