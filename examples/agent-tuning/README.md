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
# Python deps for Bayesian arm
pip install optuna

# Python deps for Claude agent arm
pip install anthropic

# API key for arm 4
export ANTHROPIC_API_KEY=sk-ant-...
```

The stream-analytics dylib is built automatically if not already present.
A release build of `tomii-core` must exist at `target/release/main`.
Build it with:

```bash
cargo build --release -p tomii-core --bin main
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
| `arms/random.py` | Uniform random sampling (seed=42) |
| `arms/bayesian.py` | Optuna TPE (Tree-structured Parzen Estimator, seed=42) |
| `arms/grid.py` | Bounded cross-product grid (first N cells of 2048-cell full grid) |
| `arms/agent.py` | Claude (`claude-sonnet-4-6`) prompted with trial history |

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

## Honest framing

If the agent arm does not outperform the best non-adaptive baseline by at least 10%,
we report that honestly. The value of this example is the **structured tuning surface**
— a reproducible benchmark comparing adaptive vs non-adaptive search — not necessarily
the agent's optimization skill on this particular workload.
