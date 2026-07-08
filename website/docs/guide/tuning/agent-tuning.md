---
title: Agent tuning
sidebar_label: Agent tuning
---

Because a Tomii graph is data and its knobs are a machine-readable catalog,
any optimizer can tune a workload: enumerate the space, propose a
configuration, run the verifier, measure, iterate. No recompilation, no
source access. `examples/agent-tuning` benchmarks four such optimizers
head-to-head.

## The structured surface

Every arm consumes the same generated space
(`python -m tomii --knob-space`, see [Runtime knobs](/docs/guide/tuning/knobs)):
runtime knobs with role `perf` plus per-graph knobs (shared factor variables,
literal node factors, `group_by` widths). The space's `forbidden` list names
the edits that encode data-dependency semantics — `$barrier`, `$dep`, and
`$res` arguments must not be removed.

## Verifier gating

Graph knobs can violate workload invariants the space generator cannot know.
So every trial runs a hard-coded correctness gate before its performance
counts:

| Workload | Gate |
|---|---|
| `stream-analytics` | golden-file `verify.py` per trial |
| `pipeline` | knob-aware verify pass with numeric checks vs a Python reference |
| `mimo` | keep-up gate: streams processed must equal frames sent |

A trial that fails its gate is logged with `verifier_ok: false` and a
rejection reason, and is excluded from the best-result comparison. An edit
that drops a barrier or removes a `$dep` edge fails the verifier and is
rejected (`examples/agent-tuning/README.md`). The MIMO gate exists so a
configuration cannot look fast by dropping frames.

## The four arms

| Arm | Strategy |
|---|---|
| `arms/random_search.py` | Uniform random sampling (seed=42) |
| `arms/bayesian.py` | Optuna TPE (seed=42) |
| `arms/grid.py` | Bounded cross-product grid |
| `arms/agent.py` | Claude via the `claude` CLI, prompted with trial history |

All four share the same space, budget (`--iterations`), fixtures, and
verifier. The agent receives only the knob descriptions and its own trial
history — no source code, no documentation.

## Running it

```bash
cd examples/agent-tuning
bash run_all.sh 50                                  # all 4 arms, stream-analytics
bash run_all.sh 50 pipeline
bash run_all.sh 30 mimo --streams 200 --warmup 20   # needs Agora sender + MKL
```

Each arm writes a `.jsonl` trial log; one line per trial:

```json
{
  "iteration": 3,
  "arm": "random",
  "knobs": {"workers": 4, "slots": 4},
  "verifier_ok": true,
  "ms_per_stream": 0.1823,
  "rejection_reason": null,
  "wall_seconds": 2.41
}
```

`aggregate.py` computes per-arm best/mean and pass counts; `plot.py` draws
best-so-far convergence curves (`examples/agent-tuning/README.md`).

## What the result means — and what it does not

The headline result is that the agent stays in the valid region of a space
where naive strategies waste most of their budget on invalid configurations.
On the generated ~14-million-cell stream-analytics space, random search
produced one valid configuration in 50 trials; the agent passed the verifier
in 41 of 50, won single-trial best on all three workloads, and used ~9× less
wall time than random search. It converges without source access — the agent
receives only knob descriptions and its own trial history.

Two honest caveats from `examples/agent-tuning/README.md`:

- On a small hand-written knob space (runtime knobs only), all four arms
  converge to within 0.04 ms with zero rejections. Small spaces do not
  separate the strategies; the generated space with graph knobs does.
- Graph knobs are disabled for the MIMO workload, because edited MIMO graphs
  have no per-trial output verification.

Measured cross-arm numbers are published on the
[benchmarks page](/docs/overview/benchmarks).
