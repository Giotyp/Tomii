---
title: Runtime knobs
sidebar_label: Knobs
---

Every `run()` option is a knob in a machine-readable catalog. The catalog is
the discovery surface for both humans and optimizers: names, CLI flags,
descriptions, roles, value domains, and search hints, all queryable from the
CLI.

## Discovering knobs

```bash
python -m tomii --list-knobs         # human-readable catalog
python -m tomii --list-knobs-json    # machine-readable, with search hints
```

Each JSON entry carries everything a search loop needs. One entry from
`--list-knobs-json`:

```json
{
  "name": "workers",
  "type": "int",
  "cli": "--workers",
  "role": "perf",
  "description": "Rayon worker threads (match physical cores)",
  "search_hint": "unimodal; binary search 1-physical_cores; diminishing returns past core count",
  "domain": { "kind": "int", "min": 1, "max": 128, "scale": "pow2" }
}
```

## Roles

Each knob has a `role`:

- `perf` — affects performance; part of the tuning space (`workers`, `slots`,
  `batching_size`, `coalesce_barriers`, ...).
- `env` — describes the environment or run setup rather than a tunable
  (`core_offset`, output paths, `debug`).

Search tools should only sweep `perf` knobs. The full catalog with roles and
domains is in the [knob catalog reference](/docs/reference/knob-catalog).

## Per-graph knob spaces

`--knob-space` generates a versioned tuning space for a specific graph:

```bash
python -m tomii --knob-space examples/stream-analytics/graph.json \
    --workload stream-analytics
```

The output (schema `version: 2`) combines two sources:

- catalog knobs with role `perf` (`kind: "cli"`), and
- graph knobs extracted from the JSON: shared factor variables, literal node
  factors, and `group_by` widths (`kind: "graph"`). For stream-analytics
  this adds `graph:init.num_sensors` and `graph:init.total_readings`.

The space also carries a `forbidden` list naming edits that break
correctness: removing `$barrier` args, removing `$dep`/`$res` args, changing
function names, adding or removing nodes. Graph knobs may still violate
workload invariants the generator cannot know, which is why searches over
this space are verifier-gated — see
[Agent tuning](/docs/guide/tuning/agent-tuning).

## A/B attribution toggles

Two environment variables disable specific runtime optimizations in the same
binary, read once at init. They exist for performance attribution: you
measure with and without a mechanism, without rebuilding.

| Variable | Disables |
|---|---|
| `TOMII_DISABLE_ARG_TEMPLATES` | Pre-computed argument templates |
| `TOMII_DISABLE_UNCHECKED_WRAPPERS` | Converter-generated unchecked wrapper twins |

Details in the [environment reference](/docs/reference/environment).

## Where to start

For manual tuning, the search hints encode the known structure: `workers` is
unimodal up to the physical core count, `slots` trades latency for
throughput (try 1, 2, 4, 8), `batching_size` matters for fine-grained graphs.
For automated tuning over the full space, use the harness described in
[Agent tuning](/docs/guide/tuning/agent-tuning), and read the run's
`report.json` to see whether scheduling overhead or compute dominates
([Observability](/docs/guide/tuning/observability)).
