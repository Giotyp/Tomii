---
name: Τομί skills index
description: Overview of Τομί agent skills and the optimization lifecycle
---

# Τομί Agent Skills

This folder contains structured workflow skills that enable AI agents to work effectively
with Τομί. Each skill is a self-contained markdown document that also follows the
Claude Code SKILL.md convention (YAML frontmatter + step-by-step instructions), so they
can be used as slash commands by copying them into `.claude/skills/`
(`./SKILLS/install-skills.sh`).

## Optimization Lifecycle

```
project-discover
      │
      ▼
graph-build ──────► plugin-author (on-demand for new kernels)
      │                    ▲
      ▼                    │
run-validate ◄─────────────┘
      │
      ▼
  diagnose
      │
      ├─ network-input graph ──► network-ingest ──► run-validate
      │
      ├─ overhead_pct > 60% ──► graph-coarsen ──► run-validate
      │
      ├─ overhead_pct 20-60% ──► knob-search ──► run-validate
      │                              │
      │                    (if still > 40%) ──► graph-coarsen
      │
      └─ overhead_pct < 20% ──► kernel optimization (outside Τομί)
```

## Skills

| Skill | File | Trigger |
|-------|------|---------|
| **project-discover** | [project-discover.md](project-discover.md) | Orient in an unknown Τομί project |
| **graph-build** | [graph-build.md](graph-build.md) | Build a graph from a computation description |
| **run-validate** | [run-validate.md](run-validate.md) | Execute, verify correctness, establish baseline |
| **diagnose** | [diagnose.md](diagnose.md) | Classify bottleneck from report.json |
| **knob-search** | [knob-search.md](knob-search.md) | Tune scheduler knobs (overhead_pct 20-60%) |
| **graph-coarsen** | [graph-coarsen.md](graph-coarsen.md) | Reduce task count (overhead_pct > 60%) |
| **network-ingest** | [network-ingest.md](network-ingest.md) | Tune packet ingest for graphs with `network_config` |
| **plugin-author** | [plugin-author.md](plugin-author.md) | Write `#[tomii_export]` Rust / C / Python kernels |

## Quick-Start for Agents

1. `python -m tomii --list-knobs-json` — every `graph.run()` option with role, domain and search hint.
2. `python -m tomii --knob-space graph.json` — the generated search space for *this* graph
   (network-only knobs are dropped for graphs without `network_config`).
3. `python -m tomii --schema` — graph construction API with optimization hints.
4. `python -m tomii --dump graph.json` (GraphViz DOT) or `--visualize graph.json --ascii` — see the topology.
5. Run with **both** `report="report.json"` and `timing="timing.txt"`: the report is only
   written when timing is collected. Then read `optimization_suggestions` for ranked actions.

## Rules that apply to every skill

- `graph.run()` returns a `subprocess.CompletedProcess` and never raises on failure:
  check `returncode` and stderr after every run.
- `graph.run()` ignores keyword arguments it does not know. Spell knobs exactly as
  `--list-knobs-json` prints them (`max_frames`, not `frames`), or the setting is silently lost.
- Delete or rename the previous `report.json` before a run, so a run that wrote no report
  cannot be mistaken for a new result.
- Reference docs: `website/docs/reference/` (`cli.md`, `knob-catalog.md`,
  `json-graph-format.md`, `python-api.md`) and `website/docs/guide/`.
