---
name: SynStream skills index
description: Overview of SynStream agent skills and the optimization lifecycle
---

# SynStream Agent Skills

This folder contains structured workflow skills that enable AI agents to work effectively
with SynStream. Each skill is a self-contained markdown document that also follows the
Claude Code SKILL.md convention (YAML frontmatter + step-by-step instructions), so they
can be used as slash commands by copying them into `.claude/skills/`.

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
      ├─ overhead_pct > 60% ──► graph-coarsen ──► run-validate
      │
      ├─ overhead_pct 20-60% ──► knob-search ──► run-validate
      │                              │
      │                    (if still > 40%) ──► graph-coarsen
      │
      └─ overhead_pct < 20% ──► kernel optimization (outside SynStream)
```

## Skills

| Skill | File | Trigger |
|-------|------|---------|
| **project-discover** | [project-discover.md](project-discover.md) | Orient in an unknown SynStream project |
| **graph-build** | [graph-build.md](graph-build.md) | Build a graph from a computation description |
| **run-validate** | [run-validate.md](run-validate.md) | Execute, verify correctness, establish baseline |
| **diagnose** | [diagnose.md](diagnose.md) | Classify bottleneck from report.json |
| **knob-search** | [knob-search.md](knob-search.md) | Tune scheduler knobs (overhead_pct 20-60%) |
| **graph-coarsen** | [graph-coarsen.md](graph-coarsen.md) | Reduce task count (overhead_pct > 60%) |
| **plugin-author** | [plugin-author.md](plugin-author.md) | Write `#[synstream_export]` Rust/C functions |

## Quick-Start for Agents

1. Run `python -m synstream --list-knobs-json` to get all tunable parameters with search hints.
2. Run `python -m synstream --schema` to get the graph construction API with optimization hints.
3. After any run with `report="report.json"`, read `optimization_suggestions` for ranked actions.

See [AGENT.md](../AGENT.md) for a brief quick-reference on plugin authoring and the performance model.
