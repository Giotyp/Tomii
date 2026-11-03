# Τομί Agent Skills

This folder is a Claude Code plugin (`tomii-skills`) of structured workflow skills that
enable AI agents to work effectively with Τομί. Each skill is a directory
`skills/<name>/` with a `SKILL.md` (YAML frontmatter + step-by-step instructions) and,
where useful, reference files and scripts that the agent loads only when needed.

## Install

As a plugin (recommended; skills are then available as `/tomii-skills:<name>` and are
picked up automatically when relevant):

```
/plugin install tomii-skills --marketplace Giotyp/Tomii
```

or, from a local checkout: `/plugin marketplace add /path/to/Tomii` then
`/plugin install tomii-skills@tomii`. For quick local use: `claude --plugin-dir SKILLS`.

Copying into a project's `.claude/skills/` (for harnesses that read that directory):

```bash
./SKILLS/install-skills.sh [project-dir]
```

The `SKILL.md` format is the open Agent Skills format, so other agents that read
`SKILL.md` directories can use `skills/` as is.

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

| Skill | Path | Trigger |
|-------|------|---------|
| **project-discover** | [skills/project-discover/](skills/project-discover/SKILL.md) | Orient in an unknown Τομί project |
| **graph-build** | [skills/graph-build/](skills/graph-build/SKILL.md) | Build a graph from a computation description |
| **run-validate** | [skills/run-validate/](skills/run-validate/SKILL.md) | Execute, verify correctness, establish baseline |
| **diagnose** | [skills/diagnose/](skills/diagnose/SKILL.md) | Classify bottleneck from report.json |
| **knob-search** | [skills/knob-search/](skills/knob-search/SKILL.md) | Tune scheduler knobs (overhead_pct 20-60%) |
| **graph-coarsen** | [skills/graph-coarsen/](skills/graph-coarsen/SKILL.md) | Reduce task count (overhead_pct > 60%) |
| **network-ingest** | [skills/network-ingest/](skills/network-ingest/SKILL.md) | Tune packet ingest for graphs with `network_config` |
| **plugin-author** | [skills/plugin-author/](skills/plugin-author/SKILL.md) | Write `#[tomii_export]` Rust / C / Python kernels |

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
