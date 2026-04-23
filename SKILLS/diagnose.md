---
name: diagnose
description: Use when report.json was just written and you need to know which axis to optimize. Returns the dominant bottleneck (scheduling / compute / imbalance) plus a ranked remediation list with specific next steps.
---

# Skill: diagnose

Classify the dominant performance bottleneck from `report.json` and produce a prioritized
remediation plan. The report provides four quantitative diagnostic dimensions that map
deterministically to action categories.

## Trigger

- Performance is unsatisfactory after running [run-validate](run-validate.md)
- User asks "why is this slow?" or "what should I optimize?"
- Automatically after any baseline measurement

## Steps

### 1. Read report.json

Open `report.json` and extract the four diagnostic dimensions:

**Dimension A — Scheduling overhead ratio:**
```
summary.scheduling_overhead_diagnostic.overhead_pct
summary.scheduling_overhead_diagnostic.interpretation
```

**Dimension B — Critical path structure:**
```
critical_path.length_nodes
critical_path.max_node_factor
critical_path.estimated_latency_us
```

**Dimension C — Worker utilization:**
```
resource_utilization.worker_busy_pct   (array, one entry per worker)
→ compute: min, max, mean, spread = max - min
```

**Dimension D — Per-node hotspots:**
```
per_node (array, sorted by total_exec_us descending)
→ identify top 3 nodes by total_exec_us, note on_critical_path for each
```

Also read:
- `optimization_suggestions` — ranked rule-engine output (priority 1 = most impactful)
- `bottleneck_hints` — free-text signals from the runtime

### 2. Apply the decision tree

```
overhead_pct > 60%?
    YES → Scheduling-dominated. Remedy: graph-coarsen.
          (Too many fine-grained tasks; scheduling cost exceeds compute cost.)

overhead_pct < 20%?
    YES → Compute-dominated. Remedy: kernel optimization.
          (Scheduling is negligible; the kernels themselves are the bottleneck.)

20% ≤ overhead_pct ≤ 60%?
    YES → Mixed profile. Try knob-search first; coarsen if knobs don't resolve it.

max(worker_busy_pct) < 50%?
    YES → Worker underutilization. Workers are mostly idle because the critical
          path serializes execution. Remedy: increase factor on CP nodes or
          restructure the graph for more parallelism.

spread(worker_busy_pct) > 30 percentage points?
    YES → Load imbalance. Workers have very uneven load. Remedy: review group_size,
          barrier placement, or use priority levels to rebalance.
```

Multiple conditions can apply simultaneously. Use `optimization_suggestions` to break ties.

### 3. Cross-reference optimization_suggestions

Each entry in `optimization_suggestions` has:
- `priority` (1 = highest)
- `category` (graph_topology | runtime_flags | parallelism)
- `description` and `action` (plain-English instructions)
- `knob` (what to change)
- `suggested_value` (concrete recommendation)
- `estimated_speedup` (e.g., "2x-4x")
- `confidence` (high | medium)

Apply priority-1 entries first. Rebuild and re-read the report before applying
lower-priority entries — coarsening often makes runtime-flag suggestions obsolete.

### 4. Read bottleneck_hints

`bottleneck_hints` contains free-text signals like:
- "Node X is on the critical path and accounts for Y% of total compute time"
- "Worker utilization imbalance: max=N%, mean=M%"
- "Critical path dominates Z% of average latency"

These complement the quantitative dimensions. Note any nodes flagged as dominant.

### 5. Optional: deeper worker analysis

If `timing_sched.csv` exists (produced by `timing="timing.txt"` + recording flags):

```bash
python3 scripts/analyze_sched.py timing_sched.csv --system-threads 1 --slots 1
```

This shows per-worker busy/idle breakdown, scheduling latency statistics, and
automated recommendations beyond what report.json provides.

For visual inspection:
```bash
python3 scripts/scheduler_visualize.py timing_sched.csv -o gantt.png
```

### 6. Produce prioritized action list

Output a structured diagnosis:

```
DIAGNOSIS
=========
overhead_pct: X%  →  <interpretation from report>
worker_busy: min=X%, max=X%, mean=X%, spread=X pp
critical_path: N nodes, max_factor=N, ~Xus

Bottleneck class: <scheduling | compute | underutilization | load_imbalance | mixed>

Actions (priority order):
1. [CATEGORY] <action description>
   Knob: <knob name>  Suggested value: <value>  Est. speedup: <range>
   → Next skill: <graph-coarsen | knob-search>

2. [CATEGORY] <action description>
   ...

Supporting evidence:
- overhead_pct=X% (threshold for graph-coarsen: 60%)
- max_node_factor=N (used by graph-coarsen Case A threshold: 64)
- top node by exec time: <name> (on_critical_path=<T/F>)
```

## Bottleneck class → skill mapping

| Bottleneck | Primary skill | Condition |
|-----------|---------------|-----------|
| Scheduling-dominated | [graph-coarsen](graph-coarsen.md) | overhead_pct > 60% |
| Mixed | [knob-search](knob-search.md) | 20% ≤ overhead_pct ≤ 60% |
| Compute-dominated | kernel optimization | overhead_pct < 20% |
| Worker underutilization | graph restructuring | max(worker_busy_pct) < 50% |
| Load imbalance | review group_size/priority | spread > 30pp |

## See also

- [knob-search](knob-search.md) — for mixed or knob-tunable bottlenecks
- [graph-coarsen](graph-coarsen.md) — for scheduling-dominated bottlenecks
- [AGENT.md](../AGENT.md) — decision rule summary (overhead_pct thresholds)
