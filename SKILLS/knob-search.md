---
name: knob-search
description: Systematically search SynStream scheduler knobs using per-knob search hints from --list-knobs-json, converging on a locally optimal configuration within 5 iterations
---

# Skill: knob-search

Systematically search the scheduler knobs to reduce latency. Uses per-knob search hints
(from `--list-knobs-json`) to choose binary search vs. enumeration vs. boolean toggle for
each dimension. Converges in at most 5 iterations.

## Trigger

- [diagnose](diagnose.md) reports overhead_pct between 20-60% (mixed profile)
- User asks to tune runtime parameters or reduce latency
- After [graph-coarsen](graph-coarsen.md) brings overhead below 60%, for further tuning

## Setup

### Load per-knob search hints

```bash
python -m synstream --list-knobs-json
```

Each knob entry has a `search_hint` field specifying how to search it, for example:
- `workers`: "unimodal; binary search 1-physical_cores; diminishing returns past core count"
- `coalesce_barriers`: "try both; helpful when node factor >> worker count"
- `batching_size`: "unimodal; binary search 1-512; larger reduces overhead for fine-grained graphs"
- `inline_continuation`: "try both; often reduces latency for linear chains"
- `slot_priority`: "try both; helps with imbalanced graphs when slots > 1"

### Record baseline

Read `report.json` from the most recent run. Record:
```
baseline_config: {workers=N, slots=1, batching_size=1, coalesce_barriers=False,
                  inline_continuation=False, slot_priority=False, fifo=False}
baseline_avg_latency_us: X
baseline_p99_latency_us: X
```

## Iteration protocol (up to 5 iterations)

### Iteration 1: Boolean knobs

Try each boolean knob independently. For each, toggle from the baseline and run:

```python
app.run(
    dylib=build_result.dylib,
    workers=<baseline_workers>,
    max_streams=20,
    exclude_streams=5,
    report="report.json",
    coalesce_barriers=True,   # toggle this knob
    # all other knobs at baseline
)
```

Knobs to try:
1. `coalesce_barriers=True` — helps when `max_node_factor >> workers` (check `critical_path.max_node_factor`)
2. `inline_continuation=True` — helps for graphs with long linear chains (check `critical_path.length_nodes > 20`)
3. `slot_priority=True` — helps when `slots > 1`; skip if running with `slots=1`

> **Warning**: `coalesce_barriers=True` suppresses `total_tasks_per_stream` in subsequent
> `report.json` outputs (field becomes `null`). Confirm graph structure is correct before enabling.

For each, record: `{knob, value, avg_latency_us, delta_pct}`.
Keep the value that gives the lowest `avg_latency_us`.
**Gate**: if improvement < 1%, leave the knob at its baseline value.

### Iteration 2: Binary-search workers

Search hint: "unimodal; binary search 1-physical_cores"

```python
import os
cores = os.cpu_count()
# Binary search: lo=1, hi=cores, target=minimum avg_latency_us
lo, hi = 1, cores
while hi - lo > 1:
    mid = (lo + hi) // 2
    run with workers=mid, record avg_latency_us
    run with workers=mid+1, record avg_latency_us
    if workers=mid+1 is better: lo = mid
    else: hi = mid
best_workers = lo or hi (whichever gave lower latency)
```

**Gate**: skip this step if workers is already at the baseline best from prior knowledge.

### Iteration 3: Binary-search batching_size

Search hint: "unimodal; binary search 1-512; larger reduces scheduling overhead for fine-grained graphs"

```python
# Try powers of 2: 1, 2, 4, 8, 16, 32, 64, 128, 256, 512
# Start at 1, double until latency stops improving, then binary search around the knee
candidates = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
for bs in candidates:
    run with batching_size=bs, record avg_latency_us
# pick the bs that minimizes avg_latency_us
```

**Gate**: if the improvement from `batching_size=1` to best is < 1%, skip.

### Iteration 4: Sweep batching_limit

Try values: 1, 2, 4, 10 (microseconds, the max wait before flushing a partial batch).

```python
for bl in [1, 2, 4, 10]:
    run with batching_limit=bl, best batching_size from iter 3, record avg_latency_us
```

**Gate**: if the best `batching_limit` gives < 1% improvement over the default (10), skip.

### Iteration 5: Verify and check variance

Run the best configuration 3 times:

```python
for trial in range(3):
    run with best_config, max_streams=30, exclude_streams=5, report=f"report_trial{trial}.json"
```

For each trial, read `avg_latency_us`. Compute mean and std_dev across the 3 trials.

If `std_dev > 0.1 * mean`:
- Increase `max_streams` to 50 or `exclude_streams` to 10
- Re-run 3 trials with the larger sample

## Logging

Maintain a trial log throughout all iterations:

```
TRIAL LOG
=========
iter | knob              | value | avg_latency_us | delta_pct
-----|-------------------|-------|----------------|----------
1    | coalesce_barriers | True  | X.X            | -Y.Y%
1    | inline_cont.      | False | X.X            | +Z.Z%
2    | workers           | 8     | X.X            | -Y.Y%
...
```

## Output

```
KNOB-SEARCH RESULT
==================
Baseline:     avg_latency_us=X  (config: workers=N, ...)
Best config:  avg_latency_us=X  (-Y.Y% improvement)
              workers=N, slots=1, batching_size=N, batching_limit=N,
              coalesce_barriers=True/False, inline_continuation=True/False,
              slot_priority=True/False

Trial log: <see above>

If improvement > 5%: run [run-validate](run-validate.md) to confirm with full stream count.
If overhead_pct is still > 40% after best config: proceed to [graph-coarsen](graph-coarsen.md).
```

## See also

- [diagnose](diagnose.md) — to understand which bottleneck class warrants knob-search
- [graph-coarsen](graph-coarsen.md) — if knob-search doesn't resolve the bottleneck
- [run-validate](run-validate.md) — to confirm the final configuration with a full measurement
- [AGENT.md](../AGENT.md) — performance model and `coalesce_barriers` warning
