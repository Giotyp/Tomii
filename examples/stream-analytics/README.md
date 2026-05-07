# stream-analytics

Sensor-reading pipeline demonstrating Tomii's conditional branching, grouped barriers,
`$dep` ordering edges, and priority levels — without external dependencies.

## Graph topology

```
generate_reading (factor=32) ─► classify_reading ─► handle_anomaly  (priority: high)
                                                 └─► smooth_reading   (priority: low)
                               ─► compute_sensor_stats (factor=4, grouped barrier)
                                  └─► aggregate_results ─► write_report (variadic fan-in)
log_stream_event ($dep on all classify tasks)
```

With `anomaly_threshold = 5.0` (default), readings are classified as anomalous and
`handle_anomaly` fires. Raise the threshold to `10.0` in `graph.json` to exercise
the `smooth_reading` branch instead.

## Requirements

- Rust toolchain
- Python 3.10+

## Build and run

```bash
# From repo root:
python examples/stream-analytics/run_bench.py

# Tune concurrency:
python examples/stream-analytics/run_bench.py --workers 4 --slots 8
```

## Verify

```bash
python examples/stream-analytics/verify.py
```

Runs a single stream with a known threshold and checks that the output block in
`result.txt` matches `result.golden.txt` exactly. Prints `PASS` on success.

## Tuning knobs

| Flag | Default | Effect |
|------|---------|--------|
| `--workers` | 2 | Rayon worker threads |
| `--system-threads` | 3 | Resolution threads |
| `--slots` | 2 | Concurrent stream slots |
| `--max-streams` | 1 | Streams processed before exit |
| `--slot-priority` | off | Enable slot-level priority scheduling |
| `--no-clean` | — | Skip library rebuild |

## Output

Per-sensor summaries are written to `result.txt`. The `write_report` node collects
all `num_sensors=4` aggregate results via variadic fan-in before writing.
