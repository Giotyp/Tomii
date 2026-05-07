# mapreduce

Word-count Map→Reduce pipeline using a C plugin. Demonstrates the canonical
fan-out / fan-in pattern: N parallel map tasks feeding a single reduce node.

## Graph topology

```
generate_shard (x num_shards) ─► map_tokens (x num_shards) ─► reduce_all ─► result.txt
```

All functions are implemented in `src/wordcount.c` (pure C, no external dependencies).

## Requirements

- GCC
- Python 3.10+

## Build and run

```bash
# From repo root (builds libwordcount_c.so via Make, then runs):
python examples/mapreduce/run_bench.py

# Tune workload size:
python examples/mapreduce/run_bench.py --num-shards 32 --tokens-per-shard 512 --workers 4
```

## Verify

```bash
bash examples/mapreduce/verify.sh
```

Byte-compares `result.txt` against `result.golden.txt`. Prints `PASS` on success.
Run the benchmark at least once before verifying.

## Tuning knobs

| Flag | Default | Effect |
|------|---------|--------|
| `--num-shards` | 16 | Number of parallel map tasks |
| `--tokens-per-shard` | 256 | Tokens generated per shard |
| `--workers` | 2 | Rayon worker threads |
| `--max-streams` | 1 | Streams processed before exit |
| `--no-clean` | — | Skip library rebuild |

## Output

Word-frequency pairs written to `result.txt`, one `word count` pair per line,
sorted by word. `result.golden.txt` is the reference output for the default parameters.
