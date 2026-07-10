# gpu-vectoradd

GPU vector-addition benchmark using a CUDA C++ plugin. Demonstrates CUDA host↔device
copy nodes pinned to dedicated worker threads via `use_workers`.

## Graph topology

```
gen_vec_a ─► copy_h2d_a ─►─┐
                            ├─► vadd_gpu ─► copy_d2h ─► validate
gen_vec_b ─► copy_h2d_b ─►─┘
```

`copy_h2d_a`, `copy_h2d_b`, `vadd_gpu`, and `copy_d2h` are pinned to workers `0-1`
via `use_workers="0-1"`, giving two dedicated GPU proxy threads each with their own
per-thread CUDA stream.

## Requirements

- NVIDIA GPU with CUDA 12+ (`nvcc` at `/usr/local/cuda-12.9/bin/nvcc` or update `Makefile`)
- Python 3.10+

## Build and run

```bash
# From repo root (builds libgpu_vadd.so via nvcc, then runs):
python examples/gpu-vectoradd/run_bench.py

# Scale up:
python examples/gpu-vectoradd/run_bench.py --workers 4 --max-frames 32 --vec-size 4194304
```

## Tuning knobs

| Flag | Default | Effect |
|------|---------|--------|
| `--workers` | 4 | Worker threads (≥2 recommended for GPU proxy) |
| `--system-threads` | 3 | Resolution threads |
| `--slots` | 2 | Concurrent frame slots |
| `--max-frames` | 4 | Frames processed before exit |
| `--vec-size` | 1048576 | Float elements per vector (1M default) |
| `--no-clean` | — | Skip library rebuild |

## Output

The `validate` node checks that `vadd_gpu` results equal `a + b` element-wise
and prints a pass/fail message per frame. Timing CSV is written when `--record`
is enabled (default).
