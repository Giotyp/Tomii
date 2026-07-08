---
title: Environment variables
sidebar_label: Environment
---

Tomii reads environment variables at two points: build time (driving the
`tomii-converter` wrapper generation) and runtime (A/B performance toggles
and Python-bridge plumbing).

## Build-time variables

`tomii-converter` parses a plugin source or header file (Rust `.rs` or C
`.h`/`.hpp`) and emits the wrapper functions and function registry that
`tomii-core` includes at build time. It is driven by three variables and is
normally invoked from `tomii-core`'s build script. See
[Rust plugins](/docs/guide/plugins/rust) and [C plugins](/docs/guide/plugins/c)
for the annotation formats.

| Variable | Required | Description |
|---|---|---|
| `FUNC_PATH` | yes, for builds using `tomii-converter` | Path to the plugin header or Rust source |
| `WRAP_PATH` | no | Wrapper functions file; auto-generated when unset |
| `REG_PATH` | no | Function registry file; auto-generated when unset |

The Python API sets these for you: `Graph.build(func_path=...)` maps its
`func_path`, `wrap_path`, and `reg_path` parameters onto these variables
(see [Python API](/docs/reference/python-api#graphbuild)).

### MIMO benchmark environment

The MIMO benchmark harness (`bench/mimo-bench/tomii/scripts/run_bench.sh`)
exports `FUNC_PATH` to the benchmark plugin source before building, and pins
the math libraries to one thread each:

```bash
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export OMP_NUM_THREADS=1
```

Keep these at 1 for benchmark runs — task-level parallelism is the
benchmark's mechanism, not intra-task library threads.

## Runtime A/B toggles

Two variables disable specific runtime optimizations. Both are read once at
runtime initialization and are presence-checked: setting the variable to any
value (including an empty string) activates the disable. They exist for
same-binary performance attribution — you measure with and without an
optimization by flipping an environment variable instead of rebuilding from
edited source, which keeps the comparison on identical machine code
elsewhere. Benchmarks in [benchmarks](/docs/overview/benchmarks) use this
mechanism.

| Variable | Effect when set |
|---|---|
| `TOMII_DISABLE_ARG_TEMPLATES` | Disable pre-computed argument templates |
| `TOMII_DISABLE_UNCHECKED_WRAPPERS` | Disable selection of converter-generated unchecked wrapper twins |

```bash
# Baseline
cargo run -p tomii-core --bin main -- --json graph.json --dylib plugin.so

# Same binary, arg templates off
TOMII_DISABLE_ARG_TEMPLATES=1 \
cargo run -p tomii-core --bin main -- --json graph.json --dylib plugin.so
```

## Python-bridge variables

When a graph uses [Python plugins](/docs/guide/plugins/python),
`Graph.run()` sets up the environment of the runner subprocess so the
embedded interpreter sees the same packages as the calling process. You
normally do not set these yourself, with one exception.

| Variable | Set by | Description |
|---|---|---|
| `TOMII_PARENT_PYTHON` | `Graph.run()` | Path to the real Python executable, so `@tomii.procs()` can spawn worker processes when `sys.executable` inside the Rust binary is not a Python interpreter |
| `TOMII_EXTRA_PYTHONPATH` | you (optional) | Extra directories appended to the `PYTHONPATH` the bridge receives |
| `PYTHONPATH`, `PYTHONHOME` | `Graph.run()` | Propagate the calling interpreter's packages and stdlib to the embedded interpreter |
| `LD_PRELOAD` | `Graph.run()` (Linux) | Preloads `libpython` for binaries built without `--features embed-python`, so C extensions resolve interpreter symbols at `dlopen` time |

## Logging

The runner binary uses `tracing` with an `EnvFilter`: `RUST_LOG` overrides
the log level implied by the `--debug` flag (see
[CLI](/docs/reference/cli)). For example, `RUST_LOG=debug` enables debug
logging without passing `--debug`.
