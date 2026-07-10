---
title: Installation
sidebar_label: Installation
---

Tomii ships as a Python package. The distribution name on PyPI is `tomii-rt`;
the used import name is `tomii`.

```bash
pip install tomii-rt
python -c "import tomii"
```

Version 1.2.0 ships wheels for Linux (x86_64, arm64) and macOS (Intel, Apple
Silicon) covering CPython 3.9–3.14, including the free-threaded 3.13t/3.14t
builds. Windows wheels are planned for a later release.

:::warning[A Rust toolchain is required]

The wheel contains the graph builder, not your kernels. `Graph.build()`
compiles the Tomii runtime binary and your plugin `.so` from source with
`cargo`. Install Rust before your first build:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

C plugins additionally need a C compiler; the C example uses GCC.
:::

## Install from source

Install from source when you want to develop Tomii itself, need a platform
without a prebuilt wheel, or want the examples and benchmarks alongside the
package. The build uses [maturin](https://www.maturin.rs/) via `pip`, so the
Rust toolchain above is required here too.

```bash
git clone https://github.com/Giotyp/Tomii.git
cd Tomii
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

Optional extras:

| Extra | Installs | For |
|---|---|---|
| `dev` | test + type-check toolchain | Developing Tomii |
| `viz` | matplotlib | `Graph.visualize()` plots |
| `agent-tuning` | optuna | The Bayesian arm of `examples/agent-tuning/` |

Combine them as needed, e.g. `pip install -e ".[dev,viz]"`.

Verify the source install the same way as the wheel — the knob catalog and
one example end-to-end:

```bash
python -m tomii --list-knobs
python examples/matrix-compute/run_bench.py --workers 4
bash examples/matrix-compute/verify.sh
```

The first `run_bench.py` invocation compiles the runtime and the example
plugin, so expect a few minutes of `cargo` output.

## Rust-only use

If you skip the Python builder and write graphs as JSON directly, the Rust
crates are published on crates.io: `tomii-core`, `tomii-types`, `tomii-macro`,
and `tomii-converter`.

```bash
cargo add tomii-core
```

The [CLI reference](/docs/reference/cli) covers the JSON + CLI workflow.

## Check the install

The package exposes a module CLI. If this prints the runtime knob catalog,
the install works:

```bash
python -m tomii --list-knobs
```

## Next

Build and run your first graph in
[Your first graph](/docs/guide/getting-started/first-graph).
