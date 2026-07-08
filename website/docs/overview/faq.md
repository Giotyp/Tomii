---
title: FAQ
sidebar_label: FAQ
---

### Is Tomii production-ready?

Tomii is a research and prototyping framework. The runtime is tested,
benchmarked, and verifier-gated, but the project's goal is fast iteration on
streaming pipeline structure — not displacing fused production systems at the
absolute latency limit. See [When to use Tomii](/docs/overview/when-to-use).

### Why is the package called `tomii-rt` when the import is `tomii`?

PyPI rejected the name `tomii` as too similar to an existing project. The
distribution is `tomii-rt`; the import name is `tomii`:

```bash
pip install tomii-rt
python -c "import tomii"
```

### Which platforms are supported?

Linux x86_64, with wheels for CPython 3.9–3.13. macOS, Windows, and ARM
wheels are planned. Building plugins requires a Rust toolchain regardless of
platform — see [Installation](/docs/guide/getting-started/installation).

### Why JSON for the graph?

Because data is easier to generate, validate, diff, and edit — by humans and
machines — than code. The JSON topology is what lets an optimizer or an LLM
propose graph changes without recompiling anything, and what lets the same
graph run against kernels in three languages. If you prefer not to write JSON
by hand, the Python builder emits it for you.

### Can the graph change shape at runtime?

No. The graph is compiled once at startup and replayed across all slots.
Data-dependent fan-out and dynamic node creation are out of scope; that
constraint is what makes O(1) generational slot reset and pre-computed
routing tables possible. If your topology is dynamic, use a framework built
for that (see the [comparison](/docs/overview/comparison)).

### How many concurrent streams can one process run?

Up to 64 slots. Each slot holds an independent instance of the graph;
initialized objects (`$ref`) are shared across slots without copying.

### Do Python kernels hold the GIL?

Python kernels run through the Python bridge and respect the interpreter's
threading model. On free-threaded builds (3.13t) kernels run without a GIL.
NumPy-dominated kernels release the GIL inside NumPy calls on standard
builds. See [Python plugins](/docs/guide/plugins/python).

### Can I mix languages in one graph?

Yes — that is a design goal. Kernels exported from Rust, C, and Python are
all registered by name in one function registry; a single graph can reference
any of them. See [One DAG, three languages](/docs/guide/plugins/polyglot).

### What schedulers are available?

Rayon (default), FIFO, a lock-free priority scheduler (`--custom`), and a
plugin interface for bringing your own. See the
[CLI reference](/docs/reference/cli) and the knob catalog.

### Where are the Rust API docs?

The Rust crates (`tomii-core`, `tomii-types`, `tomii-macro`,
`tomii-converter`) are documented on docs.rs, and the runtime internals in
[`ARCHITECTURE.md`](https://github.com/Giotyp/Tomii/blob/main/tomii-core/src/runtime/ARCHITECTURE.md).
This site documents the Python API, the JSON graph format, and the CLI.

### What license?

Apache 2.0, for the runtime, the Python package, and the examples.
