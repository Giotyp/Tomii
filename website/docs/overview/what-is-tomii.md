---
title: What is Tomii?
sidebar_label: What is Tomii?
---

Tomii is a Rust task-graph runtime for packet-driven streaming pipelines. You
define a computation graph in Python (or directly as JSON), implement its
kernels in Rust, C, or Python, and Tomii executes the graph across up to 64
concurrent frames with worker threads pinned to cores.

Tomii is a research and prototyping framework. It is **not** a
general-purpose Taskflow or TBB replacement: for single-frame micro-task
DAGs where dispatch overhead dominates, those frameworks are faster. Tomii's
advantage appears in a specific niche: concurrent frames, generational slot
reuse, network-driven MIMO pipelines, and machine-driven optimization. The
[When to use Tomii](/docs/overview/when-to-use) page states the boundary
precisely, including the losses.

## Tripartite decoupling

Most streaming frameworks entangle three independent concerns: what you
compute (the graph), how each kernel is implemented, and how execution is
organized. Application-specific systems fuse all three into one codebase to
maximize performance; general frameworks bake the graph and kernels into
compiled application code. Either way, changing one concern means editing and
rebuilding the whole program.

Tomii separates them into three independently authored artifacts:

| Artifact | Form | Changes when |
|---|---|---|
| Graph specification | JSON (`graph.json`) | The pipeline topology changes |
| Kernel library | Compiled plugin (`.so`) | A computation changes |
| Runtime control | CLI flags / `run()` options | The execution strategy changes |

The graph references kernels by name. The runtime loads the kernel library
dynamically and never knows what language a kernel is written in. Runtime
behavior — workers, slots, scheduler, batching — is a bounded, documented
control surface.

## What the separation buys

Three structural properties follow from this architecture:

**The same compiled graph replays across concurrent frames.** Up to 64
slots each hold an independent instance of the graph, sharing initialized
objects across lanes. Completing a frame is an O(1) generational reset, not
a graph reconstruction. Network packets are first-class graph sources
(`$network`), ingested by dedicated receiver threads.

**Kernels compose across languages.** Functions annotated with
`#[tomii_export]` (Rust), `// @tomii_export` (C), or `@tomii.export`
(Python) become callable node names in one graph. The
[polyglot guide](/docs/guide/plugins/polyglot) shows the same pipeline
implemented three times, one language each, against the same graph.

**The runtime surface is machine-readable.** The graph is data, the graph
schema is published, and every tuning knob carries a type, a domain, and a
search hint. An optimizer — random search, Bayesian, or a language model —
can enumerate, evaluate, and iterate without recompilation. See
[agent-driven tuning](/docs/guide/tuning/agent-tuning).

## A twelve-line pipeline

```python
import tomii as tm

app = tm.Graph()

buf  = app.var("buf_size", 100)
plan = app.var("fft_planner", func="fft_planner", args=[buf])

gen  = app.node("gen_vec", func="generate_vector", factor=200, args=[buf])
fft  = app.node("compute_fft", func="compute_fft", factor=200,
                args=[plan, gen.out()])

app.build(func_path="plugin/src/lib.rs", plugin_manifest="plugin/Cargo.toml")
app.run(workers=4, slots=2)
```

`factor=200` creates 200 parallel instances of each node. `.build()` compiles
the plugin and generates wrappers from the `#[tomii_export]` annotations in
`plugin/src/lib.rs`. `.run()` starts the runtime. The
[first graph walkthrough](/docs/guide/getting-started/first-graph) explains
every line.

## Where to go next

- [Installation](/docs/guide/getting-started/installation) — `pip install tomii-rt`
- [When to use Tomii](/docs/overview/when-to-use) — the performance envelope
- [Comparison](/docs/overview/comparison) — Tomii next to Taskflow, TBB, Rayon, Dask, and Agora
- [Benchmarks](/docs/overview/benchmarks) — measured results and methodology
