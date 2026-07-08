---
title: Polyglot plugins
sidebar_label: Polyglot
---

The same DAG runs with kernels in three languages. The repository ships the
FFT + matrix-multiply pipeline three times — Rust (`examples/matrix-compute`,
nalgebra + rustfft), C (`examples/matrix-compute-C`, FFTW + OpenBLAS), and
Python (`examples/matrix-compute-python`, NumPy) — with identical topology
(`examples/README.md`):

```bash
python examples/matrix-compute/run_bench.py --workers 4
python examples/matrix-compute-C/run_bench.py --workers 4
bash   examples/matrix-compute-python/run_bench.sh --workers 4
```

All three compute the same result. The C example's `make validation` target
cross-checks its output against the Rust reference
(`examples/matrix-compute-C/README.md`).

## Why this works

The graph identifies kernels by function name only:

```json
{ "name": "compute_fft", "factor": "num_nodes", "function": "compute_fft", ... }
```

The runtime never sees kernel source. It loads one plugin shared library,
resolves each `"function"` string through the generated registry, and calls
through a C ABI. What differs per language is only how the registry entry is
produced:

| Language | You write | Converter input |
|---|---|---|
| Rust | `#[tomii_export] pub fn compute_fft(...)` | the annotated `.rs` source |
| C | `// @tomii_export(buffer: mut_array)` above the prototype | the annotated header |
| Python | `@tomii.export def compute_fft(v): ...` | none — the bundled bridge plugin dispatches by name |

This is the kernel half of tripartite decoupling (see
[What is Tomii](/docs/overview/what-is-tomii)): the graph, the kernels, and
the runtime are separate artifacts, so you can swap the kernel language
without touching the other two. No runtime source changes were needed for any
of the three examples (repository `README.md`).

## Mixing languages in one graph

Because nodes bind to registry names, a single graph can mix languages: any
function the loaded plugin library exports under the expected name satisfies
the node, regardless of what language produced it. The Python bridge is
itself an example — a Rust dylib whose registered functions call into Python.
The runtime loads one plugin library per run, so mixed-language kernels must
be linked or bridged into that one library.

## Choosing a language

- Rust: no system dependencies beyond cargo; the default for new plugins.
- C: reuse existing native libraries (FFTW, OpenBLAS, CUDA — see
  `examples/gpu-vectoradd`).
- Python: fastest iteration; parallel when kernels are NumPy/BLAS-bound or
  under free-threaded 3.13t (see [Python plugins](/docs/guide/plugins/python)).

The [examples page](/docs/guide/examples) lists what each variant requires.
