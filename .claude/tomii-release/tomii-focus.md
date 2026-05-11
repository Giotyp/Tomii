# Tomii Open-Release Focus: Application Positioning

## 1. Executive Summary

The framing decision below is **provisional** and gated on Phase 0. Default position heading into that work: **Tomii is a research and prototyping framework for streaming, MIMO, and agent-tuneable task graphs — not a Taskflow/TBB replacement for single-stream micro-task DAGs.** The measured perf wins (slot reuse 151×, concurrent-stream memory 2.8×, bilateral denoising parity, agent-tuned MIMO gains) are real but limited to Tomii's niche. The measured losses (anti-diagonal wavefront ~2.4× behind TBB/Taskflow, MIMO raw latency 3–8× behind Agora) are intrinsic to tripartite decoupling and cannot be explained away. The release should be explicit about both. The two flagship perf workloads and two flagship ergonomics workloads described below are the concrete deliverables that make this framing credible.

---

## 2. Phase 0 — Re-evaluation Matrix

### 2.1 Matrix axes

Run a head-to-head benchmark matrix placing each framework on its **home-turf workload** *and* on **Tomii's home-turf workloads**. The current evaluation in the paper mostly runs comparators on Tomii-chosen workloads; Phase 0 fills the other half.

| # | Workload class | Designed for | Tomii fit | Why we run it |
|---|---|---|---|---|
| 1 | Anti-diagonal wavefront, single stream, fine-grained (N=128–2048) | Taskflow / TBB | Poor — intrinsic cost | Confirm the ~2.4× loss is structural across N, not a config accident |
| 2 | Block-DAG wavefront, W=2–8 | Taskflow | Good | Paper shows Tomii ≈ or slight win here; replicate cleanly and shippably |
| 3 | Pure `parallel_for` reduction (sum, map) | TBB | None — out of expressive surface | Honestly document where Tomii cannot compete (TBB wins, Tomii can't express it) |
| 4 | Iterative dataflow with cycles, low latency | Timely / Naiad | Partial (static topology) | Tests whether static-topology assumption hurts vs Timely's incremental model |
| 5 | Linear pipeline, S concurrent streams (S=1, 4, 16, 64) | Tomii (slot reuse) | Partial — win on memory, 1.2–1.4× throughput gap at S≥16 | **Stage A complete** (`pipeline-bench/audit-2026-05-07.md`): heavy kernel (16 µs/task), W×S sweep done; Taskflow comparator in place. Revised framing: gap closes from 2.5× at S=1 to 1.33× at S=16 (W=4). |
| 6 | Packet-driven MIMO-style uplink, 4×4 | Tomii / Taskflow | **Done — Tomii wins 1.26–1.39×** | Streaming overlap advantage (per-packet dispatch). Taskflow S=1 broken (single-slot stall). Full report: `.claude/tomii-release/mimo-bench-desc.md` |
| 7 | Map-reduce shallow fan-in/out | TBB / Taskflow | Good | Neutral ground; should match within accidental-overhead margin |
| 8 | Polyglot kernel pipeline (Rust + C + Python kernels, one DAG) | Tomii only | Strong | Not perf — ergonomics only; no comparator can do this without source-code changes |

### 2.2 Comparator implementation plan

- **Taskflow C++**: rows 1, 2, 5, 6, 7. Taskflow ships an anti-diagonal example; rows 1 and 2 mostly reuse paper setup from `paper/text/5-evaluation.tex:107-157`.
- **Intel TBB**: rows 1, 3, 7. TBB `parallel_for` on rows 3 and 7; TBB `parallel_pipeline` on row 5 as a check.
- **Timely-Rust**: row 4 only.
- **Agora** (internal comparison, not shipped): row 6 only, internal measurement, compare to the public Tomii port.

New code: rows 4 (Timely port), 5 (multi-stream Taskflow port), 6 (dependency-free 4×4 MIMO Taskflow port), 8 (no comparator, ergonomics only).

### 2.3 Metrics per cell

For every framework × workload cell, record:

| Metric | How |
|---|---|
| Throughput / latency (p50, p99) | `timing.txt` / RDTSC timing already in Tomii bench infra |
| Peak resident memory | `valgrind --tool=massif` or `/proc/self/status` snapshot at peak |
| Lines of application code | `cloc` excluding build glue, counted once per cell |
| Correctness | Verifier exit code (see §2.4 and §4.2); all cells must pass before perf is recorded |
| Implementation notes | One paragraph: *what was awkward or impossible to express in this framework for this workload* |

### 2.4 Methodology rules

These rules apply to every cell without exception. They fix the methodology gap from the bench-branch runs that used per-framework thresholds.

1. **Identical perf threshold across frameworks.** One threshold per workload row — e.g. *"latency under X µs at S=16 streams"* — derived from the best-in-class result in that row and applied uniformly to all frameworks. Document the threshold and its derivation inline with the row result. Frameworks that cannot hit the threshold are recorded as failing, not given a softer threshold.
2. **Identical input fixtures.** Same seeds, same input sizes, same warmup count, same iteration count. If a framework structurally cannot run the same fixture (e.g. TBB on a cyclic-dataflow row), record that as a structural finding.
3. **Identical hardware and measurement clock.** Single machine, recorded CPU model and core count, single pinned core-set, RDTSC or `clock_gettime` chosen once for a workload and held constant across frameworks in that row.
4. **Correctness gates perf.** No framework's perf number is recorded for a cell unless a verifier confirms semantic correctness on that cell's output (see §4.2).

### 2.5 Framing gates

After Phase 0, apply the following decision rules:

- **If row 5 or row 6 shows Tomii ≥ Taskflow on perf at S≥4**: promote that row to a flagship perf story and strengthen the *niche perf* framing rather than the pure *prototyping* framing.
- **If rows 1–4 confirm the gaps are structural and consistent across N**: lock in the prototyping-framework framing and present the honest scoreboard prominently in the README.
- **If row 4 (Timely) shows that Tomii and Timely are close on Tomii's streaming workloads**: add Timely to the related-work comparison to strengthen the claims about static-topology advantages.
- **Regardless**: Phase 0 produces the honest scoreboard table used in §3.1, the README, and the camera-ready paper evaluation update.

**Owner / time**: ~2 weeks if Taskflow/Timely ports are written from scratch; ~1 week if existing `notes/antidiag-overhead.md`-referenced Taskflow bench sources are lifted directly.

---

## 3. Concern #1 — Applications Where Tomii Has an Advantage

### 3.1 Honest scoreboard (current state, to be refreshed by Phase 0)

| Scenario | Tomii vs comparator | Result | Intrinsic or accidental? |
|---|---|---|---|
| Generational-reset slot reuse, N=16,384 | vs Taskflow eager | **Tomii 151× faster** (`5-evaluation.tex:57`) | Intrinsic win |
| Per-slot memory, S=8 | vs Taskflow instance | **Tomii 2.8× less** (96 B vs 271 B) (`:103-104`) | Intrinsic win |
| Slot scaling, S=1→8, linear chain | vs Taskflow sequential | **Tomii 8.01× at S=8** (`:84-87`) | Intrinsic win |
| Multi-stream pipeline throughput, S=16, W=4, N=256, ~16 µs/task | vs Taskflow clone | **Tomii 1.31× slower** (`pipeline_sweep_post_u7c.csv`) | Accidental overhead; closes from 2.45× at S=1; best cell S=64,W=8: 1.36× |
| Bilateral denoising, tile=128, W=4–8 | vs Taskflow | **Tomii 2.8–6.9 % faster** (`:209-213`) | Mixed |
| Block-DAG wavefront, W=8 sweet spot | vs Taskflow | **Tomii ~19 % faster** (notes/antidiag) | Accidental win (topology) |
| Agent-driven MIMO tuning, 16×16 | vs untuned Tomii | **2.8–12.2 % latency reduction** (`:248`) | Intrinsic to structured surface |
| Anti-diagonal wavefront W=1, N=512 | vs TBB / Taskflow | **Tomii ~2.4× slower** (`:123-125`) | Primarily intrinsic |
| Massive-MIMO 8×8 latency | vs Agora | **Tomii ~8× slower** (`:243`) | Primarily intrinsic |
| Massive-MIMO 16×16 / 64×16 | vs Agora | **Tomii 3–4× slower** (`:231-234`) | Primarily intrinsic |
| `parallel_for` reduction | vs TBB | **Tomii cannot express** | Architectural |

### 3.2 Flagship perf #1 — Public Massive-MIMO (mimolib-based)

**Status (2026-05-08): COMPLETE. Full S×W sweep recorded; Tomii wins 1.26–1.39× at all tested configurations.**

`bench/mimo-bench/` implements a real-PHY MIMO uplink benchmark (fft→csi→beam→demul, decode dropped) using cleaned mimolib plugin + Taskflow C++ comparator linking identical Intel `.so` libraries.

**What it proves.** (i) Tomii-vs-Taskflow comparison with kernel-parity guarantee: same C-ABI symbols, same `.so` binaries — timing differences are pure scheduling overhead. (ii) All distinguishing Tomii topology features on the critical path: 3 barriers, 2 `group_by` clauses, 4 factor expressions. (iii) Slot-scaling story at S={1,4,16,64}. (iv) Tomii's streaming overlap advantage (per-packet dispatch) is architecturally quantified.

**Headline result (W=4, S=4): Tomii 0.926 ms vs Taskflow 1.168 ms — 1.26× Tomii win.**

The advantage is consistent across all (W, S) cells tested (1.26–1.39×) and is structural: Tomii dispatches FFT tasks as each UDP packet arrives; Taskflow must collect all packets before submitting the full DAG. With ~17.9 µs/packet spacing over a ~1 ms frame, this overlap recovers 280–360 µs that Taskflow cannot.

**Completed work:**

| Component | Status | Location |
|---|---|---|
| mimolib Phase 1 cleanup | **Done** | `examples/mimolib/` — dead-code purge, `#[tomii_export]` migration, `wrap/` deleted (~2,200 LoC removed) |
| Tomii harness (4-node, decode dropped) | **Done** | `mimo-bench/tomii/` — Cargo build, graph_4nodes.json, tddconfig-4x4.json, run_bench.py |
| Taskflow C++ port | **Done** | `mimo-bench/taskflow/` — links same Intel `.so` files; tf_mimo binary builds clean; first-pkt→done metric aligned |
| Sweep results | **Done** | `tomii/results/mimo_sweep.csv`, `taskflow/build/tf_mimo_sweep.csv`; 200 streams × 16 cells each |
| README + methodology doc | **Done** | `mimo-bench/README.md`, `mimo-bench/mimo-bench-desc.md` |
| Comparison plot | **Done** | `mimo-bench/mimo-comparison.png` |
| Verifier | **Done** | `mimo-bench/tomii/verify.py` — determinism check via post-demul hash |
| Release report | **Done** | `.claude/tomii-release/mimo-bench-desc.md` |

**Full results:**

Tomii ms_per_slot (first-pkt→done):

| W\S | S=1   | S=4   | S=16  | S=64  |
|-----|-------|-------|-------|-------|
| W=1 | 3.259 | 0.927 | 0.923 | 0.923 |
| W=2 | 2.412 | 0.928 | 0.928 | 0.925 |
| W=4 | 2.173 | 0.926 | 0.929 | 0.928 |
| W=8 | 1.806 | 0.924 | 0.923 | 0.925 |

Taskflow ms_per_slot (first-pkt→done; S=1 broken — single-slot stall):

| W\S | S=4   | S=16  | S=64  |
|-----|-------|-------|-------|
| W=1 | 1.283 | 1.280 | 1.278 |
| W=2 | 1.191 | 1.259 | 1.199 |
| W=4 | 1.168 | 1.169 | 1.183 |
| W=8 | 1.210 | 1.171 | 1.176 |

**Honest framing.** Decode dropped (FlexRAN is non-redistributable, ~4% of compute). Agora sender is an external dep (documented in README). At 4×4, both systems are sender-rate limited — worker scaling is flat. The advantage is architectural and would persist at larger antenna counts.

**16×16 config deferred.** The `tddconfig-sim-ul.json` (16×16, FFT=2048, 1200 subcarriers, 16 pilot symbols) with Agora's measured baseline of 1.62 ms ± 9.5 µs is a natural second configuration. At that scale the workload is compute-limited rather than sender-rate limited; streaming overlap (~400-800 µs saved) would be a smaller fraction (~10-15%) of total latency. Worker scaling and the Tomii-vs-Agora 3-4× overhead gap would both be visible. This is left as a follow-on sweep: the 4×4 result is sufficient to establish the streaming-overlap argument, and the 16×16 gap vs Agora is already documented in the paper (`:231-234`) and §3.1 scoreboard.

### 3.3 Flagship perf #2 — Multi-stream pipeline (S-scaling benchmark)

**Status (2026-05-07): Stage A audit complete.** `pipeline-bench/audit-2026-05-07.md` documents the metric mismatch (Tomii reported per-stream latency; Taskflow reported amortised throughput), the workload fix (TRANSFORM_ITERS=2048, ~16 µs/task), and the corrected S×W sweep results. See §5a Upgrade 6 (bench) for full numbers.

**Revised framing (post-U7c, 2026-05-07).** Tomii does not win outright vs Taskflow at S≥4. The corrected story is: *gap closes from 2.45–2.61× at S=1 to 1.28–1.36× at S≥16 across all W levels, demonstrating that multi-slot amortisation is measurable for workloads with ≥16 µs per-task compute.* Memory is 2.8× lower at all S levels regardless of throughput.

**Honest caveats now documented:**
1. At S=1 Tomii's overhead is a fixed ~5 ms per stream — not suitable for sub-µs tasks.
2. Per-stream latency grows with S (284 ms at S=64, W=1); throughput improves, latency does not.
3. The ~16 µs threshold was chosen to reduce scheduling overhead below 15% of compute.

**Remaining work for flagship #2:**
- [x] Verify Upgrade 7c (inline primitives) reduces the gap further; re-run full S×W sweep. (`pipeline_sweep_post_u7c.csv`, 2026-05-07)
- [x] Add `pipeline-bench-desc.md` documenting the fixed methodology. (`pipeline-bench/pipeline-bench-desc.md`, 2026-05-07)
- [x] Add correctness verifier. (`pipeline-bench/tomii/verify.py`, 2026-05-07; checks all-streams determinism + 30% relative range guard.)
- [ ] Confirm memory comparison (2.8× figure) with direct measurement, not just inference from code.

**Goal.** A real (non-synthetic) linear-to-shallow-DAG pipeline run at S = {1, 4, 16, 64} concurrent streams against a Taskflow port. The comparison axis is *streams in flight*, not raw per-task throughput.

**What it proves.** Tomii's generational-reset and per-slot memory advantages get larger as S grows. At S≥16 the throughput gap narrows to ~1.2–1.4× even against Taskflow's highly-optimised clone path. This is the missing graph in the paper (`5-evaluation.tex:69-94` is today synthetic-only).

**Reuse / new code:**

| Component | Status | Source |
|---|---|---|
| Workload | **Done** | `pipeline-bench/tomii/src/lib.rs` (heavy kernel, TRANSFORM_ITERS=2048) |
| `--slots` sweep | **Done** | `pipeline-bench/tomii/run_bench.py` + `pipeline-bench/taskflow/run_bench.py` |
| Taskflow port | **Done** | `pipeline-bench/taskflow/src/main.cpp` (identical heavy kernel) |
| Result CSVs | **Done** | `pipeline_sweep_heavy.csv` for both systems |
| Comparison plot | **Done** | `pipeline-bench/pipeline-comparison.py` |
| Verifier | Not yet | Add correctness check for aggregate output against expected mean |

**Success metric.** S-scaling throughput curves with Tomii and Taskflow, p99 latency, peak memory. Tomii does not win on throughput but closes the gap to ≤1.44× at S≥16. Phase 0 row 5.

### 3.4 Candidate workloads considered and rejected

| Workload | Why rejected |
|---|---|
| Audio DSP pipeline | No distinguishing MIMO/slot-reuse structure; Tomii would match Taskflow at best |
| ML inference pipeline | Dynamic batching typically better served by frameworks with dynamic-graph support (TorchScript, Ray Serve); Tomii's static topology is a drawback |
| Graph analytics (BFS/SSSP) | Irregular memory access, work-stealing advantages Taskflow; Tomii cannot express data-dependent topology |
| Video transcoding | Large per-task work hides dispatch overhead — Tomii and Taskflow would be indistinguishable; no niche story |

---

## 4. Concern #2 — Reframing as a Prototyping / Research Framework

### 4.1 Why the intrinsic costs justify reframing

The intrinsic costs of tripartite decoupling are not bugs — they are the direct price of the abstraction:

- **K-way SeqCst on `remaining_deps[g]`** (`buffers.rs:677-790`): required for streaming-correct barrier semantics across concurrent slots. Cannot be removed without abandoning multi-slot correctness.
- **Type-erased `CmTypes` dispatch, 40–85 ns/call** (`5-evaluation.tex:171`): required for plugin isolation. Cannot inline across the plugin boundary.
- **Resolution-thread slot state machine** (`runtime.rs:1140-1693`): required for O(1) generational reset. Cannot be elided in a streaming model.

These costs dominate sub-µs task workloads (hence the 8×8 MIMO 8× gap and anti-diagonal 2.4× gap) and cannot be "optimised away". The honest claim is *"bounded and explicit cost of generality"* (`5-evaluation.tex:250`, `3-design.tex:42`).

**For open release, this means:** Tomii's README must lead with the niche, not claim general parity. Readers picking Tomii for prototyping, streaming research, agent-tunable graphs, and MIMO-class applications get real value. Readers picking Tomii as a Taskflow drop-in will be disappointed.

The paper's discussion already says this honestly. The README and the top-level framing do not yet match it.

### 4.2 Flagship ergonomics #1 — Agent-native graph editing case study

**Goal.** An open, reproducible `examples/agent-tuning/` directory demonstrating the agent-driven graph-editing loop: model receives Tomii JSON + perf goal, edits the graph or CLI flags, runtime reruns, perf improves. Apply to flagship #1 (4×4 MIMO) and flagship #2 (multi-stream pipeline). Use `examples/stream-analytics/` as a third workload for the richest conditional/grouping search surface.

**Why it differentiates.** Taskflow and TBB kernels are baked into C++ source; an agent cannot meaningfully edit them without recompilation. Tomii's JSON graph and CLI knobs are an explicit, structured, parse-validated search surface. This is the *one* structural advantage Tomii has that no comparator can match without a major redesign.

**Methodology rules (fixed from bench-branch run):**

1. **Same perf threshold across all arms.** One threshold per workload, applied to agent-Tomii, random-search-Tomii, Bayesian-Tomii, grid-search-Tomii, and any cross-framework arm. Do not set a softer threshold for Tomii because it starts lower.
2. **Hard-coded verifier per workload, exit code gates perf.** Every edit that fails the verifier is counted as rejected (logged with reason) and excluded from perf measurement. An agent that drops a barrier or removes a `$dep` edge to improve latency must fail.
   - `examples/stream-analytics/`: ✅ `verify.py` written — checks each 4-line block against `result.golden.txt`; accepts `--streams N --exclude E` to assert exact block count.
   - `examples/matrix-compute/`: ✅ `verify.sh` written — builds `perfval` binary, runs it, confirms `validation.txt` was produced.
   - `examples/mapreduce/`: ✅ `verify.sh` written — byte-compares `result.txt` vs `result.golden.txt`.
   - Public 4×4 MIMO (flagship #1): verifier checks decoded symbol output against known test sequence.
3. **Log all edits with disposition.** Record: edit description, verifier result, perf delta vs baseline, reason for rejection if applicable. The rejection log is part of the case-study evidence that the structured surface catches agent mistakes.
4. **Baseline arms required**: random search (sample uniformly from the knob space), Bayesian optimisation (Optuna), grid search. Same verifier, same threshold, same iteration budget.

**Activate disabled section.** `paper/text/7-agent-native.tex` is currently commented out at `main.tex:137`. Once this experiment has results with baselines, re-enable it and revise to include the comparison.

### 4.3 Flagship ergonomics #2 — Polyglot plugin showcase

**Goal.** Frame `examples/matrix-compute/` (Rust), `examples/matrix-compute-C/` (C, FFTW/OpenBLAS), and `examples/matrix-compute-python/` (NumPy) as a single tutorial: *"the same DAG, three kernel languages, one runtime."* Add `examples/gpu-vectoradd/` as a fourth variant (CUDA). Produce a top-level `examples/README.md` matrix and per-example READMEs.

**Why it differentiates.** Tripartite decoupling's defining claim is *"the runtime does not know what language the kernel is written in."* Three working examples make this trivially observable — no comparator framework achieves polyglot kernel dispatch without rebuilding the host binary. This story costs almost no new code; it just needs documentation and packaging.

**Work items:**

| Item | Owner | Notes |
|---|---|---|
| `examples/README.md` matrix (workload × language × framework capability) | Doc | 1–2 hours |
| Per-example `README.md` (purpose, build, run, expected output, tuning knobs) | Doc | ~1 hour per example; 4 examples |
| Python bridge GIL note and `python3.13t` path documented in `matrix-compute-python/README.md` | Doc | Already in docstrings; extract |
| Verify all four examples build from a clean clone with standard deps | Eng | Test on a fresh Ubuntu 22.04 container |

### 4.4 Documentation and paper changes required

**README rewrite (top-level):**
- Lead paragraph: *"Tomii is a task-graph framework for streaming pipelines, MIMO workloads, and agent-tuneable applications — not a general-purpose Taskflow replacement."*
- Honest disclaimer: *"For pure single-stream micro-task DAGs where dispatch overhead dominates, Taskflow is faster. See the benchmark matrix."*
- Bring forward the two flagship perf stories and two ergonomics stories above the fold.
- Link to the Phase 0 benchmark matrix results once available.

**Paper revisions (before camera-ready):**

| Location | Current text | Required change |
|---|---|---|
| `text/0-abstract.tex:9` | *"matches Taskflow within 2 % on DAG workloads"* | Remove or qualify — this number is not actually measured anywhere in §5 under that label |
| `main.tex:137` | `7-agent-native.tex` commented out | Re-enable after ergonomics #1 has results |
| `text/5-evaluation.tex:219-250` | Agent MIMO loop, no baseline | Add random-search and Bayesian-opt arms; report against same threshold |
| `text/5-evaluation.tex:176-197` | Bilateral LoC table | Separate LoC savings from *tripartite decoupling* vs *built-in services* (65 of 77 saved lines today are services, orthogonal to the decoupling argument) |
| `text/5-evaluation.tex:250-262` | 8×8 MIMO "strongest claims are architectural" | Strengthen: explicitly say the 8× loss is intrinsic, bounded, and acceptable for the target use case |

**`tomii-core/src/runtime/ARCHITECTURE.md`:**
✅ *Performance envelope* section added (2026-05-07). Names the four intrinsic costs (K-way SeqCst on `remaining_deps`, type-erased dispatch ~40–85 ns/call, resolution-thread state machine, slot lifecycle overhead), the three workload classes Tomii targets (streaming MIMO, multi-slot fan-out, heterogeneous DAGs), and the three workload classes it does not target (sub-µs micro-tasks, pure `parallel_for`, dynamic-topology DAGs).

---

## 5. Engineering Punch List (Accidental-Overhead Reductions)

These reduce *accidental* overhead that is not intrinsic to tripartite decoupling. Each is independently valuable and hardens the perf story regardless of framing.

| Item | Source | Expected gain | Complexity |
|---|---|---|---|
| Remove per-bulk-task `Box<dyn FnOnce>` allocation; use pre-staged task vector | `runtime_funcs.rs:516-550`, `:921-963` | Reduce dispatch latency floor; reduce allocator pressure on wavefront | Medium |
| Eliminate per-sweep slot reinit allocations | `runtime.rs:1640-1693` | Reduce reinit time at high N | Low–Medium |
| Document `--custom` lock-free scheduler trade-off vs Rayon | `.github/journal.md:13` (Rayon's 150–300 µs global queue lock) | User-facing: correct scheduler choice for latency-sensitive workloads | Low (doc only) |
| Reduce `remaining_deps` SeqCst to `AcqRel` where safe | `buffers.rs:677-790` | Reduce MFENCE count on wavefront; needs careful proof | High (correctness risk) |

The last item has correctness risk (bugs #14/#18-20 in `notes/antidiag-overhead.md` were from relaxing this ordering); do not attempt without a formal memory-model argument.

---

## 5a. Implemented Upgrades — Trivial-Kernel Overhead Reduction

Work completed on the `bench` branch (cherry-picked to `develop` where noted). Target workload: pipeline-bench (N=256 items/stream, 4-stage fan-out/fan-in, sub-µs kernels). Pre-upgrade baseline at W=1, S=1: **0.878 ms/stream**.

### Benchmark Infrastructure Built

- [x] **`anti-diag-bench`** (`bench/anti-diag-bench/tomii/`) — standalone anti-diagonal wavefront benchmark used as the regression gate for all Tier 1–4 changes. Includes `run_bench.py`, result CSV, and a side-by-side comparison plot script. Commits: `b3563cb`, `109c24a`, `5f3d32b`.
- [x] **`pipeline-bench`** (`bench/pipeline-bench/`) — multi-stream linear pipeline benchmark (Tomii vs Taskflow clone). Measures the 4-stage fan-out/fan-in DAG at N=256 over W ∈ {1,2,4,8} × S ∈ {1,4,16,64}. Includes `run_bench.py`, verifier, Taskflow C++ comparator, result CSVs, and `pipeline-bench-desc.md` with full methodology and results. Commit: `315603a`.

### Tier 1–4 — Bulk Wavefront Hoisting (landed on `develop`)

These target contiguous-ready barrier fan-outs (anti-diagonal wavefronts, matmul reductions). They do not fire for 1:1 pipeline edges.

- [x] **Tier 1 — arg hoist**: static arg template extended once per bulk task; per-cell only patches dynamic slots. Eliminates O(N) `Vec::clone` in wavefront loops.
- [x] **Tier 2 — AnyHeld lock elision**: `CmTypes::Any` slots upgraded to `CmTypes::AnyHeld` in the bulk prologue; `with_any` inside the kernel body skips `RwLock::read()` for the full bulk range.
- [x] **Tier 3 — barrier fast-path**: coalesced barrier dispatch (`--coalesce-barriers`) avoids O(N) individual successor enqueues for contiguous-ready fan-outs.
- [x] **Tier 4 — bulk kernel shape (`wf_cell_bulk`)**: when a `{func}_bulk_cm` symbol is registered, `execute_bulk_task` calls it once for the entire `(start, end)` range instead of looping over the per-cell function. Gated on `needs_result_store == false`. Eliminates O(N) function-pointer dispatches per wavefront diagonal.

Commits: `8e6df7b` (Tiers 1–3), `c4a80d9` (Tier 4) on `develop`.

Anti-diag benchmark: 0.134 ms/iter (unaffected by subsequent upgrades).

---

### Upgrade 5 — 1:1 Fanout-Bulk Dispatch (landed on `develop`)

Targets the `ingest → transform` edge in pipeline-bench: a 1:1 fan-out where each predecessor instance dispatches exactly one consumer instance. Per-cell dispatch pays N × (scheduler overhead + Arc clone); fanout-bulk accumulates arrivals via a gen-packed atomic counter and dispatches one bulk task when all N predecessors complete.

- [x] **Core runtime** (`tomii-core`): `is_fanout_bulk` eligibility at graph-build time; `fanout_bulk_arrived: Vec<Vec<AtomicU64>>` in `SlotData` (gen-packed); new per-cell result-storing arm in `execute_bulk_task`. Commit: `c36b472`.
- [x] **`--no-fanout-bulk` CLI flag** exposed in Python `graph.run()` API. Commit: `8a143bb`.
- [x] **W-chunk dispatch fix**: initial implementation dispatched ONE bulk task of N cells, serialising W>1 workers. Fixed to dispatch `min(workers, factor)` contiguous chunks. Commit: `1f9380b`.
- [x] **`inline_continuation` gate**: with `--inline-continuation` + W>1, per-cell dispatch runs transform inline off the Rayon queue (zero scheduler overhead). Fanout-bulk would bypass that path and force transform through the queue — net regression. Gate: `(!inline_continuation || workers == 1)`. Commit: `1f9380b`.

**Measured results (W=1, N=256, 2000 streams, S=1):** 0.878 ms → 0.614 ms — **1.43× speedup**.

Known limitation: W=1, S=4 regresses ~0.87× because the per-cell inline path saves more overhead than the batch loop at that concurrency level. `--no-fanout-bulk` recovers the per-cell path.

---

### Upgrade 6 — Primitive Variadic Fan-In Extraction (landed on `develop`)

Targets the `transform → aggregate` edge: a variadic fan-in over N=256 results. Pre-upgrade, `pl_transform` returned `Vec<f64>` wrapped as `CmTypes::Any(Arc<RwLock<Box<dyn Any>>>)` because the `with_any` variadic path requires `Any`. Each aggregate call paid N × (Arc clone + RwLock acquire + Vec<f64> clone).

- [x] **`tomii-macro` primitive variadic path**: when `#[tomii_export(variadic)]` is used and the element type is a numeric primitive (`bool`, `i8`–`i128`, `u8`–`u128`, `f32`, `f64`, `usize`, `isize`), the generated `_cm` companion uses a direct `match`-on-CmTypes-variant extraction instead of `with_any`. Non-primitive types keep the existing `with_any` path. Variant-specific fixes: `(*x as u8) as T` for `Bool` (direct bool→T cast is invalid in Rust); `**x as T` for `I128`/`U128` (Arc-wrapped). Commit: `c672737` on `develop`.
- [x] **Pipeline-bench plugin**: `pl_transform` now returns `f64` directly (stored as `CmTypes::F64`, no heap allocation); `pl_aggregate` takes `Vec<f64>`. Commit: `5205d82` on `bench`.

**What was eliminated per stream (N=256):** 256 × `Arc::new(RwLock::new(Box::new(Vec::new(...))))`, 256 × Arc clone + RwLock acquire in aggregate collection, 256 × `Vec<f64>` clone inside the `with_any` lambda.

**Measured results (W=1, N=256, 2000 streams):**

| S | Pre-upgrade (baseline) | U5+U6 | Speedup |
|---|------------------------|-------|---------|
| 1  | 0.878 ms | 0.488 ms | **1.80×** |
| 4  | 1.825 ms | 1.071 ms | **1.70×** |
| 16 | 7.968 ms | 4.633 ms | **1.72×** |
| 64 | 32.1 ms  | 18.3 ms  | **1.75×** |

W>1 is unchanged (fanout-bulk gated off by inline_continuation; allocation savings apply but are dominated by parallel dispatch overhead).

---

### Upgrade 7(b/c) — Inline Primitive Storage + Loom Model (landed on `develop`)

Targets `LockFreeResultMap`, which previously heap-allocated every result via `Box::new`.

- [x] **Upgrade 7(c) — Inline result store for primitive `CmTypes`**: 14 primitive variants (F64, F32, I64, U64, I32, U32, I16, U16, I8, U8, Bool, Usize, Isize, Char) are now stored inline in a parallel `(AtomicU8 tag, AtomicU64 val)` pair. Non-primitive types (Arc-wrapped, Vec, Any, Bytes) continue through the existing boxed path. Eliminates one `Box::new` per primitive-output cell per stream. For the pipeline-bench (all F64), that is 514 allocations saved per stream. Also fixes a pre-existing bug: `CmTypes::Isize` was missing from `PartialEq` (always returned false). Commit: `5d5e967` on `bench`; cherry-picked `12cb96a` to `develop`.
- [x] **Upgrade 7(b) — Loom model for slot-reset visibility**: `loom = "0.7"` added as `tomii-core` dev-dep. `inline_tag_slot_reset_visibility` loom model exercises the three-thread write/reinit/read interleaving under all orderings loom explores, confirming the SeqCst swap in `reinit_slot` is the correct fence. `[lints.rust] check-cfg` entry added to suppress spurious `cfg(loom)` warnings. Commit: `5d5e967`.

**Measured gain (pipeline-bench, `pipeline_sweep_post_u7c.csv`, 2026-05-07):**

| Range | Gain vs U5+U6 | Note |
|---|---|---|
| W=1 | ±0–2% (noise) | No allocator contention at W=1 |
| W=2 | 0.4–2.8% | |
| W=4 | 1.4–6.2% | Headline cell S=16 W=4: 21 µs/stream |
| W=8 | 5–17% | Allocator-contention removal dominates |

W=8 gains are largest (5–17%) because 8 concurrent workers were competing for `malloc` on every result write; inline storage eliminates that contention. W=1 is flat as expected.

---

### Remaining (from plan)

- [ ] **Upgrade 7(a) — Eliminate `Arc<SharedData>` clone per spawn**: `scheduling.rs:85` clones the Arc once per node per `send_to_scheduler` call. ~514 clones/stream for pipeline-bench; ~4 µs/stream (~0.3% at W=4, S=16). Elimination requires `unsafe` raw-pointer capture in Rayon `'static` closures. Deferred: gain is sub-1%, `unsafe` adds complexity.
- [ ] **Remaining SeqCst audit**: `scheduling.rs:173` (telemetry counter) and `task_execution.rs:394` (execute path counter) are still SeqCst; these are not on the hot result-store path and deferred pending the loom model above.

---

## 5b. Stage C — Public Release Cleanup (completed 2026-05-07)

### Item 1 — Strip absolute paths

All `/home/george/` references removed from public-facing source and scripts. Files touched:

| File | Change |
|---|---|
| `pipeline-bench/taskflow/CMakeLists.txt` | `$ENV{TASKFLOW_ROOT}` with vendored `anti-diag-bench/taskflow/src` fallback |
| `anti-diag-bench/taskflow/CMakeLists.txt` | Same pattern; vendored `src/` fallback |
| `agent-eval/references/taskflow/task_1/CMakeLists.txt` | `$ENV{TASKFLOW_ROOT}` with FATAL_ERROR fallback |
| `agent-eval/references/taskflow/task_1/run.sh` | `${TASKFLOW_ROOT:+-DTASKFLOW_DIR="$TASKFLOW_ROOT"}` conditional |
| `agent-eval/scaffolds/taskflow/task_1/CMakeLists.txt` + `tier_2/` | Same env-var pattern |
| `agent-eval/scaffolds/taskflow/task_1/TASK.md` + `tier_2/` | Build instructions no longer hardcode `-DTASKFLOW_DIR` |
| `agent-eval/config.py` | `TASKFLOW_INCLUDE = Path(os.environ.get("TASKFLOW_ROOT", ""))` |
| `agent-eval/harness.py` | `TOMII_ORACLE_DIR` via `os.environ.get`; taskflow agent blurb cleaned; `_absolutize_cargo_paths()` added and called after `shutil.copytree` so relative Cargo paths work in temp workspaces |
| `agent-eval/scaffolds/tomii/task_1/Cargo.toml` + `tier_2/` | Relative paths: `../../../../tomii-types`, etc. |

**Gitignore caveat.** `.gitignore` has `*.txt` (catches `CMakeLists.txt`) and `examples/**`. The CMakeLists.txt edits in `anti-diag-bench/` and `agent-eval/references|scaffolds/` exist on disk but are gitignored; they need `git add -f` before they can be committed. Ditto for the three verifier files. The seven tracked agent-eval files (harness.py, config.py, Cargo.tomls, TASK.md files, pipeline-bench CMakeLists.txt) are committed normally. *(Note: as of 2026-05-08, the root `.gitignore` now has `!**/.gitignore` so future nested `.gitignore` files are auto-trackable without `git add -f`; CMakeLists.txt files are handled via local per-subdir negation rules — see §5b Item 4.)*

### Item 2 — Example verifiers

- [x] `examples/stream-analytics/verify.py` — 4-line block comparison; `--streams`/`--exclude` args
- [x] `examples/matrix-compute/verify.sh` — builds `perfval`, checks `validation.txt` produced
- [x] `examples/mapreduce/verify.sh` — byte-compares `result.txt` vs `result.golden.txt`

### Item 3 — ARCHITECTURE.md performance envelope

- [x] `tomii-core/src/runtime/ARCHITECTURE.md` — 49-line *Performance envelope* section appended

### Item 4 — Gitignore refactoring (2026-05-08)

Replaced the monolithic root `.gitignore` (55 lines, 18 subdir-specific entries) with per-subdirectory `.gitignore` files. Root now holds only truly global rules; each bench dir is self-describing.

| File | Action |
|---|---|
| `.gitignore` | Rewritten: dropped all subdir-specific lines; added `**/__pycache__/`, `build/`; added `!**/.gitignore` so nested ignores are auto-trackable |
| `tomii/.gitignore` | New: `_bin/main`, `_python_bridge/target/` |
| `tomii-core/.gitignore` | New: `_examples/`, `plot.py`, `src/batch_queue_lockfree.rs`, `src/batch_queue_factory.rs`, `src/ignored/` |
| `anti-diag-bench/.gitignore` | New: `taskflow/build/` |
| `mimo-bench/.gitignore` | New: `results/`, `taskflow/build_asan/`, `taskflow/build_dbg/`, `tomii/report.json` |
| `pipeline-bench/.gitignore` | Replaced 4-line file: globals cover the rest; `!taskflow/CMakeLists.txt` negation moved here from root |
| `matcomp-taskflow/.gitignore` | New: `build/`, `*.csv`, `!CMakeLists.txt` — enables tracking of `matcomp-taskflow/CMakeLists.txt` (previously blocked by global `*.txt`) |

Commit: `2769c0c`.

### Skipped

- Item 4 (paper revisions): skipped — paper is local-only and not published.

---

## 6. Release Checklist

The following must be true before public release:

- [ ] **Phase 0 matrix complete.** All 8 rows measured, threshold-unified, verifier-gated. Scoreboard published in README.
- [x] **Flagship perf #1 shippable.** `bench/mimo-bench/`: real-PHY MIMO (fft→csi→beam→demul), Tomii + Taskflow ports linking identical Intel libs, verifier, README with Intel/Agora dep disclaimer. (2026-05-07)
- [x] **Flagship perf #2 shippable.** S-scaling sweep done (post-U7c), verifier written, Taskflow comparator in place, methodology note at `pipeline-bench/pipeline-bench-desc.md`. Memory comparison (2.8×) still needs direct measurement.
- [x] **Flagship ergonomics #1 shippable.** `examples/agent-tuning/` has 4-arm search (random, Bayesian, grid, Claude) over stream-analytics, same threshold, verifier-gated. (2026-05-11)
- [x] **Flagship ergonomics #2 shippable.** All four polyglot examples build from clean clone. `examples/README.md` matrix exists. Per-example READMEs exist. (2026-05-07; also added READMEs for stream-analytics and mapreduce)
- [ ] **Paper abstract claim qualified.** *"within 2 %"* line removed or corrected before any preprint or submission.
- [x] **ARCHITECTURE.md performance envelope section added.** (2026-05-07)
- [x] **No absolute paths in any public example.** All `/home/george/` references replaced with `$ENV{TASKFLOW_ROOT}` / `os.environ.get` patterns and repo-relative paths. (2026-05-07; see §5b for gitignore caveat on CMakeLists.txt files)
- [x] **No external private dependencies in public examples.** `examples/mimolib/` is internal; public MIMO bench is `bench/mimo-bench/` with documented Intel/Agora hard deps and link to https://github.com/Agora-wireless/Agora. (2026-05-07)

---

## 7. Risks and Open Questions

| Risk | Likelihood | Mitigation |
|---|---|---|
| Phase 0 row 5 (multi-stream) does not show Tomii winning | Medium | If S-scaling win doesn't materialise, demote flagship #2 and strengthen prototyping framing; honest scoreboard still ships |
| Public 4×4 MIMO Taskflow port takes longer than estimated | Medium | Start from paper's `antidiag-overhead.md`-referenced Taskflow bench setup; 4×4 is much simpler than full `.mimolib` |
| Agent-bench verifiers for stream-analytics are underspecified | Low | Stream-analytics has clear invariants (each event hits exactly one branch); specify before writing the verifier, not after |
| Reducing SeqCst in `buffers.rs` reintroduces bug #14/#18-20 | High if attempted naively | Only attempt with a formal memory-model proof; defer to post-release if uncertain |
| Open question: should Timely be a sustained comparator or a one-time matrix cell? | TBD | Depends on row 4 result; if Tomii and Timely are comparable on streaming workloads, Timely is a stronger related-work story |
| Open question: which Tomii workloads are best for the agent baseline paper section? | TBD | Stream-analytics (richest knob space), multi-stream pipeline (clearest perf metric); confirm after ergonomics #1 pilot run |
