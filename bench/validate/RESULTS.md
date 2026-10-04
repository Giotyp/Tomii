# Root-node completion accounting: validation of fix cd7e4b1

**Verdict: VALIDATED.** No missed decrement path and no material regression.

- Every accounting cell passes: 432 of 432. Real calls equal factor × frames, `stale_task_drops` is 0, all 20 of 20 frames complete, and nothing hangs.
- The only failing cells (48, graph `d1_condroot_fail`) come from a separate, pre-existing semantic gap. A node-level `Condition` on a root node is never evaluated. The counting there is consistent.
- Radar, MIMO and the item-5 barrier graph are unchanged within run-to-run noise. The largest shift is −1.8% at W=24, measured against a same-session pre-fix A/B.

## Provenance

| | |
|---|---|
| Tomii SHA (branch `eval/root-validate`) | `baa2674`, which is `eval/paper-rerun` 27a2edf plus fix cd7e4b1 |
| Pre-fix control | 27a2edf, taken with `git archive` and built into `target-rootval-prefix/`. The kernel and `FUNC_PATH` are identical to the post-fix build. |
| Host | `yecl-acc-01`, kernel 5.15.0-181-generic, governor `performance`, rustc 1.92.0 |
| Date | 2026-09-27 |

Measurement setup:

- Every timed run holds `/home/george/Tomii/.eval-server.lock`, taken per subprocess with `fcntl.flock`.
- Timed runs use `taskset -c 0-31`. Tomii's own allocator put the main thread on core 1, the system thread on core 2 and workers from core 3.
- Builds ran with `taskset -c 32-63 nice -n 10`, each into its own target directory:
  - `target-rootval/` and `target-rootval-kernel/`
  - `target-rootval-prefix/`
  - `target-rootval-radar{,-pre}/`
  - `target-rootval-mimo/`

Harness and kernel:

- Harness: `bench/root-validate/root_validate.py`, with the flags `--matrix`, `--barrier-regress` and `--bare-sweep`. Setting `ROOTVAL_BINARY` and `ROOTVAL_TAG=_prefix` runs the pre-fix control.
- Kernel: `bench/root-validate/kernel/src/lib.rs`. It is the item-5 kernel verbatim (the same `busy_ns` and the same `BUSY_CALLS` atexit counter) plus one condition evaluator, `cond_gt(x, thr)`.
- Raw JSON, logs and state dumps are in `bench/root-validate/results/`, which is gitignored.

## 1. Correctness matrix (ground truth = `BUSY_NS_CALLS`)

The axes were:

- `--inline-continuation` on or off
- `--no-fanout-bulk` on or off
- W ∈ {1, 4, 16}
- scheduler default (Rayon work-stealing) or `--custom`
- slots ∈ {1, 2}. Slots=2 was an addition to the requested grid.

That gives 48 cells per graph. Each cell runs 20 frames with 2 warm-up frames and `busy_ns` = 2 µs.

- **Pass criteria:** `rc == 0`, no hang within the 30 s timeout, `BUSY_NS_CALLS == expected_per_frame × 20`, `summary.stale_task_drops == 0`, no shutdown stale-drop WARN, and `completed=20` in the slot-completion log.
- **Hang handling:** SIGUSR1 goes to a `--dump-state` snapshot first, then kill. No cell hung, pre-fix or post-fix.
- **Pre-fix column:** the same matrix at slots=1 (24 cells) on the 27a2edf binary. The pre-fix report has no `stale_task_drops` field, so that column shows only how many cells had exact call counts.

| graph | shape | expected calls/frame | post-fix | pre-fix exact-count cells (min calls / 20 fr) |
|---|---|---:|---|---|
| (a) `a_fanout2048` | successor-less root, factor 2048 | 2048 | **48/48 OK** | 4/24 (128 of 40960) |
| (b) `b_filt_k0` | root a (16) → c reads `a.out(0)` | 17 | **48/48 OK** | 7/24 (58 of 340) |
| (b) `b_filt_k5` | root a (16) → c reads `a.out(5)` | 17 | **48/48 OK** | 9/24 (158 of 340) |
| (b) `b_filt_k15` | root a (16) → c reads `a.out(15)` | 17 | **48/48 OK** | 24/24 |
| (c) `c_barrier2048` | root (2048) → sink `wait(0,2048)` (covered case) | 2048 | **48/48 OK** | 24/24 |
| (d1) `d1_condroot_pass` | root (16) with node-level `Condition`, always true | 16 | **48/48 OK** | 24/24 |
| (d1) `d1_condroot_fail` | same, condition always false | 0 (semantic) | 0/48: 16 calls/frame, stale 0, 20/20 frames | same: 16/frame |
| (d2) `d2_condsucc_pass` | root r (16) → 1:1 conditional successor s, true | 32 | **48/48 OK** | 24/24 |
| (d2) `d2_condsucc_fail` | same, condition false | 16 | **48/48 OK** | 24/24 |
| (e) `e_bulkchain256` | root r (256) → 1:1 successor s (256), fanout-bulk eligible | 512 | **48/48 OK** | 24/24 |

What the graphs exercise:

- **(a) and (b)** are the bug's shapes. Before the fix they lose up to 99.7% of root work. After it, every cell is exact.
  - `k=15` was already correct before the fix, because the last-dispatched instance gates the successor.
  - The W=16 work-stealing cells of (a) were exact before the fix too, since the race is usually lost at high W. This matches bench/micro/RESULTS.md.
- **The worker-resolve decrement path** (`task_execution.rs`) is exercised by (a), (b), (c) and (e).
- **The batch-resolution decrement path** (`batch_resolution.rs`) is exercised by (d2). A root whose successor has a node condition is not `worker_resolvable`, so the root completes through the batch path. Both condition outcomes are exact, including the fail branch's `pending_cond_tasks` discharge.
- **Fanout-bulk:** (e) is the graph where `--no-fanout-bulk` actually changes dispatch. Bulk chunks of the successor decrement by `bulk_count`, and the root instances decrement individually. Exact in all 48 cells.
  - On the other graphs, `--no-fanout-bulk` does not change dispatch: no successor there has a single `$res` predecessor with an equal factor greater than 1.
  - `--inline-continuation` disables fanout-bulk when W>1. Both combinations are covered.
- **Conditional roots (d1):** the Python API supports them (`tm.Condition` on any node). The fix leaves their accounting as it was: a condition root is `is_condition`, so it counts in `pending_cond_tasks`, as before. It completes correctly.
  - **Separate pre-existing issue, not part of this fix.** A root's node-level condition is never evaluated. `initial_nodes()` (`slot_management.rs:381`) dispatches every instance unconditionally, and conditions are only evaluated when a successor becomes ready (`batch_resolution.rs`).
  - As a result, `d1_condroot_fail` runs all 16 instances per frame even though the condition is false. The behaviour is identical before and after the fix.
  - The graph builder accepts this silently. It should either evaluate root conditions at dispatch or reject them at build time.

## 2. Regressions (5 reps each; previous numbers from the cited RESULTS files)

| workload / metric | previous | post-fix, median [min–max] | same-session pre-fix control | verdict |
|---|---|---|---|---|
| Radar T32/w16/s2, 1024x128 @100 fps: e2e p50 | 6.63–6.65 ms | 6.651 ms [6.641–6.662] and 6.644 ms [6.640–6.657] (two 5-rep cells) | 6.660 and 6.646 ms | no regression |
| Radar: post-CPI p50 | 286–312 µs (bimodal) | 315 µs [302–323] and 306 µs [298–322] | 324 [306–328] and 307 [303–316] µs | no regression; the slow mode appears pre-fix too |
| Radar: verifier / stale drops | pass | 3000/3000 targets, 0 false alarms; stale 0 in all 15 reports | | pass |
| MIMO 16x16 S8 E1 best (W24, slot_priority): e2e mean / p99 | 5.968 / 6.006 ms | **5.974 / 6.013 ms** (+0.1% / +0.1%) | | no regression |
| MIMO: verify / stale | byte-exact | byte-exact 1020/1020 in all reps; stale 0 | | pass |
| item-5 barrier, ns=2000, workstealing W=8 | 1.160M tasks/s | 1.138M [1.117–1.174] | 1.130M [1.115–1.162] | no regression |
| item-5 barrier, ws W=24 | 1.005M | 0.979M [0.961–0.997] | 0.997M [0.992–1.011] | −1.8% vs control; see note |
| item-5 barrier, custom W=8 | 2.752M | 2.811M [2.782–2.845] | 2.828M [2.793–2.841] | no regression |
| item-5 barrier, custom W=24 | 0.309M | 0.307M [0.304–0.312] | 0.312M [0.307–0.316] | −1.4%, ranges overlap |

Radar and MIMO setup:

- **Radar.** The run used `bench/radar-bench/e5.py` with flags `custom,coalesce_barriers,inline_continuation,slot_priority`, `--core-offset 1`, 1000 frames and 50 warm-up. The sender ran on core 48.
- **MIMO.** The run used `bench/mimo-bench/e1/e1.py cell`, W24, S8, P=6000 µs, 1020 frames with 20 warm-up, and `MKL_NUM_THREADS=1`. The receiver started 5 s before the sender, which ran on cores 52–63.
  - This worktree lacks `bench/mimo-bench/tomii/lib/`, so the plugin `.so` is the 3c33081 build from eval-p0-validate. Its `src/` and `build.rs` are identical.
- **Why these are unaffected.** Neither graph has initial nodes: every root reads `$network`. Tasks per frame are unchanged, for example 193 for radar.

The item-5 numbers use the barrier-graph method from bench/micro/RESULTS.md:

- The **primary number here is the log timeline** (method b). A same-session pre-fix control ran interleaved, run by run, under the lock.
- **Method (a) is unreliable on this host at these run lengths.** The K1/K2 process-wall differential is quantized: the implied T2−T1 values fall on a grid of about 50 ms. For example, custom W=8 flips between 2.64M and 3.95M, and ws W=8 between 1.129M and 1.317M, in both the pre-fix and post-fix binaries.
- **The W=24 delta.** The −1.4% to −1.8% sits at the edge of run-to-run noise. If it is real, the likely cause is the one extra SeqCst `fetch_sub` on the shared `pending_tasks[slot]` line per root instance. That adds a third contended RMW per task, next to the two on `processing_count`, and would matter most at high W.
  - W=8 shows no cost.
  - If this needs closing, the next step is a longer-K log-timeline A/B at W=24/32. A possible fix is to decrement roots once per chunk rather than once per instance.

## 3. Bare successor-less fan-out: pre-fix vs post-fix (ns=2000, factor 2048)

- **Pre-fix values:** bench/micro/RESULTS.md, "W-sweep, task_ns = 2000 — re-verified". Those are tasks/s = log fps × the runtime's `total_tasks_per_frame`, measuring the race outcome.
- **Post-fix values:** log-timeline tasks/s, median [min–max] of 5 reps. Every timed run's `BUSY_NS_CALLS` equals exactly 2048 × K, in all 70 cell-reps × 2 runs, with no stale-drop WARN.

| W | ws pre-fix | **ws post-fix** | custom pre-fix | **custom post-fix** |
|---:|---:|---:|---:|---:|
| 1 | 611 | **386,647** [385,711–391,164] | 25,766 | **440,840** [439,735–441,524] |
| 2 | 628 | **660,144** [638,484–665,278] | 2,316 | **796,581** [789,627–802,713] |
| 4 | 350,761 (real_factor 609) | **1,050,316** [1,039,691–1,064,145] | 147,166 (real_factor 60) | **1,572,296** [1,555,036–1,587,413] |
| 8 | 1,104,402 | **1,042,687** [1,007,469–1,140,646] | 421,555 (unstable) | **2,720,105** [2,684,611–2,812,968] |
| 16 | 957,721 | **968,216** [897,115–1,013,624] | 332,063 | **338,893** [335,098–343,789] |
| 24 | 910,119 | **859,783** [818,795–929,949] | 311,677 | **315,485** [312,350–317,648] |
| 32 | 821,592 | **824,582** [788,613–839,179] | 294,861 | **300,488** [295,635–302,911] |

**The fix restores full work, and the bare graph now matches the barrier graph.**

- **Physically sensible at low W.** At W=1, work-stealing does 386,647 tasks/s, which is 5.30 ms per frame against 4.10 ms of declared serial compute (2048 × 2 µs), with about 1.2 ms of dispatch overhead. The barrier graph's W=1 figure was 393,286.
- **The pre-fix numbers were an artefact.** Pre-fix W=1/W=2 (611 and 628) came from about one real call per frame.
- **Custom at W=1–8** now reproduces the barrier graph's spike-then-cliff shape: 0.44M, 0.80M, 1.57M, 2.72M, then 0.34M. It is no longer a noisy race outcome.
- **W ≥ 16**, where the pre-fix race was nearly always lost, agrees with the pre-fix numbers within noise. The exception is ws W=24 (−5.5%), which has wide dispersion in both data sets.

## Other observation (not caused by the fix)

`tomii-core/tests/integration.rs::test_slot_priority_single_slot_nonnetwork_restarts` is flaky with and without the fix:

- Run alone under the lock on cores 0–31, it fails 10/10 both pre-fix and post-fix.
- In the full suite it fails in roughly half the runs: post-fix in 4 of 7, pre-fix in 2 of 3.
- The fix's two new regression tests passed in every post-fix suite run (14 tests post-fix, 12 pre-fix).

The test pins its runtime from core offset 0 regardless of `taskset`. Treat any unlocked `cargo test` as a measurement contaminant for cores 0–1.
