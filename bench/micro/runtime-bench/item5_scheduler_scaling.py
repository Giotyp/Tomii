"""Item 5: scheduler / ready-queue scalability.

Wide fan-out graph (one node, factor=M, no inter-instance dependencies — all
M instances are "initial nodes", maximally stressing the ready queue at
frame start) run with the real Tomii runtime. Sweeps:

  - W (workers) in {1,2,4,8,16,24,32} at a fixed 2us task size — the primary
    throughput-vs-W / saturation-point curve.
  - task size in {0.5,2,10,50,100}us at W in {8,32} — task-granularity
    sensitivity, answering "does the overhead-dominated regime shift with
    task size".

...for both schedulers: Tomii's default (Rayon work-stealing) and its
`--custom` crossbeam-channel MPMC scheduler (see
tomii-core/src/custom_scheduler/, tomii-core/src/scheduler.rs).

Usage:
    taskset -c 32-63 nice -n 10 python3 item5_scheduler_scaling.py --build
    python3 item5_scheduler_scaling.py --sweep-w --reps 5
    python3 item5_scheduler_scaling.py --sweep-size --reps 5
"""

from __future__ import annotations

import argparse
import fcntl
import json
import re
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[2]
sys.path.insert(0, str(REPO_ROOT))

import tomii as tm
from tomii._runner import build_command

KERNEL_DIR = HERE / "kernel"
KERNEL_LIB_RS = KERNEL_DIR / "src" / "lib.rs"
LOCKFILE = "/home/george/Tomii/.eval-server.lock"
MEASURE_CORES = "0-31"

M_FANOUT = 2048
W_SWEEP = [1, 2, 4, 8, 16, 24, 32]
SCHEDULERS = ["workstealing", "custom"]  # default vs --custom
FIXED_TASK_NS_FOR_W_SWEEP = 2000  # 2us
SIZE_SWEEP_NS = [500, 2000, 10000, 50000, 100000]
SIZE_SWEEP_WORKERS = [8, 32]

MANIFEST_PATH = HERE / "results" / "item5_manifest.json"
RESULTS_DIR = HERE / "results" / "item5"

_RSS_RE = re.compile(r"Maximum resident set size \(kbytes\):\s*(\d+)")


def _fnum(x, spec: str) -> str:
    """Format a possibly-None numeric value (e.g. per_task_overhead_ns is
    None when tasks_per_sec == 0 — a real occurrence when the runtime's
    fan-out elasticity collapses to 0 real invocations/frame, seen at W=1)."""
    return format(x, spec) if x is not None else "n/a"


def build_fanout_graph(m: int, busy_ns: int) -> tm.Graph:
    app = tm.Graph()
    ns_var = app.var("ns", tm.usize(busy_ns))
    app.node("fanout", func="busy_ns", args=[tm.f64(1.0), ns_var], factor=m)
    return app


def frames_for_ns(task_ns: int, target_serial_s: float = 1.5, m: int = M_FANOUT) -> int:
    total_tasks = max(1, int(target_serial_s / (task_ns * 1e-9)))
    k = max(8, total_tasks // m)
    return int(k)


def do_build() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    print("[build] cargo build kernel dylib...", flush=True)
    subprocess.run(
        ["cargo", "build", "--release", "--manifest-path", str(KERNEL_DIR / "Cargo.toml")],
        check=True,
    )
    dylib_candidates = sorted((KERNEL_DIR / "target" / "release").glob("*.so"))
    assert dylib_candidates, "no .so produced by kernel build"
    dylib = str(dylib_candidates[0].resolve())

    # One graph shape (factor=M) with `ns` as a graph variable — reused
    # across every task-size cell so we don't need to rebuild per size.
    graph = build_fanout_graph(M_FANOUT, FIXED_TASK_NS_FOR_W_SWEEP)
    graph_json_path = RESULTS_DIR / "fanout_graph.json"
    graph_json_path.write_text(graph.to_json(), encoding="utf-8")

    print("[build] cargo build tomii-core main (FUNC_PATH=kernel)...", flush=True)
    result = graph.build(func_path=str(KERNEL_LIB_RS), release=True, clean=False)

    manifest = {
        "dylib": dylib,
        "binary": result.binary,
        "graph_json_template": str(graph_json_path),
        "m_fanout": M_FANOUT,
    }
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"[build] manifest written: {MANIFEST_PATH}")


def graph_json_for_ns(manifest: dict, task_ns: int) -> Path:
    """Graph JSON only differs in the `ns` variable's value — patch it
    in-place per task size rather than rebuilding the plugin."""
    path = RESULTS_DIR / f"fanout_graph_ns{task_ns}.json"
    if path.exists():
        return path
    graph = build_fanout_graph(manifest["m_fanout"], task_ns)
    path.write_text(graph.to_json(), encoding="utf-8")
    return path


def run_one(manifest: dict, *, workers: int, task_ns: int, scheduler: str, k: int,
            warmup: int, tag: str, rep: int, inline_continuation: bool = False) -> dict:
    graph_json = graph_json_for_ns(manifest, task_ns)
    report_path = RESULTS_DIR / f"report_{tag}_r{rep}.json"
    timing_path = RESULTS_DIR / f"timing_{tag}_r{rep}.txt"
    rss_path = RESULTS_DIR / f"rss_{tag}_r{rep}.txt"

    kwargs = dict(
        workers=workers,
        slots=1,
        max_frames=k,
        exclude_frames=warmup,
        report=str(report_path),
        timing=str(timing_path),
        use_rdtsc=True,
    )
    if scheduler == "custom":
        kwargs["custom"] = True
    if inline_continuation:
        # NOTE: this graph is a single-node wide fan-out (factor=M, no
        # inter-instance successors) — --inline-continuation only affects
        # single-successor (chain) nodes, so it is expected to be a no-op
        # here. Included because the coordinator asked for the sanity check;
        # see RESULTS.md item 5 for the measured (non-)effect.
        kwargs["inline_continuation"] = True

    cmd = build_command(manifest["binary"], str(graph_json), manifest["dylib"], **kwargs)
    full_cmd = [
        "flock", LOCKFILE, "taskset", "-c", MEASURE_CORES,
        "/usr/bin/time", "-v", "-o", str(rss_path),
    ] + cmd

    proc = subprocess.run(full_cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        print(proc.stdout[-4000:], file=sys.stderr)
        print(proc.stderr[-4000:], file=sys.stderr)
        raise RuntimeError(f"run failed ({tag} rep={rep}): rc={proc.returncode}")

    report = json.loads(report_path.read_text())
    summary = report.get("summary", {})
    m = _RSS_RE.search(rss_path.read_text())
    rss_kb = int(m.group(1)) if m else None

    fps = summary.get("throughput_frames_per_sec") or 0.0
    m_fanout = manifest["m_fanout"]
    # IMPORTANT: the runtime elastically coalesces the declared `factor=m`
    # fan-out into fewer, larger task invocations when W or task_ns make the
    # full fan-out pointless (discovered during the 2026-09-25 eval
    # campaign: `summary.total_tasks_per_frame` is far below `m_fanout` at
    # low W / large task_ns, e.g. W=1 -> ~0-1 real invocations/frame instead
    # of 2048). Using the *declared* m_fanout to compute tasks_per_sec
    # silently overstates throughput by up to ~300x in those cells. Use the
    # runtime's own reported per-frame invocation count instead — this is
    # the real ready-queue op rate the item asks about.
    real_factor = summary.get("total_tasks_per_frame")
    if real_factor is None:
        # Older report schema without the field: fall back to the nominal
        # factor (pre-elasticity-discovery behavior), but flag it.
        real_factor = m_fanout
    tasks_per_sec = fps * real_factor
    ideal_tasks_per_sec = workers / (task_ns * 1e-9) if task_ns > 0 else float("nan")
    efficiency = (tasks_per_sec / ideal_tasks_per_sec) if ideal_tasks_per_sec else None
    per_task_overhead_ns = None
    if tasks_per_sec > 0:
        per_task_overhead_ns = (workers / tasks_per_sec) * 1e9 - task_ns

    return {
        "tag": tag,
        "rep": rep,
        "workers": workers,
        "task_ns": task_ns,
        "scheduler": scheduler,
        "m_fanout": m_fanout,
        "real_factor_per_frame": real_factor,
        "k_frames": k,
        "rss_kb": rss_kb,
        "avg_latency_us": summary.get("avg_latency_us"),
        "p50_latency_us": summary.get("p50_latency_us"),
        "p99_latency_us": summary.get("p99_latency_us"),
        "throughput_frames_per_sec": fps,
        "tasks_per_sec": tasks_per_sec,
        "ideal_tasks_per_sec": ideal_tasks_per_sec,
        "efficiency": efficiency,
        "per_task_overhead_ns": per_task_overhead_ns,
    }


def do_sweep_w(reps: int) -> None:
    manifest = json.loads(MANIFEST_PATH.read_text())
    rows = []
    for scheduler in SCHEDULERS:
        for w in W_SWEEP:
            k = frames_for_ns(FIXED_TASK_NS_FOR_W_SWEEP)
            warmup = max(1, k // 10)
            for rep in range(1, reps + 1):
                tag = f"wsweep_{scheduler}_w{w}"
                print(f"[w-sweep] {tag} rep={rep}/{reps} k={k}", flush=True)
                row = run_one(
                    manifest, workers=w, task_ns=FIXED_TASK_NS_FOR_W_SWEEP,
                    scheduler=scheduler, k=k, warmup=warmup, tag=tag, rep=rep,
                )
                rows.append(row)
                print(f"  tasks/s={_fnum(row['tasks_per_sec'], '.0f')} eff={_fnum(row['efficiency'], '.3f')} "
                      f"overhead_ns={_fnum(row['per_task_overhead_ns'], '.1f')}", flush=True)
    out = RESULTS_DIR / "w_sweep_results.json"
    out.write_text(json.dumps(rows, indent=2))
    print(f"[w-sweep] wrote {out}")


TUNED_TASK_NS = [2000, 10000]


def do_tuned_sweep_w(reps: int) -> None:
    """Sanity-check sweep: same W range as do_sweep_w, but with
    --inline-continuation added on top of both schedulers, at 2us and 10us
    task sizes (per the coordinator's item-5 follow-up request)."""
    manifest = json.loads(MANIFEST_PATH.read_text())
    rows = []
    for task_ns in TUNED_TASK_NS:
        for scheduler in SCHEDULERS:
            for w in W_SWEEP:
                k = frames_for_ns(task_ns)
                warmup = max(1, k // 10)
                for rep in range(1, reps + 1):
                    tag = f"tuned_{scheduler}_ns{task_ns}_w{w}"
                    print(f"[tuned-w-sweep] {tag} rep={rep}/{reps} k={k}", flush=True)
                    row = run_one(
                        manifest, workers=w, task_ns=task_ns,
                        scheduler=scheduler, k=k, warmup=warmup, tag=tag, rep=rep,
                        inline_continuation=True,
                    )
                    rows.append(row)
                    print(f"  tasks/s={_fnum(row['tasks_per_sec'], '.0f')} eff={_fnum(row['efficiency'], '.3f')} "
                          f"overhead_ns={_fnum(row['per_task_overhead_ns'], '.1f')}", flush=True)
    out = RESULTS_DIR / "tuned_w_sweep_results.json"
    out.write_text(json.dumps(rows, indent=2))
    print(f"[tuned-w-sweep] wrote {out}")


def do_sweep_size(reps: int) -> None:
    manifest = json.loads(MANIFEST_PATH.read_text())
    rows = []
    for scheduler in SCHEDULERS:
        for w in SIZE_SWEEP_WORKERS:
            for ns in SIZE_SWEEP_NS:
                k = frames_for_ns(ns)
                warmup = max(1, k // 10)
                for rep in range(1, reps + 1):
                    tag = f"sizesweep_{scheduler}_w{w}_ns{ns}"
                    print(f"[size-sweep] {tag} rep={rep}/{reps} k={k}", flush=True)
                    row = run_one(
                        manifest, workers=w, task_ns=ns, scheduler=scheduler,
                        k=k, warmup=warmup, tag=tag, rep=rep,
                    )
                    rows.append(row)
                    print(f"  tasks/s={_fnum(row['tasks_per_sec'], '.0f')} eff={_fnum(row['efficiency'], '.3f')} "
                          f"overhead_ns={_fnum(row['per_task_overhead_ns'], '.1f')}", flush=True)
    out = RESULTS_DIR / "size_sweep_results.json"
    out.write_text(json.dumps(rows, indent=2))
    print(f"[size-sweep] wrote {out}")


def run_one_true(manifest: dict, *, workers: int, task_ns: int, scheduler: str, k: int,
                  warmup: int, tag: str, rep: int) -> dict:
    """Like `run_one`, but measures throughput from an *external* wall-clock
    timer around the subprocess call instead of trusting the runtime's own
    `summary.throughput_frames_per_sec` (see RESULTS.md item 2's "Third
    follow-up": that field is computed as `num_frames /
    sum(each_frame's_own_latency)`, i.e. `1/avg_latency_us` by
    construction, and cannot reflect real concurrency).

    The eval-server lock is acquired here via `fcntl.flock` on our own file
    handle (not the `flock` *command* wrapping the whole call) so that the
    perf_counter measurement starts only after the lock is held and stops
    before it's released -- lock-wait time is never counted as "processing
    time", and the lock is held for exactly one cell (not the whole sweep),
    same as every other harness in this campaign.
    """
    graph_json = graph_json_for_ns(manifest, task_ns)
    report_path = RESULTS_DIR / f"report_{tag}_r{rep}.json"
    timing_path = RESULTS_DIR / f"timing_{tag}_r{rep}.txt"

    kwargs = dict(
        workers=workers,
        slots=1,
        max_frames=k,
        exclude_frames=warmup,
        report=str(report_path),
        timing=str(timing_path),
        use_rdtsc=True,
    )
    if scheduler == "custom":
        kwargs["custom"] = True

    cmd = build_command(manifest["binary"], str(graph_json), manifest["dylib"], **kwargs)
    full_cmd = ["taskset", "-c", MEASURE_CORES] + cmd

    with open(LOCKFILE, "w") as lockf:
        fcntl.flock(lockf, fcntl.LOCK_EX)
        try:
            t0 = time.perf_counter()
            proc = subprocess.run(full_cmd, capture_output=True, text=True)
            t1 = time.perf_counter()
        finally:
            fcntl.flock(lockf, fcntl.LOCK_UN)

    if proc.returncode != 0:
        print(proc.stdout[-4000:], file=sys.stderr)
        print(proc.stderr[-4000:], file=sys.stderr)
        raise RuntimeError(f"run failed ({tag} rep={rep}): rc={proc.returncode}")

    true_elapsed_s = t1 - t0
    report = json.loads(report_path.read_text())
    summary = report.get("summary", {})
    real_factor = summary.get("total_tasks_per_frame")
    if real_factor is None:
        real_factor = manifest["m_fanout"]

    # k = total frames actually run (including the excluded warmup, which
    # the external timer also covers) -- the honest denominator for a true
    # wall-clock rate, matching item 2's corrected methodology.
    true_fps = k / true_elapsed_s if true_elapsed_s > 0 else 0.0
    true_tasks_per_sec = true_fps * real_factor
    ideal_tasks_per_sec = workers / (task_ns * 1e-9) if task_ns > 0 else float("nan")
    true_efficiency = (true_tasks_per_sec / ideal_tasks_per_sec) if ideal_tasks_per_sec else None

    return {
        "tag": tag,
        "rep": rep,
        "workers": workers,
        "task_ns": task_ns,
        "scheduler": scheduler,
        "k_frames": k,
        "true_elapsed_s": true_elapsed_s,
        "real_factor_per_frame": real_factor,
        "true_fps": true_fps,
        "true_tasks_per_sec": true_tasks_per_sec,
        "ideal_tasks_per_sec": ideal_tasks_per_sec,
        "true_efficiency": true_efficiency,
        # kept for comparison against the (misleading) old metric:
        "reported_fps": summary.get("throughput_frames_per_sec"),
    }


def do_true_sweep(reps: int) -> None:
    """Corrected item-5 W x task-size grid using external wall-clock
    throughput + real invocation counts (see run_one_true's docstring).
    W in {1,2,4,8,16,24,32} x task_ns in {500,2000,10000,50000,100000} x
    scheduler in {workstealing, custom}, 5 reps -- the exact grid asked for
    in the coordinator's ready-queue-bottleneck follow-up."""
    manifest = json.loads(MANIFEST_PATH.read_text())
    rows = []
    for scheduler in SCHEDULERS:
        for ns in SIZE_SWEEP_NS:
            for w in W_SWEEP:
                k = frames_for_ns(ns)
                warmup = max(1, k // 10)
                for rep in range(1, reps + 1):
                    tag = f"true_{scheduler}_ns{ns}_w{w}"
                    print(f"[true-sweep] {tag} rep={rep}/{reps} k={k}", flush=True)
                    row = run_one_true(
                        manifest, workers=w, task_ns=ns, scheduler=scheduler,
                        k=k, warmup=warmup, tag=tag, rep=rep,
                    )
                    rows.append(row)
                    print(f"  true_tasks/s={row['true_tasks_per_sec']:.0f} "
                          f"true_eff={_fnum(row['true_efficiency'], '.3f')} "
                          f"(old reported_fps={row['reported_fps']})", flush=True)
    out = RESULTS_DIR / "true_sweep_results.json"
    out.write_text(json.dumps(rows, indent=2))
    print(f"[true-sweep] wrote {out}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--build", action="store_true")
    p.add_argument("--sweep-w", action="store_true")
    p.add_argument("--sweep-size", action="store_true")
    p.add_argument("--tuned-sweep-w", action="store_true")
    p.add_argument("--true-sweep", action="store_true")
    p.add_argument("--reps", type=int, default=5)
    args = p.parse_args()

    if args.build:
        do_build()
    if args.sweep_w:
        do_sweep_w(args.reps)
    if args.sweep_size:
        do_sweep_size(args.reps)
    if args.tuned_sweep_w:
        do_tuned_sweep_w(args.reps)
    if args.true_sweep:
        do_true_sweep(args.reps)
    if not (args.build or args.sweep_w or args.sweep_size or args.tuned_sweep_w or args.true_sweep):
        p.print_help()


if __name__ == "__main__":
    main()
