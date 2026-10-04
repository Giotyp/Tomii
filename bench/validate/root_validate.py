"""Root-node completion-accounting validation (fix cd7e4b1).

Correctness matrix with ground-truth call counts, plus the item-5 barrier
regression and the bare successor-less fan-out re-measurement.

Ground truth: `busy_ns` in `kernel/src/lib.rs` (byte-identical to
bench/micro/runtime-bench/kernel) counts every real invocation in a process-
global atomic and prints `BUSY_NS_CALLS=<n>` at exit. Every cell asserts
    BUSY_NS_CALLS == expected_calls_per_frame * frames
    summary.stale_task_drops == 0   (new report field from cd7e4b1)
    completed frames == max_frames  (from the runtime's INFO slot-completion log)
    no hang                         (timeout; SIGUSR1 -> --dump-state first)

Build (NUMA1, separate target dirs so no shared binary is clobbered):
    cd kernel && CARGO_TARGET_DIR=<wt>/target-rootval-kernel \
        taskset -c 32-63 nice -n 10 cargo build --release
    FUNC_PATH=<wt>/bench/root-validate/kernel/src/lib.rs CARGO_TARGET_DIR=<wt>/target-rootval \
        taskset -c 32-63 nice -n 10 cargo build -r -p tomii-core --bin main

Usage:
    python3 root_validate.py --matrix
    python3 root_validate.py --barrier-regress --reps 5
    python3 root_validate.py --bare-sweep --reps 5
"""

from __future__ import annotations

import argparse
import fcntl
import itertools
import json
import re
import signal
import statistics
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO))
import tomii as tm  # noqa: E402

import os  # noqa: E402

# ROOTVAL_BINARY / ROOTVAL_TAG: point at a pre-fix build (27a2edf, built from a
# `git archive` into target-rootval-prefix/) to run the same matrix as a control.
BINARY = Path(os.environ.get("ROOTVAL_BINARY", REPO / "target-rootval" / "release" / "main"))
RUN_TAG = os.environ.get("ROOTVAL_TAG", "")
DYLIB = REPO / "target-rootval-kernel" / "release" / "librootval_kernel.so"
LOCKFILE = "/home/george/Tomii/.eval-server.lock"
MEASURE_CORES = "0-31"
RES = HERE / "results"
GRAPHS = RES / "graphs"

NS = 2000
BIG = 1e300

_ANSI = re.compile(r"\x1b\[[0-9;]*m")
_CALLS = re.compile(r"BUSY_NS_CALLS=(\d+)")
_COMPLETED = re.compile(r"completed=(\d+)")


# --------------------------------------------------------------------------- graphs
def g_fanout(m=2048):
    """(a) successor-less root fan-out."""
    app = tm.Graph()
    ns = app.var("ns", tm.usize(NS))
    app.node("fanout", func="busy_ns", args=[tm.f64(1.0), ns], factor=m)
    return app, m


def g_filtered(k, m=16):
    """(b) root with a filtered successor: c reads a.out(k) only."""
    app = tm.Graph()
    ns = app.var("ns", tm.usize(NS))
    a = app.node("a", func="busy_ns", args=[tm.f64(1.0), ns], factor=m)
    app.node("c", func="busy_ns", args=[a.out(k), ns], factor=1)
    return app, m + 1


def g_barrier(m=2048):
    """(c) root with a full $barrier successor (the covered case)."""
    app = tm.Graph()
    ns = app.var("ns", tm.usize(NS))
    f = app.node("fanout", func="busy_ns", args=[tm.f64(1.0), ns], factor=m)
    app.node("sink", func="sink_probe", args=[f.wait(0, m)], factor=1)
    return app, m


def g_cond_root(passes, m=16):
    """(d1) conditional root: node-level condition, no predecessors."""
    app = tm.Graph()
    ns = app.var("ns", tm.usize(NS))
    thr = app.var("thr", tm.f64(-BIG if passes else BIG))
    cond = tm.Condition(operation="Eq", value=True, value_type="bool",
                        func="cond_gt", args=[tm.f64(1.0), thr])
    app.node("r", func="busy_ns", args=[tm.f64(1.0), ns], factor=m, condition=cond)
    return app, (m if passes else 0)


def g_cond_succ(passes, m=16):
    """(d2) root with a 1:1 conditional successor -> root completes through
    the batch-resolution path (not worker_resolvable)."""
    app = tm.Graph()
    ns = app.var("ns", tm.usize(NS))
    thr = app.var("thr", tm.f64(-BIG if passes else BIG))
    r = app.node("r", func="busy_ns", args=[tm.f64(1.0), ns], factor=m)
    cond = tm.Condition(operation="Eq", value=True, value_type="bool",
                        func="cond_gt", args=[r.out(), thr])
    app.node("s", func="busy_ns", args=[r.out(), ns], factor=m, condition=cond)
    return app, (2 * m if passes else m)


def g_bulk_chain(m=256):
    """(e) root -> 1:1 successor with equal factor: fanout-bulk eligible, so
    --no-fanout-bulk actually changes the dispatch path."""
    app = tm.Graph()
    ns = app.var("ns", tm.usize(NS))
    r = app.node("r", func="busy_ns", args=[tm.f64(1.0), ns], factor=m)
    app.node("s", func="busy_ns", args=[r.out(), ns], factor=m)
    return app, 2 * m


GRAPH_SET = {
    "a_fanout2048": lambda: g_fanout(),
    "b_filt_k0": lambda: g_filtered(0),
    "b_filt_k5": lambda: g_filtered(5),
    "b_filt_k15": lambda: g_filtered(15),
    "c_barrier2048": lambda: g_barrier(),
    "d1_condroot_pass": lambda: g_cond_root(True),
    "d1_condroot_fail": lambda: g_cond_root(False),
    "d2_condsucc_pass": lambda: g_cond_succ(True),
    "d2_condsucc_fail": lambda: g_cond_succ(False),
    "e_bulkchain256": lambda: g_bulk_chain(),
}


def write_graph(name, builder):
    GRAPHS.mkdir(parents=True, exist_ok=True)
    app, per_frame = builder()
    p = GRAPHS / f"{name}.json"
    p.write_text(app.to_json(), encoding="utf-8")
    return p, per_frame


# --------------------------------------------------------------------------- runner
def run_cell(graph_json: Path, *, tag: str, workers: int, custom: bool, inline: bool,
             no_bulk: bool, slots: int, frames: int, warmup: int, timeout_s: float,
             outdir: Path, report: bool = True, extra: list | None = None) -> dict:
    outdir.mkdir(parents=True, exist_ok=True)
    rep = outdir / f"{tag}.report.json"
    dump = outdir / f"{tag}.dump.json"
    log = outdir / f"{tag}.log"
    for p in (rep, dump):
        p.unlink(missing_ok=True)
    cmd = [str(BINARY), "--json", str(graph_json), "--dylib", str(DYLIB),
           "--workers", str(workers), "--slots", str(slots),
           "--max-frames", str(frames), "--exclude-frames", str(warmup),
           "--dump-state", str(dump)]
    if report:
        # write_json_report needs the timing buffer, i.e. --timing too.
        cmd += ["--report", str(rep), "--timing", str(outdir / f"{tag}.timing.txt")]
    if custom:
        cmd.append("--custom")
    if inline:
        cmd.append("--inline-continuation")
    if no_bulk:
        cmd.append("--no-fanout-bulk")
    cmd += extra or []
    full = ["taskset", "-c", MEASURE_CORES] + cmd

    hung = False
    with open(LOCKFILE, "w") as lk:
        fcntl.flock(lk, fcntl.LOCK_EX)
        try:
            t0 = time.perf_counter()
            with open(log, "w") as lf:
                proc = subprocess.Popen(full, stdout=lf, stderr=subprocess.STDOUT)
                try:
                    proc.wait(timeout=timeout_s)
                except subprocess.TimeoutExpired:
                    hung = True
                    proc.send_signal(signal.SIGUSR1)  # live snapshot first
                    time.sleep(3)
                    proc.kill()
                    proc.wait()
            t1 = time.perf_counter()
        finally:
            fcntl.flock(lk, fcntl.LOCK_UN)

    text = _ANSI.sub("", log.read_text(errors="replace"))
    m = _CALLS.search(text)
    calls = int(m.group(1)) if m else 0  # no line == busy_ns never called
    comp = [int(x) for x in _COMPLETED.findall(text)]
    stale = None
    summary = {}
    if report and rep.exists():
        summary = json.loads(rep.read_text()).get("summary", {})
        stale = summary.get("stale_task_drops")
    warn = "stale-task guard dropped" in text
    return {
        "tag": tag, "rc": proc.returncode, "hung": hung, "wall_s": t1 - t0,
        "calls": calls, "frames_completed": max(comp) if comp else 0,
        "stale_task_drops": stale, "stale_warn": warn,
        "total_tasks_per_frame": summary.get("total_tasks_per_frame"),
        "log": str(log), "dump": str(dump) if dump.exists() else None,
        "log_text": text,
    }


# --------------------------------------------------------------------------- matrix
W_SET = [1, 4, 16]
MATRIX_FRAMES = 20
MATRIX_WARMUP = 2


def do_matrix(slots_set):
    out = RES / f"matrix{RUN_TAG}"
    rows = []
    graphs = {n: write_graph(n, b) for n, b in GRAPH_SET.items()}
    for (name, (gj, per_frame)), slots, inline, no_bulk, w, custom in itertools.product(
            graphs.items(), slots_set, [False, True], [False, True], W_SET, [False, True]):
        tag = (f"{name}_s{slots}_{'inl' if inline else 'noinl'}_"
               f"{'nobulk' if no_bulk else 'bulk'}_w{w}_{'custom' if custom else 'ws'}")
        r = run_cell(gj, tag=tag, workers=w, custom=custom, inline=inline, no_bulk=no_bulk,
                     slots=slots, frames=MATRIX_FRAMES, warmup=MATRIX_WARMUP,
                     timeout_s=30, outdir=out)
        expected = per_frame * MATRIX_FRAMES
        ok = (r["rc"] == 0 and not r["hung"] and r["calls"] == expected
              and r["stale_task_drops"] == 0 and not r["stale_warn"]
              and r["frames_completed"] == MATRIX_FRAMES)
        row = {"graph": name, "slots": slots, "inline": inline, "no_fanout_bulk": no_bulk,
               "workers": w, "scheduler": "custom" if custom else "ws",
               "expected_calls": expected, "ok": ok,
               **{k: v for k, v in r.items() if k != "log_text"}}
        rows.append(row)
        print(f"[matrix] {tag}: calls={r['calls']}/{expected} stale={r['stale_task_drops']} "
              f"frames={r['frames_completed']}/{MATRIX_FRAMES} rc={r['rc']} hung={r['hung']} "
              f"{'OK' if ok else 'FAIL'}", flush=True)
    (RES / f"matrix{RUN_TAG}_results.json").write_text(json.dumps(rows, indent=2))
    n_fail = sum(not r["ok"] for r in rows)
    print(f"[matrix] {len(rows) - n_fail}/{len(rows)} cells OK")


# --------------------------------------------------------------------------- throughput
def _log_fps(text, warmup, frames):
    """Frame-completion rate from the runtime's INFO slot-completion log
    (same method (b) as item5_scheduler_scaling._log_fps)."""
    done = {}
    for line in text.splitlines():
        if "slot completed" not in line or "completed=" not in line:
            continue
        n = int(line.split("completed=")[1].split()[0])
        t = datetime.fromisoformat(line.split()[0].rstrip("Z")).timestamp()
        done.setdefault(n, t)
    if warmup not in done or frames not in done:
        return float("nan")
    return (frames - warmup) / (done[frames] - done[warmup])


def frames_for(ns, target_s=0.4, m=2048):
    return max(8, int(target_s / (ns * 1e-9)) // m)


def _throughput_sweep(name, builder, workers_set, reps, out_name, ns_set=(2000,)):
    """Item-5 method: uninstrumented K1 vs K2=3*K1 process-wall differential
    ((K2-K1)/(T2-T1)) cross-checked by the K2 run's log timeline. Real calls
    come from BUSY_NS_CALLS on the timed runs themselves (no calibration
    step needed: the fix makes the per-frame count exact)."""
    out = RES / f"throughput{RUN_TAG}"
    rows = []
    for ns in ns_set:
        global NS
        NS = ns
        gj, per_frame = write_graph(f"{name}_ns{ns}", builder)
        for custom in (False, True):
            for w in workers_set:
                k1 = frames_for(ns)
                k2 = 3 * k1
                for rep in range(1, reps + 1):
                    base = f"{name}_ns{ns}_{'custom' if custom else 'ws'}_w{w}_r{rep}"
                    rr = []
                    for k in (k1, k2):
                        r = run_cell(gj, tag=f"{base}_k{k}", workers=w, custom=custom,
                                     inline=False, no_bulk=False, slots=1, frames=k,
                                     warmup=max(1, k // 10), timeout_s=120, outdir=out,
                                     report=False)
                        r["k"] = k
                        r["log_fps"] = _log_fps(r["log_text"], max(1, k // 10), k)
                        rr.append(r)
                    r1, r2 = rr
                    diff_fps = (k2 - k1) / (r2["wall_s"] - r1["wall_s"])
                    calls_ok = (r1["calls"] == per_frame * k1 and r2["calls"] == per_frame * k2
                                and not r1["hung"] and not r2["hung"]
                                and not r1["stale_warn"] and not r2["stale_warn"])
                    row = {"graph": name, "task_ns": ns, "scheduler": "custom" if custom else "ws",
                           "workers": w, "rep": rep, "k1": k1, "k2": k2,
                           "calls_k1": r1["calls"], "calls_k2": r2["calls"],
                           "calls_per_frame": r2["calls"] / k2, "calls_ok": calls_ok,
                           "stale_warn": r1["stale_warn"] or r2["stale_warn"],
                           "diff_fps": diff_fps,
                           "diff_tasks_per_sec": diff_fps * per_frame,
                           "log_tasks_per_sec_k2": r2["log_fps"] * per_frame}
                    rows.append(row)
                    print(f"[{name}] ns={ns} {row['scheduler']} w={w} rep={rep} "
                          f"calls/frame={row['calls_per_frame']:.1f} ok={calls_ok} "
                          f"diff_tasks/s={row['diff_tasks_per_sec']:.0f} "
                          f"log_tasks/s={row['log_tasks_per_sec_k2']:.0f}", flush=True)
    (RES / out_name.replace(".json", f"{RUN_TAG}.json")).write_text(json.dumps(rows, indent=2))
    summarize(rows)


def summarize(rows):
    key = lambda r: (r["task_ns"], r["scheduler"], r["workers"])  # noqa: E731
    for k, grp in itertools.groupby(sorted(rows, key=key), key=key):
        grp = list(grp)
        d = [g["diff_tasks_per_sec"] for g in grp]
        lg = [g["log_tasks_per_sec_k2"] for g in grp]
        print(f"  ns={k[0]} {k[1]:6s} W={k[2]:2d}: diff med={statistics.median(d)/1e6:.3f}M "
              f"[{min(d)/1e6:.3f},{max(d)/1e6:.3f}] log med={statistics.median(lg)/1e6:.3f}M "
              f"calls_ok={all(g['calls_ok'] for g in grp)}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--matrix", action="store_true")
    p.add_argument("--slots", default="1,2")
    p.add_argument("--barrier-regress", action="store_true")
    p.add_argument("--bare-sweep", action="store_true")
    p.add_argument("--reps", type=int, default=5)
    a = p.parse_args()
    if a.matrix:
        do_matrix([int(s) for s in a.slots.split(",")])
    if a.barrier_regress:
        _throughput_sweep("barrier", lambda: g_barrier(), [8, 24], a.reps,
                          "barrier_regress_results.json")
    if a.bare_sweep:
        _throughput_sweep("bare", lambda: g_fanout(), [1, 2, 4, 8, 16, 24, 32], a.reps,
                          "bare_sweep_results.json")


if __name__ == "__main__":
    main()
