"""Item 2: slot scaling (static topology sharing) with the real Tomii runtime.

Linear chain of N=128 nodes, W=8 workers, K=512 frames, ~2us busy tasks,
sweep S (concurrent slots) in {1,2,4,8,16}. Reports frames/sec (from the
runtime's own --report JSON: throughput_frames_per_sec, p50/p99 latency)
and peak RSS (via `/usr/bin/time -v`) per S.

Usage:
    # Build (once) — must run off the measurement lock, on the build core set:
    taskset -c 32-63 nice -n 10 python3 item2_slot_scaling.py --build

    # Measure (5 reps per S, each under the eval-server lock):
    python3 item2_slot_scaling.py --sweep --reps 5
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent  # bench/micro/runtime-bench/
REPO_ROOT = HERE.parents[2]  # workspace root
sys.path.insert(0, str(REPO_ROOT))

import tomii as tm
from tomii._runner import build_command

KERNEL_DIR = HERE / "kernel"
KERNEL_LIB_RS = KERNEL_DIR / "src" / "lib.rs"
LOCKFILE = "/home/george/Tomii/.eval-server.lock"
MEASURE_CORES = "0-31"
BUILD_CORES = "32-63"

N_NODES = 128
WORKERS = 8
K_FRAMES = 512
WARMUP_FRAMES = 32
BUSY_NS = 2000
SLOTS_SWEEP = [1, 2, 4, 8, 16]

MANIFEST_PATH = HERE / "results" / "item2_manifest.json"
RESULTS_DIR = HERE / "results" / "item2"


def build_chain_graph(n_nodes: int, busy_ns: int) -> tm.Graph:
    app = tm.Graph()
    ns_var = app.var("ns", tm.usize(busy_ns))
    prev = app.node("n0", func="busy_ns", args=[tm.f64(1.0), ns_var])
    for i in range(1, n_nodes):
        prev = app.node(f"n{i}", func="busy_ns", args=[prev.out(), ns_var])
    return app


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

    graph = build_chain_graph(N_NODES, BUSY_NS)
    graph_json_path = RESULTS_DIR / "chain_graph.json"
    graph_json_path.write_text(graph.to_json(), encoding="utf-8")

    print("[build] cargo build tomii-core main (FUNC_PATH=kernel)...", flush=True)
    result = graph.build(func_path=str(KERNEL_LIB_RS), release=True, clean=False)

    manifest = {
        "dylib": dylib,
        "binary": result.binary,
        "graph_json": str(graph_json_path),
        "n_nodes": N_NODES,
        "workers": WORKERS,
        "k_frames": K_FRAMES,
        "warmup_frames": WARMUP_FRAMES,
        "busy_ns": BUSY_NS,
    }
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"[build] manifest written: {MANIFEST_PATH}", flush=True)
    print(f"[build] dylib={dylib}")
    print(f"[build] binary={result.binary}")


_RSS_RE = re.compile(r"Maximum resident set size \(kbytes\):\s*(\d+)")


def run_one(
    manifest: dict,
    slots: int,
    rep: int,
    system_threads: int = 1,
    tag: str = "",
    extra_flags: dict | None = None,
) -> dict:
    suffix = f"s{slots}_r{rep}{tag}"
    report_path = RESULTS_DIR / f"report_{suffix}.json"
    timing_path = RESULTS_DIR / f"timing_{suffix}.txt"
    rss_path = RESULTS_DIR / f"rss_{suffix}.txt"

    cmd = build_command(
        manifest["binary"],
        manifest["graph_json"],
        manifest["dylib"],
        workers=WORKERS,
        slots=slots,
        system_threads=system_threads,
        max_frames=K_FRAMES,
        exclude_frames=WARMUP_FRAMES,
        report=str(report_path),
        timing=str(timing_path),  # required: --report reads the timing buffer,
        # which is only populated when --timing is also set (timing_enabled
        # gates collection in tomii-core/src/bin/main.rs).
        use_rdtsc=True,
        **(extra_flags or {}),
    )
    full_cmd = [
        "flock",
        LOCKFILE,
        "taskset",
        "-c",
        MEASURE_CORES,
        "/usr/bin/time",
        "-v",
        "-o",
        str(rss_path),
    ] + cmd

    proc = subprocess.run(full_cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        print(proc.stdout, file=sys.stderr)
        print(proc.stderr, file=sys.stderr)
        raise RuntimeError(f"run failed (slots={slots} rep={rep}): rc={proc.returncode}")

    report = json.loads(report_path.read_text())
    summary = report.get("summary", {})
    time_text = rss_path.read_text()
    m = _RSS_RE.search(time_text)
    rss_kb = int(m.group(1)) if m else None

    worker_busy = report.get("resource_utilization", {}).get("worker_busy_pct", [])
    worker_busy_avg = sum(worker_busy) / len(worker_busy) if worker_busy else None

    return {
        "slots": slots,
        "rep": rep,
        "system_threads": system_threads,
        "extra_flags": sorted((extra_flags or {}).keys()),
        "rss_kb": rss_kb,
        "avg_latency_us": summary.get("avg_latency_us"),
        "p50_latency_us": summary.get("p50_latency_us"),
        "p99_latency_us": summary.get("p99_latency_us"),
        "p999_latency_us": summary.get("p999_latency_us"),
        "throughput_frames_per_sec": summary.get("throughput_frames_per_sec"),
        "total_frames": summary.get("total_frames"),
        "critical_path_exec_us": summary.get("scheduling_overhead_diagnostic", {}).get(
            "critical_path_exec_us"
        ),
        "overhead_pct": summary.get("scheduling_overhead_diagnostic", {}).get("overhead_pct"),
        "worker_busy_pct_avg": worker_busy_avg,
    }


def do_sweep(reps: int, system_threads: int = 1, tag: str = "", slots_sweep=None) -> None:
    manifest = json.loads(MANIFEST_PATH.read_text())
    all_rows = []
    for slots in (slots_sweep or SLOTS_SWEEP):
        for rep in range(1, reps + 1):
            print(f"[sweep{tag}] slots={slots} system_threads={system_threads} rep={rep}/{reps}", flush=True)
            row = run_one(manifest, slots, rep, system_threads=system_threads, tag=tag)
            all_rows.append(row)
            print(f"  throughput_fps={row.get('throughput_frames_per_sec')} "
                  f"p50_us={row.get('p50_latency_us')} p99_us={row.get('p99_latency_us')} "
                  f"rss_kb={row.get('rss_kb')}", flush=True)

    out_path = RESULTS_DIR / f"sweep_results{tag}.json"
    out_path.write_text(json.dumps(all_rows, indent=2), encoding="utf-8")
    print(f"[sweep{tag}] wrote {out_path}", flush=True)


# Tuned configurations, per the recommended fast path for chain-dominant
# (factor=1) graphs: --custom --inline-continuation, swept over
# system_threads in {1,2,4}. slot_priority is included as an optional 4th
# arm (round-robins single-active-slot processing for cache locality; not
# expected to matter much here since it forces only one slot Active at a
# time, defeating the point of S>1, but included for completeness).
TUNED_CONFIGS: dict[str, dict] = {
    "tuned_st1": {"custom": True, "inline_continuation": True},
    "tuned_st2": {"custom": True, "inline_continuation": True},
    "tuned_st4": {"custom": True, "inline_continuation": True},
    "tuned_slotprio_st1": {"custom": True, "inline_continuation": True, "slot_priority": True},
}
TUNED_SYSTEM_THREADS: dict[str, int] = {
    "tuned_st1": 1,
    "tuned_st2": 2,
    "tuned_st4": 4,
    "tuned_slotprio_st1": 1,
}


def do_tuned_sweep(reps: int, slots_sweep=None) -> None:
    manifest = json.loads(MANIFEST_PATH.read_text())
    for cfg_name, flags in TUNED_CONFIGS.items():
        st = TUNED_SYSTEM_THREADS[cfg_name]
        all_rows = []
        for slots in (slots_sweep or SLOTS_SWEEP):
            for rep in range(1, reps + 1):
                print(f"[tuned:{cfg_name}] slots={slots} system_threads={st} rep={rep}/{reps}", flush=True)
                row = run_one(
                    manifest, slots, rep, system_threads=st,
                    tag=f"_{cfg_name}", extra_flags=flags,
                )
                all_rows.append(row)
                print(f"  throughput_fps={row.get('throughput_frames_per_sec')} "
                      f"worker_busy_pct_avg={row.get('worker_busy_pct_avg')} "
                      f"overhead_pct={row.get('overhead_pct')}", flush=True)
        out_path = RESULTS_DIR / f"sweep_results_{cfg_name}.json"
        out_path.write_text(json.dumps(all_rows, indent=2), encoding="utf-8")
        print(f"[tuned:{cfg_name}] wrote {out_path}", flush=True)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--build", action="store_true")
    p.add_argument("--sweep", action="store_true")
    p.add_argument("--tuned-sweep", action="store_true")
    p.add_argument("--reps", type=int, default=5)
    p.add_argument("--system-threads", type=int, default=1)
    p.add_argument("--tag", type=str, default="")
    p.add_argument("--slots", type=int, nargs="+", default=None)
    args = p.parse_args()

    if args.build:
        do_build()
    if args.sweep:
        do_sweep(args.reps, system_threads=args.system_threads, tag=args.tag, slots_sweep=args.slots)
    if args.tuned_sweep:
        do_tuned_sweep(args.reps, slots_sweep=args.slots)
    if not args.build and not args.sweep and not args.tuned_sweep:
        p.print_help()


if __name__ == "__main__":
    main()
