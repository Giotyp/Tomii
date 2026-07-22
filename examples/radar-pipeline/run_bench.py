"""FMCW radar pipeline: build, run, verify, and measure latency/jitter.

Flow: build kernels/libradar_kernels.so (Make) -> build the plugin dylib and
tomii-core (Cargo, FUNC_PATH) -> generate the graph JSON from the scene ->
start the Tomii receiver -> wait -> start sender.py -> wait for completion ->
verify detections against the scene ground truth -> report latency
percentiles (p50/p99/p99.9) from the runtime's JSON report.

Every cell is verifier-gated: a perf number is only reported when every
ground-truth target was detected in every frame.

Usage (from repo root):
    python3 examples/radar-pipeline/run_bench.py --frames 100
    python3 examples/radar-pipeline/run_bench.py --frames 2000 --frame-period 0.005
    python3 examples/radar-pipeline/run_bench.py --sweep-workers 2 4 8 --sweep-slots 1 2 4
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[1]
sys.path.insert(0, str(REPO_ROOT))

from tomii._runner import build_command
from build_graph import build_radar_graph, radar_dims_from_scene

SENDER_DELAY_S = 5  # receiver-first startup order


def build_all(clean: bool, gpu: bool = False) -> tuple[str, str]:
    print("Building radar kernels (FFTW)...", flush=True)
    subprocess.run(["make", "-C", str(HERE / "kernels")], check=True)
    env = {**os.environ, "FUNC_PATH": str(HERE / "src" / "lib.rs")}
    if gpu:
        print("Building radar kernels (CUDA/cuFFT)...", flush=True)
        subprocess.run(["make", "-C", str(HERE / "kernels"), "gpu"], check=True)
        # build.rs links/rpaths libradar_kernels.so from this dir instead.
        env["RADAR_KERNELS_DIR"] = str(HERE / "kernels" / "gpu")
    if clean:
        subprocess.run(
            ["cargo", "clean", "--manifest-path", str(HERE / "Cargo.toml")], check=True
        )

    print("Building radar plugin...", flush=True)
    subprocess.run(
        ["cargo", "build", "--manifest-path", str(HERE / "Cargo.toml"), "--release"],
        check=True,
        env=env,
    )
    print("Building tomii-core (radar function registry)...", flush=True)
    subprocess.run(
        ["cargo", "build", "-p", "tomii-core", "--bin", "main", "--release"],
        check=True,
        env=env,
        cwd=str(REPO_ROOT),
    )
    dylib = str(HERE / "target" / "release" / "libradar_pipeline_tomii.so")
    binary = str(REPO_ROOT / "target" / "release" / "main")
    return dylib, binary


def latency_summary(report_path: Path) -> dict:
    """Extract frame latency/jitter statistics from the runtime JSON report."""
    report = json.loads(report_path.read_text())
    s = report["summary"]
    lat = report.get("frame_latencies_us", [])
    out = {
        "frames": s["total_frames"],
        "avg_us": s["avg_latency_us"],
        "p50_us": s["p50_latency_us"],
        "p99_us": s["p99_latency_us"],
        "p999_us": s.get("p999_latency_us"),
        "throughput_fps": s["throughput_frames_per_sec"],
    }
    if lat:
        mean = sum(lat) / len(lat)
        out["min_us"] = lat[0]
        out["max_us"] = lat[-1]
        out["std_us"] = (sum((v - mean) ** 2 for v in lat) / len(lat)) ** 0.5
    return out


def aggregate_median(runs: list[dict]) -> dict:
    """Median-of-N across repeat runs of one cell (matches compare.py's method)."""
    if len(runs) == 1:
        return runs[0]
    keys = ["avg_us", "p50_us", "p99_us", "p999_us", "throughput_fps",
            "min_us", "max_us", "std_us"]
    out = dict(runs[0])  # carry workers/slots/frames/etc from the first run
    for k in keys:
        vals = [r[k] for r in runs if r.get(k) is not None]
        if vals:
            out[k] = statistics.median(vals)
    out["repeats"] = len(runs)
    out["verified"] = None if any(r["verified"] is None for r in runs) \
        else all(r["verified"] for r in runs)
    out["wall_s"] = sum(r["wall_s"] for r in runs)
    return out


def run_cell(
    *,
    args,
    dylib: str,
    binary: str,
    graph_json: Path,
    workers: int,
    slots: int,
    frames: int,
    frame_period: float,
    run_index: int | None = None,
) -> dict:
    tag = f"s{slots}_w{workers}"
    timing_file = args.results_dir / f"radar_{tag}.txt"
    report_file = args.results_dir / f"radar_report_{tag}.json"
    verify_path = args.results_dir / f"detections_{tag}.txt"
    if verify_path.exists():
        verify_path.unlink()

    send_window_s = frames * frame_period
    max_runtime = args.max_runtime or int(SENDER_DELAY_S + send_window_s + 30)

    cmd = build_command(
        binary,
        str(graph_json),
        dylib,
        workers=workers,
        core_offset=1,
        system_threads=args.system_threads,
        receiver_threads=args.receiver_threads,
        slots=slots,
        max_frames=frames,
        exclude_frames=args.warmup,
        max_runtime=max_runtime,
        timing=str(timing_file),
        report=str(report_file),
        use_rdtsc=True,
        custom=True,
        coalesce_barriers=True,
        inline_continuation=True,
        slot_priority=True,
        frame_timeout_ms=(args.frame_timeout_ms or None),
    )

    env = {**os.environ}
    if args.verify:
        env["TOMII_VERIFY_PATH"] = str(verify_path)

    print(
        f"\n=== radar | frames={frames} slots={slots} workers={workers} "
        f"period={frame_period * 1e3:.1f}ms ===",
        flush=True,
    )
    t0 = time.monotonic()
    tomii_log = open(args.results_dir / f"tomii_{tag}.log", "w")
    tomii_proc = subprocess.Popen(cmd, env=env, stdout=tomii_log, stderr=tomii_log)

    time.sleep(SENDER_DELAY_S)
    sender_cmd = [
        sys.executable, str(HERE / "sender.py"),
        "--scene", str(args.scene),
        "--host", args.host, "--port", str(args.port),
        "--frames", str(frames),
        "--frame-period", str(frame_period),
        "--quiet",
    ]
    # Forward the gap whenever the user set it explicitly (including 0, which the
    # sender interprets as "burst the whole frame"). Leaving it unset lets the
    # sender fall back to the scene's physical chirp_interval_s.
    if args.drop_chirp:
        sender_cmd += ["--drop-chirp", args.drop_chirp]
    if args.chirp_gap_us is not None:
        sender_cmd += ["--chirp-gap-us", str(args.chirp_gap_us)]
    sender_proc = subprocess.Popen(sender_cmd)

    watchdog = max_runtime + 15
    try:
        ret = tomii_proc.wait(timeout=watchdog)
    except subprocess.TimeoutExpired:
        tomii_proc.kill()
        raise RuntimeError(f"Tomii hung (>{watchdog}s) slots={slots} workers={workers}")
    finally:
        if sender_proc.poll() is None:
            sender_proc.terminate()
    wall_s = time.monotonic() - t0

    if ret != 0:
        raise subprocess.CalledProcessError(ret, cmd)

    # Verifier gates the perf numbers.
    verified = None
    if args.verify:
        rc = subprocess.run(
            [
                sys.executable, str(HERE / "verify.py"),
                "--scene", str(args.scene),
                "--detections", str(verify_path),
                "--expect-frames", str(frames),
            ]
        ).returncode
        verified = rc == 0
        if not verified:
            raise RuntimeError(f"verification FAILED for slots={slots} workers={workers}")

    # Snapshot the per-run report so repeats don't overwrite each other's
    # provenance (the fixed {tag} name is reused every run).
    if run_index is not None:
        shutil.copyfile(
            report_file, args.results_dir / f"radar_report_{tag}_r{run_index}.json"
        )

    stats = latency_summary(report_file)
    stats.update(workers=workers, slots=slots, wall_s=wall_s, verified=verified)
    print(
        f"  latency us: p50={stats['p50_us']:.0f} p99={stats['p99_us']:.0f} "
        f"p99.9={stats['p999_us']:.0f} max={stats.get('max_us', float('nan')):.0f} "
        f"std={stats.get('std_us', float('nan')):.0f} "
        f"| {stats['frames']} steady-state frames, wall {wall_s:.1f}s",
        flush=True,
    )
    return stats


def main() -> None:
    p = argparse.ArgumentParser(description="Radar pipeline end-to-end run.")
    p.add_argument("--scene", type=Path, default=HERE / "data" / "out" / "scene.json")
    p.add_argument("--frames", type=int, default=None,
                   help="frames to send/process (default: frames in the scene)")
    p.add_argument("--frame-period", type=float, default=None,
                   help="sender frame period in seconds (default: scene value)")
    p.add_argument("--chirp-gap-us", type=float, default=None,
                   help="sender pause between chirp packets [us]; unset -> sender "
                        "uses the scene's physical chirp_interval_s. Pass 0 to force "
                        "a single burst per frame (note: bursts can overrun the "
                        "receiver socket buffer). For compressed streamed pacing, "
                        "set this to frame_period/n_chirps.")
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--slots", type=int, default=2)
    p.add_argument("--sweep-workers", type=int, nargs="+", default=None,
                   help="sweep cells over these worker counts")
    p.add_argument("--sweep-slots", type=int, nargs="+", default=None,
                   help="sweep cells over these slot counts")
    p.add_argument("--system-threads", type=int, default=2)
    p.add_argument("--receiver-threads", type=int, default=2)
    p.add_argument("--tiles", type=int, default=8)
    p.add_argument("--guard", type=int, default=2)
    p.add_argument("--train", type=int, default=8)
    p.add_argument("--pfa-scale", type=float, default=15.0)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8100)
    p.add_argument("--frame-timeout-ms", type=int, default=0,
                   help="evict incomplete frames after this idle time (0=off)")
    p.add_argument("--drop-chirp", default=None, metavar="FRAME:CHIRP",
                   help="sender skips one packet (eviction testing; use --no-verify)")
    p.add_argument("--warmup", type=int, default=10,
                   help="leading frames excluded from timing averages")
    p.add_argument("--repeat", type=int, default=1,
                   help="repeat each cell N times and report the per-column median "
                        "(each run's report is snapshotted to radar_report_<tag>_rN.json)")
    p.add_argument("--max-runtime", type=int, default=None,
                   help="watchdog seconds (default: derived from frames x period)")
    p.add_argument("--results-dir", type=Path, default=HERE / "results")
    p.add_argument("--csv-out", type=Path, default=None,
                   help="summary CSV path (default: results/radar_sweep.csv)")
    p.add_argument("--gpu", action="store_true",
                   help="link the CUDA kernel twin (kernels/gpu) instead of FFTW")
    p.add_argument("--no-verify", dest="verify", action="store_false", default=True)
    p.add_argument("--no-clean", dest="clean", action="store_false", default=True)
    p.add_argument("--graph", type=Path, default=None, help="graph JSON override")
    args = p.parse_args()

    # Generate the scene if missing.
    if not args.scene.exists():
        print(f"Scene {args.scene} missing — generating default...", flush=True)
        subprocess.run(
            [sys.executable, str(HERE / "data" / "make_scene.py")], check=True
        )

    scene_meta = json.loads(args.scene.read_text())
    n_samples, n_chirps = radar_dims_from_scene(args.scene)
    frame_period = args.frame_period or scene_meta["radar"]["frame_interval_s"]
    frames = args.frames if args.frames is not None else scene_meta["radar"]["n_frames"]

    args.results_dir.mkdir(parents=True, exist_ok=True)
    dylib, binary = build_all(clean=args.clean, gpu=args.gpu)

    workers_list = args.sweep_workers or [args.workers]
    slots_list = args.sweep_slots or [args.slots]
    sweep = len(workers_list) * len(slots_list) > 1

    results = []
    for workers in workers_list:
        for slots in slots_list:
            # frame_wnd must cover the concurrent slots.
            if args.graph is not None:
                graph_json = args.graph
            else:
                graph = build_radar_graph(
                    n_samples,
                    n_chirps,
                    n_tiles=args.tiles,
                    frame_wnd=slots,
                    guard=args.guard,
                    train=args.train,
                    pfa_scale=args.pfa_scale,
                    address=args.host,
                    port=args.port,
                )
                tmp = tempfile.NamedTemporaryFile(
                    prefix="radar_graph_", suffix=".json", delete=False, mode="w"
                )
                tmp.write(graph.to_json())
                tmp.close()
                graph_json = Path(tmp.name)

            cell_runs = [
                run_cell(
                    args=args,
                    dylib=dylib,
                    binary=binary,
                    graph_json=graph_json,
                    workers=workers,
                    slots=slots,
                    frames=frames,
                    frame_period=frame_period,
                    run_index=(i if args.repeat > 1 else None),
                )
                for i in range(args.repeat)
            ]
            agg = aggregate_median(cell_runs)
            if args.repeat > 1:
                print(
                    f"  median-of-{args.repeat}: p50={agg['p50_us']:.0f} "
                    f"p99={agg['p99_us']:.0f} p99.9={agg['p999_us']:.0f} us",
                    flush=True,
                )
            results.append(agg)

    if sweep or args.csv_out:
        csv_path = args.csv_out or (args.results_dir / "radar_sweep.csv")
        with open(csv_path, "w") as f:
            f.write(
                "system,slots,workers,frames,p50_us,p99_us,p999_us,max_us,std_us,"
                "throughput_fps\n"
            )
            for r in results:
                f.write(
                    f"tomii,{r['slots']},{r['workers']},{r['frames']},"
                    f"{r['p50_us']},{r['p99_us']},{r['p999_us']},"
                    f"{r.get('max_us', '')},{r.get('std_us', 0):.2f},"
                    f"{r['throughput_fps']}\n"
                )
        print(f"\nSummary written to: {csv_path}", flush=True)


if __name__ == "__main__":
    main()
