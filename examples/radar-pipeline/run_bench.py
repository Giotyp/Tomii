"""FMCW radar pipeline: build, run, and verify end-to-end.

Flow: build kernels/libradar_kernels.so (Make) -> build the plugin dylib and
tomii-core (Cargo, FUNC_PATH) -> generate the graph JSON from the scene ->
start the Tomii receiver -> wait -> start sender.py -> wait for completion ->
verify detections against the scene ground truth.

Usage (from repo root):
    python3 examples/radar-pipeline/run_bench.py
    python3 examples/radar-pipeline/run_bench.py --frames 100 --workers 8 --slots 2
    python3 examples/radar-pipeline/run_bench.py --no-verify --frames 500
"""

from __future__ import annotations

import argparse
import os
import re
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


def parse_avg_ms(timing_file: Path) -> float:
    if not timing_file.exists():
        return float("nan")
    m = re.search(r"Avg Time Per Frame:\s+([\d.]+)(ms|µs|us|s)", timing_file.read_text())
    if not m:
        return float("nan")
    val, unit = float(m.group(1)), m.group(2)
    if unit in ("µs", "us"):
        return val / 1e3
    if unit == "s":
        return val * 1e3
    return val


def build_all(clean: bool) -> tuple[str, str]:
    print("Building radar kernels (FFTW)...", flush=True)
    subprocess.run(["make", "-C", str(HERE / "kernels")], check=True)

    env = {**os.environ, "FUNC_PATH": str(HERE / "src" / "lib.rs")}
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


def main() -> None:
    p = argparse.ArgumentParser(description="Radar pipeline end-to-end run.")
    p.add_argument("--scene", type=Path, default=HERE / "data" / "out" / "scene.json")
    p.add_argument("--frames", type=int, default=None,
                   help="frames to send/process (default: frames in the scene)")
    p.add_argument("--frame-period", type=float, default=None,
                   help="sender frame period in seconds (default: scene value)")
    p.add_argument("--chirp-gap-us", type=float, default=0.0,
                   help="sender pause between chirp packets [us]")
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--slots", type=int, default=2)
    p.add_argument("--system-threads", type=int, default=2)
    p.add_argument("--receiver-threads", type=int, default=2)
    p.add_argument("--tiles", type=int, default=8)
    p.add_argument("--guard", type=int, default=2)
    p.add_argument("--train", type=int, default=8)
    p.add_argument("--pfa-scale", type=float, default=15.0)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8100)
    p.add_argument("--warmup", type=int, default=10,
                   help="leading frames excluded from timing averages")
    p.add_argument("--max-runtime", type=int, default=None,
                   help="watchdog seconds (default: derived from frames x period)")
    p.add_argument("--results-dir", type=Path, default=HERE / "results")
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

    import json
    scene_meta = json.loads(args.scene.read_text())
    n_samples, n_chirps = radar_dims_from_scene(args.scene)
    scene_frames = scene_meta["radar"]["n_frames"]
    frame_period = args.frame_period or scene_meta["radar"]["frame_interval_s"]
    frames = args.frames if args.frames is not None else scene_frames

    args.results_dir.mkdir(parents=True, exist_ok=True)
    dylib, binary = build_all(clean=args.clean)

    # Graph JSON — frame_wnd must cover the concurrent slots.
    if args.graph is not None:
        graph_json = args.graph
    else:
        graph = build_radar_graph(
            n_samples,
            n_chirps,
            n_tiles=args.tiles,
            frame_wnd=args.slots,
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
        print(f"Graph written to: {graph_json}", flush=True)

    timing_file = args.results_dir / f"radar_s{args.slots}_w{args.workers}.txt"
    verify_path = args.results_dir / "detections.txt"
    if verify_path.exists():
        verify_path.unlink()

    send_window_s = frames * frame_period
    max_runtime = args.max_runtime or int(SENDER_DELAY_S + send_window_s + 30)

    cmd = build_command(
        binary,
        str(graph_json),
        dylib,
        workers=args.workers,
        core_offset=1,
        system_threads=args.system_threads,
        receiver_threads=args.receiver_threads,
        slots=args.slots,
        max_frames=frames,
        exclude_frames=args.warmup,
        max_runtime=max_runtime,
        timing=str(timing_file),
        use_rdtsc=True,
        custom=True,
        coalesce_barriers=True,
        inline_continuation=True,
        slot_priority=True,
    )

    env = {**os.environ}
    if args.verify:
        env["TOMII_VERIFY_PATH"] = str(verify_path)

    print(f"\n=== radar | frames={frames} slots={args.slots} workers={args.workers} "
          f"period={frame_period * 1e3:.1f}ms ===", flush=True)
    t0 = time.monotonic()
    tomii_proc = subprocess.Popen(cmd, env=env)

    time.sleep(SENDER_DELAY_S)
    sender_cmd = [
        sys.executable, str(HERE / "sender.py"),
        "--scene", str(args.scene),
        "--host", args.host, "--port", str(args.port),
        "--frames", str(frames),
        "--frame-period", str(frame_period),
    ]
    if args.chirp_gap_us:
        sender_cmd += ["--chirp-gap-us", str(args.chirp_gap_us)]
    sender_proc = subprocess.Popen(sender_cmd)
    print("  sender started", flush=True)

    watchdog = max_runtime + 15
    try:
        ret = tomii_proc.wait(timeout=watchdog)
    except subprocess.TimeoutExpired:
        tomii_proc.kill()
        if sender_proc.poll() is None:
            sender_proc.kill()
        raise RuntimeError(f"Tomii hung (>{watchdog}s)")
    finally:
        if sender_proc.poll() is None:
            sender_proc.terminate()
    t1 = time.monotonic()

    if ret != 0:
        raise subprocess.CalledProcessError(ret, cmd)

    latency_ms = parse_avg_ms(timing_file)
    print(f"  latency: {latency_ms:.4f} ms/frame  (wall: {(t1 - t0):.1f} s)", flush=True)

    if args.verify:
        print("\nVerifying detections against ground truth...", flush=True)
        rc = subprocess.run(
            [
                sys.executable, str(HERE / "verify.py"),
                "--scene", str(args.scene),
                "--detections", str(verify_path),
            ]
        ).returncode
        if rc != 0:
            sys.exit(rc)


if __name__ == "__main__":
    main()
