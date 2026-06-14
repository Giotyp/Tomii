"""Tomii MIMO benchmark: 4-node uplink pipeline (fft → csi → beam → demul).

Requires:
  - Intel MKL at /opt/intel/oneapi/mkl/2024.0
  - lib/libbeamfuncs.so, lib/libdemod.so, lib/libfftfuncs.so (vendored under lib/)
  - Agora built at ~/Agora (https://github.com/Agora-wireless/Agora)
    The script starts ~/Agora/build/sender automatically for each sweep cell.
Usage (from bench worktree root):
    python mimo-bench/tomii/run_bench.py
    python mimo-bench/tomii/run_bench.py --slots 1 4 16 --workers 2 4 8
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

HERE = Path(__file__).resolve().parent  # mimo-bench/tomii/
BENCH_ROOT = HERE.parents[2]  # workspace root
DEVELOP_ROOT = BENCH_ROOT  # same as workspace root on develop
AGORA_DIR = Path("~/Agora").expanduser().resolve()
sys.path.insert(0, str(DEVELOP_ROOT))

from tomii._runner import build_command, _find_binary
from build_graph import build_mimo_graph


def _parse_avg_ms(timing_file: Path) -> float:
    if not timing_file.exists():
        return float("nan")
    text = timing_file.read_text()
    m = re.search(r"Avg Time Per Stream:\s+([\d.]+)(ms|µs|us|s)", text)
    if not m:
        return float("nan")
    val, unit = float(m.group(1)), m.group(2)
    if unit in ("µs", "us"):
        return val / 1e3
    if unit == "s":
        return val * 1e3
    return val


def _start_sender(
    sender_config: str, frame_duration: int = 1000
) -> "subprocess.Popen[bytes]":
    sender_bin = AGORA_DIR / "build" / "sender"
    cmd = [
        str(sender_bin),
        "--num_threads=2",
        "--core_offset=55",
        f"--frame_duration={frame_duration}",
        "--enable_slow_start=0",
        "--inter_frame_delay=0",
        f"--conf_file={sender_config}",
    ]
    return subprocess.Popen(cmd, cwd=str(AGORA_DIR), env=os.environ.copy())


def run_one(
    *,
    slots: int,
    workers: int,
    system_threads: int,
    receiver_threads: int,
    warmup: int,
    max_runtime: int,
    sender_config: str,
    frame_duration: int,
    graph_json: Path,
    results_dir: Path,
    dylib: str,
    binary: str,
    sender_delay: int = 5,
) -> float:
    timing_file = results_dir / f"tomii_mimo_s{slots}_w{workers}.txt"
    print(f"\n=== Tomii MIMO | slots={slots}  workers={workers} ===", flush=True)

    # max_streams is set large so slots always restart; max_runtime is the real
    # exit trigger (sender sends 500 frames and stops after ~0.5 s).
    cmd = build_command(
        binary,
        str(graph_json),
        dylib,
        workers=workers,
        core_offset=1,
        system_threads=system_threads,
        receiver_threads=receiver_threads,
        slots=slots,
        max_streams=5000,
        exclude_streams=warmup,
        max_runtime=max_runtime,
        timing=str(timing_file),
        use_rdtsc=True,
        custom=True,
        coalesce_barriers=True,
        inline_continuation=True,
        slot_priority=True,
    )

    bench_env = {
        **os.environ,
        "MKL_NUM_THREADS": os.environ.get("MKL_NUM_THREADS", "1"),
        "OMP_NUM_THREADS": os.environ.get("OMP_NUM_THREADS", "1"),
        "OPENBLAS_NUM_THREADS": os.environ.get("OPENBLAS_NUM_THREADS", "1"),
        "GOTO_NUM_THREADS": os.environ.get("GOTO_NUM_THREADS", "1"),
    }

    t0 = time.monotonic()
    tomii_proc = subprocess.Popen(cmd, env=bench_env)

    # Give Tomii time to bind sockets before the sender fires.
    time.sleep(sender_delay)
    sender_proc = _start_sender(sender_config, frame_duration=frame_duration)
    print("  sender started", flush=True)

    # Wait for Tomii — it exits via max_runtime after the sender finishes.
    watchdog = max_runtime + 15
    try:
        ret = tomii_proc.wait(timeout=watchdog)
    except subprocess.TimeoutExpired:
        tomii_proc.kill()
        if sender_proc.poll() is None:
            sender_proc.terminate()
            try:
                sender_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                sender_proc.kill()
        raise RuntimeError(f"Tomii hung (>{watchdog}s) slots={slots} workers={workers}")
    t1 = time.monotonic()

    # Clean up sender if it outlasted Tomii (shouldn't happen, but be safe).
    if sender_proc.poll() is None:
        sender_proc.terminate()
        try:
            sender_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            sender_proc.kill()

    if ret != 0:
        raise subprocess.CalledProcessError(ret, cmd)

    wall_ms = (t1 - t0) * 1000.0
    latency_ms = _parse_avg_ms(timing_file)
    print(
        f"  latency: {latency_ms:.4f} ms/slot  (wall: {wall_ms:.1f} ms)",
        flush=True,
    )
    return latency_ms


def main() -> None:
    p = argparse.ArgumentParser(
        description="Tomii MIMO 4-node benchmark sweep over slots and workers."
    )
    p.add_argument(
        "--slots",
        type=int,
        nargs="+",
        default=[1, 4, 16, 64],
        help="concurrent slot counts to sweep",
    )
    p.add_argument(
        "--workers",
        type=int,
        nargs="+",
        default=[1, 2, 4, 8],
        help="worker thread counts to sweep",
    )
    p.add_argument("--system-threads", type=int, default=2, help="resolution threads")
    p.add_argument(
        "--receiver-threads",
        type=int,
        default=4,
        help="dedicated network receiver threads",
    )
    p.add_argument(
        "--warmup",
        type=int,
        default=20,
        help="leading streams excluded from timing averages",
    )
    p.add_argument(
        "--max-runtime",
        type=int,
        default=30,
        dest="max_runtime",
        help="per-cell time limit in seconds (sender stops after ~0.5 s; "
        "this lets Tomii finish in-flight work then exit cleanly)",
    )
    p.add_argument(
        "--sender-config",
        default="files/config/ci/tddconfig-16x16.json",
        dest="sender_config",
        help="Agora sender --conf_file path (relative to ~/Agora)",
    )
    p.add_argument(
        "--frame-duration",
        type=int,
        default=50000,
        dest="frame_duration",
        help="sender --frame_duration in µs; floored per cell at ceil(48000/slots) "
        "to prevent sender from outrunning the receiver",
    )
    p.add_argument(
        "--graph",
        type=Path,
        default=None,
        help="graph JSON override (default: build from Python API via build_graph.py)",
    )
    p.add_argument(
        "--config",
        default=str(HERE / "graphs" / "tddconfig-16x16.json"),
        help="tddconfig JSON path forwarded to build_graph.py",
    )
    p.add_argument("--results-dir", type=Path, default=HERE / "results")
    p.add_argument("--csv-out", type=Path, default=None)
    p.add_argument("--no-clean", dest="clean", action="store_false", default=True)
    args = p.parse_args()

    args.results_dir.mkdir(parents=True, exist_ok=True)

    # Build graph JSON — from Python API unless an explicit override was given.
    if args.graph is not None:
        graph_json = args.graph
    else:
        kwargs = {}
        if args.config:
            kwargs["config_path"] = args.config
        graph = build_mimo_graph(**kwargs)
        _tmp = tempfile.NamedTemporaryFile(
            prefix="mimo_graph_", suffix=".json", delete=False, mode="w"
        )
        _tmp.write(graph.to_json())
        _tmp.close()
        graph_json = Path(_tmp.name)
        print(f"Graph written to: {graph_json}", flush=True)

    print("Building Tomii MIMO plugin...", flush=True)
    bench_build_env = {**os.environ, "FUNC_PATH": str(HERE / "src" / "lib.rs")}
    bench_manifest = str(BENCH_ROOT / "Cargo.toml")
    if args.clean:
        subprocess.run(
            ["cargo", "clean", "--manifest-path", str(HERE / "Cargo.toml")],
            check=True,
        )

    # Build plugin dylib
    subprocess.run(
        ["cargo", "build", "--manifest-path", str(HERE / "Cargo.toml"), "--release"],
        check=True,
        env=bench_build_env,
    )
    # Build main binary from the bench workspace (needed to register MIMO functions)
    subprocess.run(
        [
            "cargo",
            "build",
            "--manifest-path",
            bench_manifest,
            "-p",
            "tomii-core",
            "--bin",
            "main",
            "--release",
        ],
        check=True,
        env=bench_build_env,
    )

    dylib = str(HERE / "target" / "release" / "libmimo_bench_tomii.so")
    binary = str(DEVELOP_ROOT / "target" / "release" / "main")
    print(f"  dylib: {dylib}", flush=True)
    print(f"  binary: {binary}", flush=True)

    csv_path = args.csv_out or (args.results_dir / "mimo_sweep.csv")
    with open(csv_path, "w") as f:
        f.write("system,slots,workers,streams,ms_per_slot\n")

    for w in args.workers:
        for s in args.slots:
            # Floor so sender never fires faster than receiver throughput (~48 ms/slot).
            cell_frame_dur = max(args.frame_duration, -(-48_000 // s))
            # max_runtime must exceed sender_delay + full send window (500 frames).
            sender_runtime_s = (500 * cell_frame_dur) // 1_000_000
            cell_max_runtime = max(args.max_runtime, 5 + sender_runtime_s + 15)
            ms = run_one(
                slots=s,
                workers=w,
                system_threads=args.system_threads,
                receiver_threads=args.receiver_threads,
                warmup=args.warmup,
                max_runtime=cell_max_runtime,
                sender_config=args.sender_config,
                frame_duration=cell_frame_dur,
                graph_json=graph_json,
                results_dir=args.results_dir,
                dylib=dylib,
                binary=binary,
            )
            with open(csv_path, "a") as f:
                f.write(f"tomii,{s},{w},200,{ms:.6f}\n")

    print(f"\nResults written to: {csv_path}", flush=True)


if __name__ == "__main__":
    main()
