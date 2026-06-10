"""Taskflow MIMO benchmark driver.

Builds the CMake project, then sweeps slots x workers, writing results to
build/tf_mimo_sweep.csv in the same format as the Tomii mimo_sweep.csv so
the two can be compared directly.

Requires:
  - Intel MKL at /opt/intel/oneapi/mkl/2024.0
  - lib/libbeamfuncs.so, lib/libdemod.so, lib/libfftfuncs.so in ../tomii/lib/
  - Taskflow headers in anti-diag-bench/taskflow/src/ (or TASKFLOW_ROOT set)
  - Agora built at ~/Agora (https://github.com/Agora-wireless/Agora)
    The script starts ~/Agora/build/sender automatically for each sweep cell.

Usage (from bench worktree root):
    python mimo-bench/taskflow/run_bench.py
    python mimo-bench/taskflow/run_bench.py --slots 1 4 16 --workers 2 4 8
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent   # mimo-bench/taskflow/
BUILD_DIR = HERE / "build"
DEFAULT_CONFIG = HERE.parent / "tomii" / "graphs" / "tddconfig-4x4.json"
AGORA_DIR = Path("~/Agora").expanduser().resolve()


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def build_cmake(clean: bool) -> None:
    BUILD_DIR.mkdir(parents=True, exist_ok=True)

    if clean and (BUILD_DIR / "CMakeCache.txt").exists():
        print("Cleaning build directory...", flush=True)
        subprocess.run(
            ["cmake", "--build", str(BUILD_DIR), "--target", "clean"],
            check=False,
        )

    print("Configuring CMake...", flush=True)
    subprocess.run(
        ["cmake", "-S", str(HERE), "-B", str(BUILD_DIR),
         "-DCMAKE_BUILD_TYPE=Release"],
        check=True,
    )

    print("Building tf_mimo...", flush=True)
    subprocess.run(
        ["cmake", "--build", str(BUILD_DIR), "--", "-j4"],
        check=True,
    )

    binary = BUILD_DIR / "tf_mimo"
    if not binary.exists():
        print(f"ERROR: binary not found at {binary}", file=sys.stderr)
        sys.exit(1)
    print(f"  binary: {binary}", flush=True)


def _start_sender(sender_config: str, frame_duration: int = 1000) -> "subprocess.Popen[bytes]":
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


# ---------------------------------------------------------------------------
# Single (slots, workers) run
# ---------------------------------------------------------------------------

def run_one(
    *,
    slots: int,
    workers: int,
    streams: int,
    warmup: int,
    config: Path,
    sender_config: str,
    frame_duration: int,
    output_csv: Path,
    sender_delay: int = 5,
) -> None:
    binary = BUILD_DIR / "tf_mimo"
    cmd = [
        str(binary),
        "--slots",   str(slots),
        "--workers", str(workers),
        "--streams", str(streams),
        "--warmup",  str(warmup),
        "--config",  str(config),
        "--output",  str(output_csv),
    ]
    print(
        f"\n=== Taskflow MIMO | slots={slots}  workers={workers} ===",
        flush=True,
    )

    bench_env = {
        **os.environ,
        "MKL_NUM_THREADS": "1",
        "OMP_NUM_THREADS": "1",
        "OPENBLAS_NUM_THREADS": "1",
        "GOTO_NUM_THREADS": "1",
    }

    tf_proc = subprocess.Popen(cmd, env=bench_env)

    time.sleep(sender_delay)
    sender_proc = _start_sender(sender_config, frame_duration=frame_duration)
    print("  sender started", flush=True)

    ret = tf_proc.wait()

    if sender_proc.poll() is None:
        sender_proc.terminate()
        try:
            sender_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            sender_proc.kill()

    if ret != 0:
        raise subprocess.CalledProcessError(ret, cmd)


# ---------------------------------------------------------------------------
# Main sweep
# ---------------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser(
        description="Taskflow MIMO benchmark sweep over slots and workers."
    )
    p.add_argument("--slots", type=int, nargs="+", default=[1, 4, 16, 64],
                   help="concurrent slot counts to sweep")
    p.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4, 8],
                   help="worker thread counts to sweep")
    p.add_argument("--streams", type=int, default=200,
                   help="total frames to time (excluding warmup)")
    p.add_argument("--warmup", type=int, default=20,
                   help="warmup frames excluded from timing")
    p.add_argument("--sender-config", default="files/config/ci/tddconfig-4x4.json",
                   dest="sender_config",
                   help="Agora sender --conf_file path (relative to ~/Agora)")
    p.add_argument("--frame-duration", type=int, default=1000, dest="frame_duration",
                   help="sender --frame_duration in µs (use ≥2000 for 16×16)")
    p.add_argument("--config", type=Path, default=DEFAULT_CONFIG,
                   help="tddconfig JSON path")
    p.add_argument("--csv-out", type=Path, default=None,
                   help="output CSV path (default: build/tf_mimo_sweep.csv)")
    p.add_argument("--no-clean", dest="clean", action="store_false",
                   default=True,
                   help="skip cmake clean before building")
    args = p.parse_args()

    build_cmake(args.clean)

    output_csv = args.csv_out or (BUILD_DIR / "tf_mimo_sweep.csv")

    if output_csv.exists():
        output_csv.unlink()

    for w in args.workers:
        for s in args.slots:
            run_one(
                slots=s,
                workers=w,
                streams=args.streams,
                warmup=args.warmup,
                config=args.config,
                sender_config=args.sender_config,
                frame_duration=args.frame_duration,
                output_csv=output_csv,
            )

    print(f"\nResults written to: {output_csv}", flush=True)


if __name__ == "__main__":
    main()
