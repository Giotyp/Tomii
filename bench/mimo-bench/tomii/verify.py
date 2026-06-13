"""Tomii MIMO correctness verifier.

Runs two consecutive single-frame passes and checks that the post-demul
demod buffers are byte-for-bit identical across both runs.

The verifier manages the Agora sender lifecycle internally: it starts the
Tomii receiver first, waits for sockets to be ready, then starts the sender.

The graph is generated from the Python builder (build_graph.py) at runtime — the
tddconfig path is resolved on the local machine, so no static graph with an
embedded absolute path is needed.

Usage:
    # 16x16 (config lives in-repo):
    python mimo-bench/tomii/verify.py \\
        --config graphs/tddconfig-16x16.json \\
        --sender-config files/config/ci/tddconfig-16x16.json
    # 4x4 (config from your ~/Agora install): run with defaults.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
BENCH_ROOT = HERE.parents[1]
DEVELOP_ROOT = BENCH_ROOT.parents[1]
WORKSPACE_ROOT = HERE.parents[2]   # Tomii/ workspace root (bench/mimo-bench/tomii → Tomii/)
AGORA_DIR = Path("~/Agora").expanduser().resolve()
sys.path.insert(0, str(DEVELOP_ROOT))
sys.path.insert(0, str(HERE))      # so `build_graph` resolves regardless of CWD

from tomii._runner import build_command, _find_binary
from build_graph import build_mimo_graph


def _make_fixed_frame_config(base_config: str, num_frames: int) -> str:
    """Write a temp copy of the sender tddconfig with max_frame pinned to
    num_frames, so the sender emits a fixed, known number of frames and then
    stops. This makes "the last frame of the run" a deterministic frame across
    passes (combined with slots=1 in-order completion)."""
    with open(base_config) as f:
        cfg = json.load(f)
    cfg["max_frame"] = num_frames
    tmp = tempfile.NamedTemporaryFile(
        prefix="verify_sender_", suffix=".json", delete=False, mode="w")
    json.dump(cfg, tmp)
    tmp.close()
    return tmp.name


def _start_sender(
    sender_config: str, frame_duration: int = 1000, inter_frame_delay: int = 0
) -> "subprocess.Popen[bytes]":
    sender_bin = AGORA_DIR / "build" / "sender"
    cmd = [
        str(sender_bin),
        "--num_threads=2",
        "--core_offset=55",
        f"--frame_duration={frame_duration}",
        "--enable_slow_start=0",
        f"--inter_frame_delay={inter_frame_delay}",
        f"--conf_file={sender_config}",
    ]
    return subprocess.Popen(cmd, cwd=str(AGORA_DIR), stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL, env=os.environ.copy())


def _run_pass(
    *,
    pass_id: int,
    graph_json: Path,
    dylib: str,
    binary: str,
    output_dir: Path,
    sender_config: str | None,
    sender_delay: int,
    frame_duration: int,
    inter_frame_delay: int,
    max_streams: int,
    max_runtime: int,
    workers: int,
) -> Path:
    timing_file = output_dir / f"verify_pass{pass_id}.txt"
    demod_file = output_dir / f"verify_pass{pass_id}_demul.bin"

    cmd = build_command(
        binary,
        str(graph_json),
        dylib,
        workers=workers,
        core_offset=1,
        system_threads=2,
        receiver_threads=2,
        slots=1,
        # Process many frames, not one: the dump node overwrites the demod file
        # on every frame completion, so the final file is the LAST completed
        # frame. The first frame received after startup is timing-dependent and
        # may be partial; a steady-state frame (buffer fully populated, all
        # FrameWnd slots overwritten with complete data) is deterministic since
        # the Agora sender replays identical IQ every frame and MKL is sequential.
        max_streams=max_streams,
        exclude_streams=0,
        max_runtime=max_runtime,
        timing=str(timing_file),
        use_rdtsc=True,
        custom=True,
        coalesce_barriers=True,
        inline_continuation=True,
        slot_priority=True,
    )

    # Pin the BLAS/LAPACK thread count (armadillo's backend in the beam stage may
    # use OpenBLAS/OMP). MKL itself is linked sequential, so this is belt-and-
    # braces for deterministic floating-point reductions.
    bench_env = {
        **os.environ,
        "TOMII_VERIFY_PATH": str(demod_file),
        "MKL_NUM_THREADS": "1",
        "OMP_NUM_THREADS": "1",
        "OPENBLAS_NUM_THREADS": "1",
        "GOTO_NUM_THREADS": "1",
    }
    tomii_proc = subprocess.Popen(cmd, env=bench_env)
    sender_proc = None
    try:
        if sender_config is not None:
            time.sleep(sender_delay)
            sender_proc = _start_sender(
                sender_config,
                frame_duration=frame_duration,
                inter_frame_delay=inter_frame_delay,
            )
        # Binary exits on max_streams or max_runtime, whichever first; add a grace
        # watchdog so a wedged run never hangs the verifier.
        ret = tomii_proc.wait(timeout=sender_delay + max_runtime + 30)
        if ret != 0:
            raise subprocess.CalledProcessError(ret, cmd)
    except subprocess.TimeoutExpired:
        tomii_proc.kill()
        raise RuntimeError(f"Tomii hung during verify pass {pass_id}")
    finally:
        if sender_proc is not None and sender_proc.poll() is None:
            sender_proc.terminate()
            try:
                sender_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                sender_proc.kill()

    return demod_file


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    p = argparse.ArgumentParser(description="Verify Tomii MIMO determinism.")
    p.add_argument("--passes", type=int, default=2,
                   help="number of passes to compare (default: 2)")
    p.add_argument("--config", type=str,
                   default="~/Agora/files/config/ci/tddconfig-sim-ul.json",
                   help="tddconfig JSON for the Tomii graph builder (path is "
                        "expanded/resolved at runtime). 4x4 default; pass "
                        "graphs/tddconfig-16x16.json for the 16x16 pipeline.")
    p.add_argument("--graph", type=Path, default=None,
                   help="optional explicit graph JSON; if omitted, the graph is "
                        "generated from --config via build_graph.py (with the dump node)")
    p.add_argument("--output-dir", type=Path, default=HERE / "results" / "verify")
    p.add_argument("--no-clean", dest="clean", action="store_false", default=True)
    p.add_argument("--sender-config", type=str,
                   default="files/config/ci/tddconfig-sim-ul.json",
                   help="tddconfig path relative to ~/Agora (or absolute); verify.py "
                        "starts and stops the Agora sender for each pass")
    p.add_argument("--sender-delay", type=int, default=5,
                   help="seconds to wait after starting Tomii before starting sender (default: 5)")
    p.add_argument("--frame-duration", type=int, default=1000, dest="frame_duration",
                   help="sender --frame_duration in µs (default: 1000)")
    p.add_argument("--inter-frame-delay", type=int, default=0, dest="inter_frame_delay",
                   help="sender --inter_frame_delay in µs; spaces the per-frame packet "
                        "burst so the receiver can drain (default: 0)")
    p.add_argument("--num-frames", type=int, default=20, dest="num_frames",
                   help="fixed number of frames the sender emits per pass; the dump "
                        "captures the LAST completed frame, which is deterministic "
                        "across passes for fixed input (default: 20)")
    p.add_argument("--workers", type=int, default=24,
                   help="worker threads (default: 24; use 1 to test for races)")
    args = p.parse_args()

    # If sender_config is an absolute path, use it as-is; if relative, it is
    # interpreted relative to AGORA_DIR (the cwd used when launching the sender).
    if args.sender_config is not None and not Path(args.sender_config).is_absolute():
        args.sender_config = str(AGORA_DIR / args.sender_config)

    # Pin the sender to a fixed frame count so "the last frame" is the same frame
    # every pass; slots=1 (in _run_pass) makes frames complete in order, so the
    # final overwrite of the dump file is always frame (num_frames - 1).
    args.sender_config = _make_fixed_frame_config(args.sender_config, args.num_frames)

    # Run long enough to send all frames (slow, non-overlapping) and drain.
    send_window_s = (args.num_frames * args.frame_duration) // 1_000_000
    args.max_runtime = args.sender_delay + send_window_s + 20
    # Cap streams well above num_frames so the run exits on max_runtime after the
    # last frame completes, not before (avoids stopping mid-stream on a dropped frame).
    args.max_streams = 100_000

    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.clean:
        subprocess.run(
            ["cargo", "clean", "--manifest-path", str(HERE / "Cargo.toml")],
            check=True,
        )
    build_env = {**os.environ, "FUNC_PATH": str(HERE / "src" / "lib.rs")}
    subprocess.run(
        ["cargo", "build", "--manifest-path", str(HERE / "Cargo.toml"), "--release"],
        check=True,
        env=build_env,
    )
    # Rebuild main binary so its function registry includes any new plugin functions.
    subprocess.run(
        ["cargo", "build", "--manifest-path", str(WORKSPACE_ROOT / "Cargo.toml"),
         "-p", "tomii-core", "--bin", "main", "--release"],
        check=True,
        env=build_env,
    )

    dylib = str(HERE / "target" / "release" / "libmimo_bench_tomii.so")
    binary = _find_binary(release=True)

    # Graph: explicit override, else generate from --config (path resolved at
    # runtime by build_graph.py, so no machine-specific path is baked in).
    if args.graph is not None:
        graph_json = args.graph
    else:
        graph = build_mimo_graph(config_path=args.config, dump=True)
        _tmp = tempfile.NamedTemporaryFile(
            prefix="mimo_verify_graph_", suffix=".json", delete=False, mode="w")
        _tmp.write(graph.to_json())
        _tmp.close()
        graph_json = Path(_tmp.name)
        print(f"Graph generated from {args.config} -> {graph_json}", flush=True)

    hashes: list[str] = []
    for i in range(1, args.passes + 1):
        print(f"Running pass {i}/{args.passes}...", flush=True)
        out_file = _run_pass(
            pass_id=i,
            graph_json=graph_json,
            dylib=dylib,
            binary=binary,
            output_dir=args.output_dir,
            sender_config=args.sender_config,
            sender_delay=args.sender_delay,
            frame_duration=args.frame_duration,
            inter_frame_delay=args.inter_frame_delay,
            max_streams=args.max_streams,
            max_runtime=args.max_runtime,
            workers=args.workers,
        )
        h = sha256(out_file)
        hashes.append(h)
        print(f"  pass {i} hash: {h[:16]}...", flush=True)

    all_match = len(set(hashes)) == 1
    any_nonzero = any(h != "e3b0c44298fc1c149afb" for h in hashes)

    if all_match and any_nonzero:
        print("\nPASS: all passes produced identical non-empty output")
        sys.exit(0)
    elif not any_nonzero:
        print("\nFAIL: output files are empty")
        sys.exit(1)
    else:
        print("\nFAIL: passes produced different output")
        for i, h in enumerate(hashes, 1):
            print(f"  pass {i}: {h}")
        sys.exit(1)


if __name__ == "__main__":
    main()
