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
) -> Path:
    timing_file = output_dir / f"verify_pass{pass_id}.txt"
    demod_file = output_dir / f"verify_pass{pass_id}_demul.bin"

    cmd = build_command(
        binary,
        str(graph_json),
        dylib,
        workers=4,
        core_offset=1,
        system_threads=2,
        receiver_threads=2,
        slots=1,
        max_streams=1,           # single frame
        exclude_streams=0,
        timing=str(timing_file),
        use_rdtsc=True,
        custom=True,
        coalesce_barriers=True,
        inline_continuation=True,
        slot_priority=True,
    )

    bench_env = {**os.environ, "TOMII_VERIFY_PATH": str(demod_file)}
    tomii_proc = subprocess.Popen(cmd, env=bench_env)
    sender_proc = None
    try:
        if sender_config is not None:
            time.sleep(sender_delay)
            sender_proc = _start_sender(sender_config, frame_duration=frame_duration)
        ret = tomii_proc.wait()
        if ret != 0:
            raise subprocess.CalledProcessError(ret, cmd)
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
    args = p.parse_args()

    # If sender_config is an absolute path, use it as-is; if relative, it is
    # interpreted relative to AGORA_DIR (the cwd used when launching the sender).
    if args.sender_config is not None and not Path(args.sender_config).is_absolute():
        args.sender_config = str(AGORA_DIR / args.sender_config)

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
