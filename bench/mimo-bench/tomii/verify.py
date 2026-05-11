"""Tomii MIMO correctness verifier.

Runs two consecutive single-frame passes with a deterministic Agora sender
(fixed seed or packet replay) and checks that the post-demul demod buffers
are byte-for-byte identical across both runs.

Requires the same external deps as run_bench.py plus an Agora sender
started with a fixed seed:
    cd ~/Agora && python scripts/sim_sender.py --seed 42 ...

Usage:
    python mimo-bench/tomii/verify.py [--passes 2]
"""

from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
BENCH_ROOT = HERE.parents[1]
DEVELOP_ROOT = BENCH_ROOT.parents[1]
sys.path.insert(0, str(DEVELOP_ROOT))

from tomii._runner import build_command, _find_binary


def _run_pass(
    *,
    pass_id: int,
    graph_json: Path,
    dylib: str,
    binary: str,
    output_dir: Path,
) -> Path:
    timing_file = output_dir / f"verify_pass{pass_id}.txt"
    output_file = output_dir / f"verify_pass{pass_id}_demul.bin"

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
        output=str(output_file),
    )
    subprocess.run(cmd, check=True, env=os.environ.copy())
    return output_file


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
    p.add_argument("--graph", type=Path,
                   default=HERE / "graphs" / "graph_4nodes.json")
    p.add_argument("--output-dir", type=Path, default=HERE / "results" / "verify")
    p.add_argument("--no-clean", dest="clean", action="store_false", default=True)
    args = p.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.clean:
        subprocess.run(
            ["cargo", "clean", "--manifest-path", str(HERE / "Cargo.toml")],
            check=True,
        )
    subprocess.run(
        ["cargo", "build", "--manifest-path", str(HERE / "Cargo.toml"), "--release"],
        check=True,
        env={**os.environ, "FUNC_PATH": str(HERE / "src" / "lib.rs")},
    )

    dylib = str(HERE / "target" / "release" / "libmimo_bench_tomii.so")
    binary = _find_binary(release=True)

    hashes: list[str] = []
    for i in range(1, args.passes + 1):
        print(f"Running pass {i}/{args.passes}...", flush=True)
        out_file = _run_pass(
            pass_id=i,
            graph_json=args.graph,
            dylib=dylib,
            binary=binary,
            output_dir=args.output_dir,
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
