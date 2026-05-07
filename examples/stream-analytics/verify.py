#!/usr/bin/env python3
"""Verifier for the stream-analytics example.

Checks that result.txt contains N repetitions of the 4-line golden output,
one block per measured stream.

Usage:
    python verify.py                        # check result.txt in this dir
    python verify.py --streams 5 --exclude 2   # expect exactly 3 blocks
    python verify.py --result path/to/result.txt
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent


def _load_golden(golden_path: Path) -> list[str]:
    return [ln.rstrip("\n") for ln in golden_path.read_text().splitlines() if ln.strip()]


def _load_result(result_path: Path) -> list[str]:
    return [ln.rstrip("\n") for ln in result_path.read_text().splitlines() if ln.strip()]


def verify(result_path: Path, golden_path: Path, expected_blocks: int | None) -> tuple[bool, str]:
    if not golden_path.exists():
        return False, f"golden file not found: {golden_path}"
    if not result_path.exists():
        return False, f"result file not found: {result_path}"

    golden = _load_golden(golden_path)
    result = _load_result(result_path)
    block_size = len(golden)

    if block_size == 0:
        return False, "golden file is empty"
    if len(result) % block_size != 0:
        return False, (
            f"result has {len(result)} lines, not a multiple of "
            f"golden block size {block_size}"
        )

    num_blocks = len(result) // block_size

    if expected_blocks is not None and num_blocks != expected_blocks:
        return False, (
            f"expected {expected_blocks} stream block(s) but result has {num_blocks}"
        )

    for i in range(num_blocks):
        block = result[i * block_size : (i + 1) * block_size]
        if block != golden:
            return False, (
                f"block {i} mismatch:\n"
                f"  expected: {golden}\n"
                f"  got:      {block}"
            )

    return True, f"PASS ({num_blocks} stream block(s))"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--result", default=str(HERE / "result.txt"))
    p.add_argument("--golden", default=str(HERE / "result.golden.txt"))
    p.add_argument("--streams", type=int, default=None, help="total streams run")
    p.add_argument("--exclude", type=int, default=0, help="warm-up streams excluded")
    args = p.parse_args()

    expected_blocks: int | None = None
    if args.streams is not None:
        expected_blocks = args.streams - args.exclude

    ok, msg = verify(Path(args.result), Path(args.golden), expected_blocks)
    print(msg)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
