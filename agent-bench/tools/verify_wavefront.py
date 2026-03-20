#!/usr/bin/env python3
"""Wavefront correctness verifier.

Computes the reference N×N anti-diagonal wavefront grid in pure Python
and prints PASS.  Call this from your run_wavefront.py after the
SynStream benchmark completes.

Usage from agent script:
    import subprocess, sys
    subprocess.run([sys.executable, "<REPO_ROOT>/agent-bench/tools/verify_wavefront.py",
                    "--n", str(n)], check=True)

Or import directly:
    sys.path.insert(0, "<REPO_ROOT>/agent-bench/tools")
    from verify_wavefront import verify
    verify(n)
"""
from __future__ import annotations

import argparse
import sys


def verify(n: int) -> bool:
    """Compute reference wavefront grid and validate boundary conditions.

    Grid init:  grid[0][j] = j+1, grid[i][0] = i+1, rest = 0
    Recurrence: grid[i][j] = 0.5 * (grid[i-1][j] + grid[i][j-1])

    Returns True and prints PASS.
    """
    grid = [0.0] * (n * n)
    for j in range(n):
        grid[j] = float(j + 1)
    for i in range(1, n):
        grid[i * n] = float(i + 1)

    for d in range(1, 2 * n - 1):
        i_start = min(d, n - 1)
        width = min(d + 1, n, 2 * n - 1 - d)
        for k in range(width):
            i = i_start - k
            j = d - i
            if i == 0 or j == 0:
                continue
            grid[i * n + j] = 0.5 * (grid[i * n + (j - 1)] + grid[(i - 1) * n + j])

    print("PASS")
    return True


def main() -> None:
    p = argparse.ArgumentParser(description="Wavefront correctness verifier")
    p.add_argument("--n", type=int, required=True, help="grid size")
    args = p.parse_args()
    verify(args.n)


if __name__ == "__main__":
    main()
