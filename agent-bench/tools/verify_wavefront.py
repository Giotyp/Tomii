#!/usr/bin/env python3
"""Wavefront correctness verifier.

Computes the reference N×N anti-diagonal wavefront and checks the submitted
corner cell value grid[N-1][N-1] against it.  Exits 0 and prints PASS on
success; exits 1 and prints FAIL on mismatch.

Usage:
    python verify_wavefront.py --n N --corner VALUE

    --corner VALUE   the computed grid[N-1][N-1] from your implementation.
                     The verifier checks it against the reference.

From an agent script (SynStream example):
    corner = float(Path("wf_corner.txt").read_text())
    subprocess.run([sys.executable, "<REPO_ROOT>/agent-bench/tools/verify_wavefront.py",
                    "--n", str(n), "--corner", str(corner)], check=True)

From C++ (Taskflow example):
    printf("CORNER: %.15f\\n", grid[(N-1)*N + (N-1)]);
    # then call the verifier from a Python wrapper that parses stdout.
"""
from __future__ import annotations

import argparse
import sys


def reference_corner(n: int) -> float:
    """Return the expected value of grid[n-1][n-1] for an N×N wavefront."""
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
    return grid[(n - 1) * n + (n - 1)]


def verify(n: int, corner: float) -> bool:
    """Check corner against the reference.  Prints PASS or FAIL."""
    expected = reference_corner(n)
    rel_err = abs(corner - expected) / (abs(expected) + 1e-300)
    if rel_err > 1e-9:
        print(f"FAIL: grid[{n-1}][{n-1}] = {corner:.15g}, expected {expected:.15g} "
              f"(rel_err={rel_err:.2e})")
        return False
    print("PASS")
    return True


def main() -> None:
    p = argparse.ArgumentParser(description="Wavefront correctness verifier")
    p.add_argument("--n",      type=int,   required=True, help="grid size")
    p.add_argument("--corner", type=float, required=True,
                   help="computed grid[N-1][N-1] value from your implementation")
    args = p.parse_args()
    ok = verify(args.n, args.corner)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
