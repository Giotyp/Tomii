"""MapReduce word-count example — Python orchestration using the Τομί API.

Demonstrates a canonical Map→Reduce pipeline over synthetic text shards:

    generate_shard (x num_shards)   — produce a random token stream per shard
         ↓  1:1 $res
    map_tokens     (x num_shards)   — count word frequencies in each shard
         ↓  variadic $res (all results)
    reduce_all     (singleton)     — sum tables, write word-count pairs to file

Node functions are implemented in src/wordcount.c (pure C, no external deps)
and compiled to libwordcount_c.so by the Makefile.

Usage (from repo root, with venv active):
    uv run python examples/mapreduce/run_bench.py
    uv run python examples/mapreduce/run_bench.py --num-shards 32 --workers 4
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

HERE = Path(__file__).resolve().parent  # examples/mapreduce/
REPO_ROOT = HERE.parents[1]  # workspace root
sys.path.insert(0, str(REPO_ROOT))

import tomii as tm

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MapReduce word-count Τομί example")
    p.add_argument("--num-shards",       type=int, default=16,
                   help="Number of parallel map tasks (default: 16)")
    p.add_argument("--tokens-per-shard", type=int, default=256,
                   help="Tokens generated per shard (default: 256)")
    p.add_argument("--workers",          type=int, default=2)
    p.add_argument("--max-streams",      type=int, default=1)
    p.add_argument("--no-clean", dest="clean", action="store_false", default=True)
    p.add_argument("--debug",            action="store_true", default=False)
    return p.parse_args()


# ---------------------------------------------------------------------------
# Graph definition
# ---------------------------------------------------------------------------


def build_graph(num_shards: int, tokens_per_shard: int) -> tm.Graph:
    app = tm.Graph()

    # --- Initialization objects ---
    num_shards_var = app.var("num_shards", num_shards)
    tokens_var = app.var("tokens_per_shard", tokens_per_shard)
    vocab_size_var = app.var("vocab_size", 8)  # must match VOCAB_SIZE in C

    vocab = app.var("vocab", func="make_vocabulary", args=[])
    result_file = app.var(
        "result_file",
        func="get_out_file",
        args=[tm.String("SCRIPT_DIR"), tm.String("result.txt")],
    )

    # --- Stage 1 (Map source): generate one text shard per instance ---
    # Each instance receives the same base_seed; a per-call atomic counter
    # inside generate_shard XORs it so every shard gets a unique seed.
    gen = app.node(
        "generate_shard",
        func="generate_shard",
        factor=num_shards_var,
        args=[vocab, tm.u64(0xDEADBEEFCAFEBABE), tokens_var],
    )

    # --- Stage 2 (Map): count word frequencies within each shard ---
    mapped = app.node(
        "map_tokens",
        func="map_tokens",
        factor=num_shards_var,
        args=[gen.out(), vocab_size_var],  # 1:1 $res from generate_shard
    )

    # --- Stage 3 (Reduce): sum all local tables and write the result ---
    app.node(
        "reduce_all",
        func="reduce_all",
        args=[
            result_file,
            vocab,
            mapped.out(0, num_shards_var),  # variadic $res: all num_shards results
        ],
    )

    return app


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _verify_result(result_path: Path, num_shards: int, tokens_per_shard: int) -> None:
    """Parse result.txt and assert total token count matches expectation."""
    expected_total = num_shards * tokens_per_shard
    total = 0
    with open(result_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) == 2:
                total += int(parts[1])
    if total == expected_total:
        print(
            f"==> Verified: total token count = {total} "
            f"(= {num_shards} shards × {tokens_per_shard} tokens/shard)"
        )
    else:
        print(f"==> MISMATCH: expected {expected_total}, got {total}")
        sys.exit(1)


def main() -> None:
    args = _parse_args()

    c_lib = HERE / "libwordcount_c.so"
    header = HERE / "include" / "wordcount.h"
    env = {"SCRIPT_DIR": str(HERE)}

    # ── Step 1: build the C shared library ──────────────────────────────────
    print("==> Building C library (make)...")
    result = subprocess.run(["make", "-C", str(HERE)], check=False)
    if result.returncode != 0:
        sys.exit(f"make failed (exit {result.returncode})")

    # ── Step 2: build tomii-core with C header wrappers ─────────────────────
    app = build_graph(args.num_shards, args.tokens_per_shard)
    app.build(
        func_path=str(header),
        release=True,
        clean=args.clean,
        env=env,
    )

    # ── Step 3: run ──────────────────────────────────────────────────────────
    result_file = HERE / "result.txt"
    result_file.unlink(missing_ok=True)

    app.run(
        dylib=str(c_lib),
        env=env,
        workers=args.workers,
        max_streams=args.max_streams,
        debug=args.debug,
    )

    # ── Step 4: verify result ────────────────────────────────────────────────
    if result_file.exists():
        _verify_result(result_file, args.num_shards, args.tokens_per_shard)
    else:
        print("==> result.txt not found — run may have failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
