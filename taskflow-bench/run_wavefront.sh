#!/bin/bash
# Taskflow wavefront benchmark runner.
# Matches the same N / workers sweep as TBB and Timely benchmarks.
#
# Usage:
#   cd taskflow-bench
#   bash run_wavefront.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${RESULTS_DIR:-$SCRIPT_DIR/../benchmarks/results/csvs}"
WORKERS_LIST="${WORKERS_LIST:-1 2 4 8 16 32}"
N_LIST="${N_LIST:-64 128 256 512}"
ITERS="${ITERS:-20}"
WARMUP="${WARMUP:-3}"
TILE_SIZE="${TILE_SIZE:-32}"

mkdir -p "$RESULTS_DIR"

BIN_ANTIDIAG="$SCRIPT_DIR/wavefront"
BIN_BLOCK="$SCRIPT_DIR/wavefront_block"
if [ ! -x "$BIN_ANTIDIAG" ] || [ ! -x "$BIN_BLOCK" ]; then
    echo "Building Taskflow wavefront benchmarks..."
    make -C "$SCRIPT_DIR"
fi

echo "=========================================="
echo "  Taskflow Anti-diagonal Wavefront"
echo "  Results dir: $RESULTS_DIR"
echo "  Workers: $WORKERS_LIST"
echo "  Grid sizes: $N_LIST"
echo "=========================================="

for N in $N_LIST; do
    for W in $WORKERS_LIST; do
        OUT="$RESULTS_DIR/taskflow_wavefront_n${N}_w${W}.csv"
        echo "  Taskflow anti-diag (pinned) N=$N workers=$W -> $OUT"
        "$BIN_ANTIDIAG" \
            --n "$N" \
            --workers "$W" \
            --iterations "$ITERS" \
            --warmup "$WARMUP" \
            --pin \
            --output "$OUT"
    done
done

echo ""
echo "=========================================="
echo "  Taskflow Block-DAG Wavefront"
echo "  Tile size: $TILE_SIZE"
echo "=========================================="

for N in $N_LIST; do
    for W in $WORKERS_LIST; do
        OUT="$RESULTS_DIR/taskflow_block_wavefront_n${N}_w${W}.csv"
        echo "  Taskflow block-DAG (pinned) N=$N workers=$W tile=$TILE_SIZE -> $OUT"
        "$BIN_BLOCK" \
            --n "$N" \
            --tile-size "$TILE_SIZE" \
            --workers "$W" \
            --iterations "$ITERS" \
            --warmup "$WARMUP" \
            --pin \
            --output "$OUT"
    done
done

echo ""
echo "All Taskflow wavefront benchmarks complete. Results in $RESULTS_DIR"
