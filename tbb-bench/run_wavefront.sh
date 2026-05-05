#!/bin/bash
# Intel TBB wavefront benchmark runner.
# Mirrors the same N / workers sweep as Timely and Taskflow benchmarks.
# Workers are pinned starting from core 1 (base_core=1 hardcoded in wavefront.cpp),
# keeping all workers on NUMA node 0 for W<=31.
#
# Usage:
#   cd tbb-bench
#   bash run_wavefront.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${RESULTS_DIR:-$SCRIPT_DIR/../benchmarks/results/csvs}"
WORKERS_LIST="${WORKERS_LIST:-1 2 4 8 16 32}"
N_LIST="${N_LIST:-64 128 256 512}"
ITERS="${ITERS:-20}"
WARMUP="${WARMUP:-3}"

mkdir -p "$RESULTS_DIR"

BIN="$SCRIPT_DIR/wavefront"
BIN_FLOW="$SCRIPT_DIR/wavefront_flow"
TILE_SIZE="${TILE_SIZE:-32}"

if [ ! -x "$BIN" ] || [ ! -x "$BIN_FLOW" ]; then
    echo "Building wavefront benchmarks..."
    make -C "$SCRIPT_DIR"
fi

echo "=========================================="
echo "  Intel TBB Wavefront Benchmark"
echo "  Results dir: $RESULTS_DIR"
echo "  Workers: $WORKERS_LIST"
echo "  Grid sizes: $N_LIST"
echo "  Core offset: 1 (pinned, NUMA node 0)"
echo "=========================================="

for N in $N_LIST; do
    for W in $WORKERS_LIST; do
        OUT="$RESULTS_DIR/tbb_wavefront_n${N}_w${W}.csv"
        echo "  TBB parallel_for (pinned) N=$N workers=$W -> $OUT"
        "$BIN" \
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
echo "  Intel TBB flow_graph Block-DAG Benchmark"
echo "  Tile size: $TILE_SIZE"
echo "=========================================="

for N in $N_LIST; do
    for W in $WORKERS_LIST; do
        OUT="$RESULTS_DIR/tbb_flow_wavefront_n${N}_w${W}.csv"
        echo "  TBB flow_graph N=$N workers=$W tile=$TILE_SIZE -> $OUT"
        "$BIN_FLOW" \
            --n "$N" \
            --tile-size "$TILE_SIZE" \
            --workers "$W" \
            --iterations "$ITERS" \
            --warmup "$WARMUP" \
            --output "$OUT"
    done
done

echo ""
echo "All TBB wavefront benchmarks complete. Results in $RESULTS_DIR"
