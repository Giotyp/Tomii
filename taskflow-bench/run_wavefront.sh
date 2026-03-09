#!/bin/bash
# Taskflow wavefront benchmark runner.
# Matches the same N / workers sweep as TBB and Timely benchmarks.
#
# Usage:
#   cd taskflow-bench
#   bash run_wavefront.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${RESULTS_DIR:-$SCRIPT_DIR/../benchmarks/results}"
WORKERS_LIST="${WORKERS_LIST:-1 2 4 8 16 32}"
N_LIST="${N_LIST:-64 128 256 512}"
ITERS="${ITERS:-20}"
WARMUP="${WARMUP:-3}"

mkdir -p "$RESULTS_DIR"

BIN="$SCRIPT_DIR/wavefront"
if [ ! -x "$BIN" ]; then
    echo "Building wavefront benchmark..."
    make -C "$SCRIPT_DIR"
fi

echo "=========================================="
echo "  Taskflow Wavefront Benchmark"
echo "  Results dir: $RESULTS_DIR"
echo "  Workers: $WORKERS_LIST"
echo "  Grid sizes: $N_LIST"
echo "=========================================="

for N in $N_LIST; do
    for W in $WORKERS_LIST; do
        OUT="$RESULTS_DIR/taskflow_wavefront_n${N}_w${W}.csv"
        echo "  Taskflow (pinned) N=$N workers=$W"
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
echo "All Taskflow wavefront benchmarks complete. Results in $RESULTS_DIR"
