#!/bin/bash
# Timely Dataflow wavefront benchmark runner.
# Mirrors the same N / workers sweep as TBB and Taskflow benchmarks.
# Workers are pinned starting from core 1 (--core-offset 1), matching SynStream
# and TBB's base_core=1, keeping all workers on NUMA node 0 for W<=31.
#
# Usage:
#   cd timely-bench
#   bash run_wavefront.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${RESULTS_DIR:-$SCRIPT_DIR/../benchmarks/results/csvs}"
WORKERS_LIST="${WORKERS_LIST:-1 2 4 8 16 32}"
N_LIST="${N_LIST:-64 128 256 512}"
ITERS="${ITERS:-20}"
WARMUP="${WARMUP:-3}"

mkdir -p "$RESULTS_DIR"

BIN="$SCRIPT_DIR/target/release/wavefront"
if [ ! -x "$BIN" ]; then
    echo "Building wavefront benchmark..."
    cargo build --release --manifest-path "$SCRIPT_DIR/Cargo.toml"
fi

echo "=========================================="
echo "  Timely Dataflow Wavefront Benchmark"
echo "  Results dir: $RESULTS_DIR"
echo "  Workers: $WORKERS_LIST"
echo "  Grid sizes: $N_LIST"
echo "  Core offset: 1 (pinned, NUMA node 0)"
echo "=========================================="

for N in $N_LIST; do
    for W in $WORKERS_LIST; do
        OUT="$RESULTS_DIR/timely_wavefront_n${N}_w${W}.csv"
        echo "  Timely (pinned) N=$N workers=$W -> $OUT"
        "$BIN" \
            --n "$N" \
            --workers "$W" \
            --iterations "$ITERS" \
            --warmup "$WARMUP" \
            --core-offset 1 \
            --output "$OUT"
    done
done

echo ""
echo "All Timely wavefront benchmarks complete. Results in $RESULTS_DIR"
