#!/bin/bash
# TBB flow_graph bilateral denoising benchmark runner.
# Matches the image_size / tile_size / thread sweep from scripts/run_all.sh.
# Worker count controlled via global_control (no core pinning).
#
# Usage:
#   cd bilateral-bench/tbb
#   bash run_bilateral.sh [--quick]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${DATA_DIR:-$SCRIPT_DIR/../data}"
RESULTS_DIR="${RESULTS_DIR:-$SCRIPT_DIR/results}"
IMAGE_SIZES="${IMAGE_SIZES:-1024 4096 8192}"
TILE_SIZES="${TILE_SIZES:-128 256 512}"
KERNEL_RADII="${KERNEL_RADII:-2 4 7}"
THREAD_COUNTS="${THREAD_COUNTS:-1 2 4 8 16}"
ITERS="${ITERS:-10}"
WARMUP="${WARMUP:-2}"

if [[ "${1:-}" == "--quick" ]]; then
    IMAGE_SIZES="1024"
    TILE_SIZES="256"
    KERNEL_RADII="4"
    THREAD_COUNTS="1 4"
    ITERS=3
    WARMUP=1
fi

mkdir -p "$RESULTS_DIR"

BIN="$SCRIPT_DIR/bilateral_flow"
if [ ! -x "$BIN" ]; then
    echo "Building TBB flow_graph bilateral benchmark..."
    make -C "$SCRIPT_DIR"
fi

OUT="$RESULTS_DIR/tbb_flow_bilateral_all.csv"
rm -f "$OUT"

echo "=========================================="
echo "  TBB flow_graph Bilateral Denoising"
echo "  Results: $OUT"
echo "  Image sizes: $IMAGE_SIZES"
echo "  Tile sizes:  $TILE_SIZES"
echo "  Threads:     $THREAD_COUNTS"
echo "=========================================="

for img in $IMAGE_SIZES; do
    for tile in $TILE_SIZES; do
        [[ $((img % tile)) -ne 0 ]] && continue
        for kr in $KERNEL_RADII; do
            for t in $THREAD_COUNTS; do
                echo "  tbb_flow img=${img} tile=${tile} kr=${kr} threads=${t} -> $OUT"
                "$BIN" \
                    --image-size "$img" \
                    --tile-size  "$tile" \
                    --kernel-radius "$kr" \
                    --threads    "$t" \
                    --iterations "$ITERS" \
                    --warmup     "$WARMUP" \
                    --data-dir   "$DATA_DIR" \
                    --output     "$OUT"
            done
        done
    done
done

echo ""
echo "All TBB flow_graph bilateral benchmarks complete. Results in $OUT"
