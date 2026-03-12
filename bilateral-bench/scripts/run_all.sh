#!/usr/bin/env bash
# Run the full bilateral denoising benchmark sweep: Taskflow + SynStream.
#
# Usage:
#   cd bilateral-bench
#   bash scripts/run_all.sh [--quick]
#
# Options:
#   --quick   Use small image (1024) and fewer thread counts (fast CI check)
#
# Results are written to:
#   taskflow/results/tf_bilateral_all.csv
#   synstream/results/ss_bilateral_all.csv

set -euo pipefail
BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

IMAGE_SIZES=(1024 4096 8192)
TILE_SIZES=(128 256 512)
KERNEL_RADII=(2 4 7)
THREAD_COUNTS=(1 2 4 8 16)
ITERATIONS=10
WARMUP=2

if [[ "${1:-}" == "--quick" ]]; then
    IMAGE_SIZES=(1024)
    TILE_SIZES=(256)
    KERNEL_RADII=(4)
    THREAD_COUNTS=(1 4)
    ITERATIONS=3
    WARMUP=1
fi

# ---------------------------------------------------------------------------
# Step 1: generate input data
# ---------------------------------------------------------------------------
echo "=== Generating input data ==="
cd "$BENCH_DIR"
python data/generate_input.py --sizes "${IMAGE_SIZES[@]}"

# ---------------------------------------------------------------------------
# Step 2: build Taskflow binary
# ---------------------------------------------------------------------------
echo ""
echo "=== Building Taskflow binary ==="
cd "$BENCH_DIR/taskflow"
make -j"$(nproc)"

# ---------------------------------------------------------------------------
# Step 3: run Taskflow sweep
# ---------------------------------------------------------------------------
echo ""
echo "=== Running Taskflow sweep ==="
mkdir -p "$BENCH_DIR/taskflow/results"
TF_CSV="$BENCH_DIR/taskflow/results/tf_bilateral_all.csv"
rm -f "$TF_CSV"

for img in "${IMAGE_SIZES[@]}"; do
    for tile in "${TILE_SIZES[@]}"; do
        # Skip invalid combos (tile > image)
        if (( tile >= img )); then continue; fi
        for kr in "${KERNEL_RADII[@]}"; do
            for threads in "${THREAD_COUNTS[@]}"; do
                echo "  Taskflow img=$img tile=$tile kr=$kr threads=$threads"
                taskset -c "1-$((threads))" \
                    "$BENCH_DIR/taskflow/bilateral_wavefront" \
                    --image-size "$img" \
                    --tile-size  "$tile" \
                    --kernel-radius "$kr" \
                    --sigma-s 3.0 \
                    --sigma-r 0.1 \
                    --threads "$threads" \
                    --iterations "$ITERATIONS" \
                    --warmup "$WARMUP" \
                    --data-dir "$BENCH_DIR/data" \
                    --output "$TF_CSV" \
                    --pin
            done
        done
    done
done

# ---------------------------------------------------------------------------
# Step 4: run SynStream sweep
# ---------------------------------------------------------------------------
echo ""
echo "=== Running SynStream sweep ==="
cd "$BENCH_DIR/synstream"

# Build synstream env
REPO_ROOT="$(cd "$BENCH_DIR/../.." && pwd)"
if [[ -f "$REPO_ROOT/examples/mimolib/scripts/export.sh" ]]; then
    source "$REPO_ROOT/examples/mimolib/scripts/export.sh"
fi

python run_benchmark.py \
    --image-sizes "${IMAGE_SIZES[@]}" \
    --tile-size 256 \
    --kernel-radius 4 \
    --sigma-s 3.0 \
    --sigma-r 0.1 \
    --workers "${THREAD_COUNTS[@]}" \
    --system-threads 2 \
    --iterations "$ITERATIONS" \
    --warmup "$WARMUP" \
    --no-clean

# ---------------------------------------------------------------------------
# Step 5: plot
# ---------------------------------------------------------------------------
echo ""
echo "=== Plotting results ==="
cd "$BENCH_DIR"
python scripts/plot_results.py \
    --tf-csv  "$BENCH_DIR/taskflow/results/tf_bilateral_all.csv" \
    --ss-csv  "$BENCH_DIR/synstream/results/ss_bilateral_all.csv" \
    --out-dir "$BENCH_DIR/results"

echo ""
echo "Done. Plots in $BENCH_DIR/results/"
