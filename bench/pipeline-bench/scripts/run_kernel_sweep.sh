#!/usr/bin/env bash
# run_kernel_sweep.sh — sweep TRANSFORM_ITERS ∈ {1, 512, 2048, 8192} on both
# Tomii and Taskflow, producing per-size CSVs and a combined CSV for each side.
#
# Usage (from the bench worktree root):
#   bash pipeline-bench/scripts/run_kernel_sweep.sh
#
# Each iteration edits TRANSFORM_ITERS in both source files, rebuilds both
# frameworks from scratch, runs the W×S sweep, and verifies correctness.
# After the loop, restores TRANSFORM_ITERS to 2048 (the canonical bench value).
#
# Output:
#   pipeline-bench/tomii/results/pipeline_sweep_kernels.csv
#   pipeline-bench/taskflow/build/tf_pipeline_sweep_kernels.csv

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(dirname "$SCRIPT_DIR")"   # pipeline-bench/
WORKTREE="$(dirname "$BENCH_DIR")"     # bench worktree root

TOMII_DIR="$BENCH_DIR/tomii"
TF_DIR="$BENCH_DIR/taskflow"
RESULTS_DIR="$TOMII_DIR/results"
TF_BUILD_DIR="$TF_DIR/build"

RUST_SRC="$TOMII_DIR/src/lib.rs"
CPP_SRC="$TF_DIR/src/main.cpp"

COMBINED_TOMII="$RESULTS_DIR/pipeline_sweep_kernels.csv"
COMBINED_TF="$TF_BUILD_DIR/tf_pipeline_sweep_kernels.csv"

CANONICAL_ITERS=2048

SWEEP_ITERS=(1 512 2048 8192)

# Verify that sed changes the expected lines.
_check_const() {
    local file="$1" expected="$2"
    if ! grep -qE "TRANSFORM_ITERS[^=]*=[^0-9]*${expected}[^0-9]" "$file"; then
        echo "ERROR: TRANSFORM_ITERS != $expected in $file" >&2
        exit 1
    fi
}

# Edit TRANSFORM_ITERS in both source files.
_set_iters() {
    local iters="$1"
    # Rust: const TRANSFORM_ITERS: usize = <N>;
    sed -i "s/const TRANSFORM_ITERS: usize = [0-9]*/const TRANSFORM_ITERS: usize = ${iters}/" "$RUST_SRC"
    # C++: static constexpr int TRANSFORM_ITERS = <N>;
    sed -i "s/static constexpr int TRANSFORM_ITERS = [0-9]*/static constexpr int TRANSFORM_ITERS = ${iters}/" "$CPP_SRC"
    _check_const "$RUST_SRC" "$iters"
    _check_const "$CPP_SRC"  "$iters"
    echo ">>> Set TRANSFORM_ITERS = $iters in both source files"
}

# Append data rows (skip header) from a per-size CSV to the combined CSV.
_append_rows() {
    local src="$1" dst="$2"
    if [[ -f "$src" ]]; then
        tail -n +2 "$src" >> "$dst"
    else
        echo "WARNING: $src not found, skipping append" >&2
    fi
}

mkdir -p "$RESULTS_DIR" "$TF_BUILD_DIR"

# Write combined CSV headers once.
echo "system,n,items_per_stream,slots,workers,streams,ms_per_stream,transform_iters" > "$COMBINED_TOMII"
echo "system,n,items_per_stream,slots,workers,streams,ms_per_stream,transform_iters" > "$COMBINED_TF"

for ITERS in "${SWEEP_ITERS[@]}"; do
    echo ""
    echo "========================================================"
    echo "  TRANSFORM_ITERS = $ITERS"
    echo "========================================================"

    _set_iters "$ITERS"

    TOMII_CSV="$RESULTS_DIR/pipeline_sweep_iters_${ITERS}.csv"
    TF_CSV="$TF_BUILD_DIR/tf_pipeline_sweep_iters_${ITERS}.csv"

    # --- Tomii ---
    python "$TOMII_DIR/run_bench.py" \
        --csv-out "$TOMII_CSV" \
        --transform-iters "$ITERS"

    # --- Taskflow ---
    python "$TF_DIR/run_bench.py" \
        --csv-out "$TF_CSV"

    # --- Verify (Tomii only; uses --no-build since dylib was just built) ---
    python "$TOMII_DIR/verify.py" \
        --no-build \
        --transform-iters "$ITERS" \
        --streams 5

    # --- Append to combined CSVs ---
    _append_rows "$TOMII_CSV" "$COMBINED_TOMII"
    _append_rows "$TF_CSV"    "$COMBINED_TF"

    echo "  Done: TRANSFORM_ITERS=$ITERS"
done

# Restore canonical value.
echo ""
echo "Restoring TRANSFORM_ITERS = $CANONICAL_ITERS"
_set_iters "$CANONICAL_ITERS"

echo ""
echo "Kernel sweep complete."
echo "  Tomii combined:    $COMBINED_TOMII"
echo "  Taskflow combined: $COMBINED_TF"
echo ""
echo "To regenerate the comparison plot:"
echo "  python $BENCH_DIR/pipeline-comparison.py \\"
echo "    --tomii-csv     $COMBINED_TOMII \\"
echo "    --taskflow-csv  $COMBINED_TF \\"
echo "    --kernel-sweep"
