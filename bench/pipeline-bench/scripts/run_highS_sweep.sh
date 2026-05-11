#!/usr/bin/env bash
# run_highS_sweep.sh — sweep S ∈ {1, 4, 16, 64, 256, 1024, 4096} at fixed
# W=4, TRANSFORM_ITERS=8192, capturing both throughput and peak RSS for each
# (system, S) cell.
#
# Usage (from the bench worktree root):
#   bash pipeline-bench/scripts/run_highS_sweep.sh
#
# Building is done once per framework before the sweep loop; each cell runs
# with --no-clean so the binary is not rebuilt per cell.
#
# RSS is captured via /usr/bin/time -v for both frameworks, giving a single
# consistent measurement mechanism for direct comparison.
#
# Output:
#   pipeline-bench/tomii/results/pipeline_highS.csv
#   pipeline-bench/taskflow/build/tf_pipeline_highS.csv
# (columns: system,n,items_per_stream,slots,workers,streams,ms_per_stream,
#           transform_iters,peak_rss_kb)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(dirname "$SCRIPT_DIR")"   # pipeline-bench/
WORKTREE="$(dirname "$BENCH_DIR")"     # bench worktree root

TOMII_DIR="$BENCH_DIR/tomii"
TF_DIR="$BENCH_DIR/taskflow"
RESULTS_DIR="$TOMII_DIR/results"
TF_BUILD_DIR="$TF_DIR/build"
TMP_DIR="$BENCH_DIR/.highS_tmp"

RUST_SRC="$TOMII_DIR/src/lib.rs"
CPP_SRC="$TF_DIR/src/main.cpp"

COMBINED_TOMII="$RESULTS_DIR/pipeline_highS.csv"
COMBINED_TF="$TF_BUILD_DIR/tf_pipeline_highS.csv"

FIXED_W=4
FIXED_ITERS=8192
N=256

# S values to sweep.  At S=4096, T scales up to keep ≥4 full batches.
SWEEP_S=(1 4 16 64 256 1024 4096)

# Per-cell wall-time cap (seconds).  Kills the cell if it runs over.
CELL_TIMEOUT=600

# ---------------------------------------------------------------------------
# Helpers shared with run_kernel_sweep.sh
# ---------------------------------------------------------------------------

_check_const() {
    local file="$1" expected="$2"
    if ! grep -qE "TRANSFORM_ITERS[^=]*=[^0-9]*${expected}[^0-9]" "$file"; then
        echo "ERROR: TRANSFORM_ITERS != $expected in $file" >&2
        exit 1
    fi
}

_set_iters() {
    local iters="$1"
    sed -i "s/const TRANSFORM_ITERS: usize = [0-9]*/const TRANSFORM_ITERS: usize = ${iters}/" "$RUST_SRC"
    sed -i "s/static constexpr int TRANSFORM_ITERS = [0-9]*/static constexpr int TRANSFORM_ITERS = ${iters}/" "$CPP_SRC"
    _check_const "$RUST_SRC" "$iters"
    _check_const "$CPP_SRC"  "$iters"
}

_parse_rss() {
    # Extract VmHWM (peak RSS) in kB from /usr/bin/time -v output.
    local log="$1"
    grep "Maximum resident set size" "$log" 2>/dev/null | awk '{print $NF}'
}

# Append the single data row from a cell CSV to the combined CSV, adding the
# RSS column.  If the cell CSV is missing or empty (timed-out cell), write a
# NaN row instead so the combined CSV stays well-formed.
_append_with_rss() {
    local cell_csv="$1" combined="$2" rss_kb="$3"
    if [[ -f "$cell_csv" ]]; then
        # The cell CSV has a header line; we want only data rows.
        local dataline
        dataline=$(tail -n +2 "$cell_csv" | head -1)
        if [[ -n "$dataline" ]]; then
            echo "${dataline},${rss_kb:-0}" >> "$combined"
            return
        fi
    fi
    # Fallback: write a NaN row so the combined CSV is gap-free.
    echo "FAILED,$N,$N,$(basename "$cell_csv" .csv | awk -F_ '{print $NF}'),$FIXED_W,0,NaN,$FIXED_ITERS,${rss_kb:-0}" >> "$combined"
}

# ---------------------------------------------------------------------------
# Pre-flight
# ---------------------------------------------------------------------------

mkdir -p "$RESULTS_DIR" "$TF_BUILD_DIR" "$TMP_DIR"

# Ensure TRANSFORM_ITERS is set to the fixed value in both source files.
_set_iters "$FIXED_ITERS"
echo "Set TRANSFORM_ITERS = $FIXED_ITERS"

# ---------------------------------------------------------------------------
# Build both frameworks once
# ---------------------------------------------------------------------------

echo ""
echo "Building Tomii plugin (TRANSFORM_ITERS=$FIXED_ITERS)..."
# Pass --slots 1 --workers 4 --streams 1 --warmup 0 so the driver does the
# cargo build and tomii-macro codegen steps, then exits quickly.
python "$TOMII_DIR/run_bench.py" \
    --slots 1 --workers "$FIXED_W" --streams 1 --warmup 0 \
    --transform-iters "$FIXED_ITERS" \
    --csv-out "$TMP_DIR/build_probe_tomii.csv"
# Subsequent cells use --no-clean (binary already built above).

echo ""
echo "Building Taskflow (TRANSFORM_ITERS=$FIXED_ITERS)..."
python "$TF_DIR/run_bench.py" \
    --slots 1 --workers "$FIXED_W" --streams 1 --warmup 0 \
    --csv-out "$TMP_DIR/build_probe_tf.csv"
# Subsequent TF cells use --no-clean.

# ---------------------------------------------------------------------------
# Write combined CSV headers
# ---------------------------------------------------------------------------

echo "system,n,items_per_stream,slots,workers,streams,ms_per_stream,transform_iters,peak_rss_kb" > "$COMBINED_TOMII"
echo "system,n,items_per_stream,slots,workers,streams,ms_per_stream,transform_iters,peak_rss_kb" > "$COMBINED_TF"

# ---------------------------------------------------------------------------
# Sweep loop
# ---------------------------------------------------------------------------
#
# RSS measurement strategy:
#   Tomii  — run_bench.py --measure-rss runs a short /usr/bin/time -v probe
#             on the tomii-core binary directly, capturing the binary's VmHWM
#             (not the Python wrapper's RSS).  The RSS is embedded in the CSV.
#
#   Taskflow — tf_pipeline writes its own VmHWM to a sidecar file
#              <csv>.rss after each invocation; we read from there.
#              We still wrap the Python call in /usr/bin/time -v as a sanity
#              check but take the sidecar value as ground truth.

_read_tf_rss_sidecar() {
    local cell_csv="$1"
    local sidecar="${cell_csv}.rss"
    if [[ -f "$sidecar" ]]; then
        # Format: system,n,S,W,T,rss_kb — take last field of last line.
        tail -1 "$sidecar" | awk -F, '{print $NF}'
    fi
}

for S in "${SWEEP_S[@]}"; do
    # Scale T to ensure at least 4 full batches of S (minimum 2000).
    T=$(( S * 4 < 2000 ? 2000 : S * 4 ))
    WARMUP=$(( T / 10 < 200 ? 200 : T / 10 ))
    # Cap warmup to avoid unreasonably long warmup at high S.
    (( WARMUP > 400 )) && WARMUP=400

    CELL_CSV_TOMII="$TMP_DIR/tomii_highS_s${S}.csv"
    CELL_CSV_TF="$TMP_DIR/tf_highS_s${S}.csv"

    echo ""
    echo "========================================================"
    echo "  S=$S  W=$FIXED_W  T=$T  warmup=$WARMUP  iters=$FIXED_ITERS"
    echo "========================================================"

    # --- Tomii ---
    # RSS is measured by the Python driver itself (--measure-rss flag) using
    # a separate /usr/bin/time -v probe on the tomii-core binary.
    echo "  [Tomii] running..."
    set +e
    timeout "$CELL_TIMEOUT" \
        python "$TOMII_DIR/run_bench.py" \
            --slots "$S" \
            --workers "$FIXED_W" \
            --streams "$T" \
            --warmup "$WARMUP" \
            --transform-iters "$FIXED_ITERS" \
            --no-clean \
            --measure-rss \
            --csv-out "$CELL_CSV_TOMII"
    TOMII_EXIT=$?
    set -e

    if [[ $TOMII_EXIT -ne 0 ]]; then
        echo "  WARNING: Tomii cell S=$S timed out or failed (exit=$TOMII_EXIT)"
        # Write a NaN placeholder.
        echo "tomii,256,256,$S,$FIXED_W,$T,NaN,$FIXED_ITERS,0" >> "$COMBINED_TOMII"
    else
        # The cell CSV already has peak_rss_kb embedded — just append data rows.
        if [[ -f "$CELL_CSV_TOMII" ]]; then
            tail -n +2 "$CELL_CSV_TOMII" >> "$COMBINED_TOMII"
            RSS_TOMII=$(tail -1 "$CELL_CSV_TOMII" | awk -F, '{print $NF}')
            echo "  Tomii binary RSS: $RSS_TOMII kB"
        fi
    fi

    # --- Taskflow ---
    # RSS comes from the binary's own /proc/self/status read (written to
    # <csv>.rss sidecar).
    echo "  [Taskflow] running..."
    set +e
    timeout "$CELL_TIMEOUT" \
        python "$TF_DIR/run_bench.py" \
            --slots "$S" \
            --workers "$FIXED_W" \
            --streams "$T" \
            --warmup "$WARMUP" \
            --no-clean \
            --csv-out "$CELL_CSV_TF"
    TF_EXIT=$?
    set -e

    if [[ $TF_EXIT -ne 0 ]]; then
        echo "  WARNING: Taskflow cell S=$S timed out or failed (exit=$TF_EXIT)"
        echo "taskflow_clone,256,256,$S,$FIXED_W,$T,NaN,$FIXED_ITERS,0" >> "$COMBINED_TF"
    else
        RSS_TF=$(_read_tf_rss_sidecar "$CELL_CSV_TF")
        echo "  Taskflow binary RSS: $RSS_TF kB"
        # Append data rows with RSS column added.
        if [[ -f "$CELL_CSV_TF" ]]; then
            tail -n +2 "$CELL_CSV_TF" | while IFS= read -r line; do
                echo "${line},${RSS_TF:-0}"
            done >> "$COMBINED_TF"
        fi
    fi

    echo "  Done: S=$S"
done

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

echo ""
echo "High-S sweep complete."
echo "  Tomii combined:    $COMBINED_TOMII"
echo "  Taskflow combined: $COMBINED_TF"
echo ""
echo "To generate the two-panel comparison plot:"
echo "  python $BENCH_DIR/pipeline-comparison.py \\"
echo "    --highS \\"
echo "    --highS-tomii-csv  $COMBINED_TOMII \\"
echo "    --highS-taskflow-csv $COMBINED_TF"
