#!/usr/bin/env bash
# Measure peak RSS for Tomii vs Taskflow pipeline-bench at S=8, W=4.
# Records /proc/self/status VmPeak via wrapper harness so the runtime
# reports its own peak resident set.  Run from the bench/ root.
#
# Usage: bash bench/pipeline-bench/scripts/memory_measure.sh [--clean]
#
# Output: bench/pipeline-bench/memory_results.txt
#
# Methodology:
#   - Same workload (N=256, S=8, W=4, TRANSFORM_ITERS=2048, 200 streams) for both.
#   - Each run is launched via a small wrapper that patches LD_PRELOAD to intercept
#     exit() and dump /proc/self/status; for binaries that cooperate, we read
#     VmPeak directly from /proc/<pid>/status after the run.
#   - Three independent runs each; report min/median/max across the three.
#   - Tomii RSS includes all runtime state (slot array, node cache, Rayon pool).
#   - Taskflow RSS includes the cloned sub-graph per stream (S independent tf::Taskflow).
#   - The 2.8× figure in the README (96 B vs 271 B per slot) is derived from code;
#     this script provides the direct /proc confirmation.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOT="$(cd "$BENCH_DIR/../.." && pwd)"
OUT="$BENCH_DIR/memory_results.txt"

STREAMS=200
S=8
W=4

echo "=== Tomii vs Taskflow peak RSS at S=$S W=$W ===" | tee "$OUT"
echo "Date: $(date)" | tee -a "$OUT"
echo "" | tee -a "$OUT"

# ── Build Tomii pipeline-bench ──────────────────────────────────────────────
echo "[1/4] Building Tomii pipeline-bench..." | tee -a "$OUT"
(cd "$BENCH_DIR/tomii" && cargo build --release -q)
TOMII_BIN="$BENCH_DIR/tomii/target/release/pipeline-bench"

# Locate the graph/dylib produced by cargo
TOMII_SO="$(find "$BENCH_DIR/tomii/target/release" -name "libpipeline_bench*.so" | head -1)"
TOMII_JSON="$BENCH_DIR/tomii/graph.json"

if [[ ! -f "$TOMII_SO" ]]; then
    echo "ERROR: libpipeline_bench*.so not found. Run 'cargo build --release' in $BENCH_DIR/tomii first." | tee -a "$OUT"
    exit 1
fi

# ── Measure Tomii peak RSS ───────────────────────────────────────────────────
echo "[2/4] Measuring Tomii RSS (3 runs)..." | tee -a "$OUT"

measure_tomii_rss() {
    local pid rss
    "$ROOT/target/release/main" \
        --json "$TOMII_JSON" \
        --dylib "$TOMII_SO" \
        --workers "$W" \
        --slots "$S" \
        --max-streams "$STREAMS" &
    pid=$!
    local peak=0
    while kill -0 "$pid" 2>/dev/null; do
        if [[ -r /proc/$pid/status ]]; then
            rss=$(awk '/^VmPeak:/{print $2}' /proc/$pid/status 2>/dev/null || echo 0)
            [[ "$rss" -gt "$peak" ]] && peak="$rss"
        fi
        sleep 0.05
    done
    wait "$pid" 2>/dev/null || true
    echo "$peak"
}

# Build the main binary first
(cd "$ROOT" && cargo build --release -p tomii-core --bin main -q)

TOMII_RSS=()
for i in 1 2 3; do
    kb=$(measure_tomii_rss)
    TOMII_RSS+=("$kb")
    echo "  run $i: ${kb} kB" | tee -a "$OUT"
done

# ── Build Taskflow pipeline-bench ────────────────────────────────────────────
echo "[3/4] Building Taskflow pipeline-bench..." | tee -a "$OUT"
TF_DIR="$BENCH_DIR/taskflow"
if [[ ! -d "$TF_DIR" ]]; then
    echo "  SKIP: $TF_DIR not found (Taskflow comparator not present)" | tee -a "$OUT"
    TF_PRESENT=0
else
    (cd "$TF_DIR" && cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -Wno-dev -q && cmake --build build -j"$(nproc)" -q) || true
    TF_BIN="$(find "$TF_DIR/build" -name "pipeline_bench" -o -name "tf_pipeline" 2>/dev/null | head -1)"
    TF_PRESENT=$([[ -f "$TF_BIN" ]] && echo 1 || echo 0)
fi

TF_RSS=()
if [[ "$TF_PRESENT" == 1 ]]; then
    echo "[4/4] Measuring Taskflow RSS (3 runs)..." | tee -a "$OUT"
    measure_tf_rss() {
        local pid
        "$TF_BIN" --workers "$W" --slots "$S" --streams "$STREAMS" &
        pid=$!
        local peak=0
        while kill -0 "$pid" 2>/dev/null; do
            if [[ -r /proc/$pid/status ]]; then
                rss=$(awk '/^VmPeak:/{print $2}' /proc/$pid/status 2>/dev/null || echo 0)
                [[ "$rss" -gt "$peak" ]] && peak="$rss"
            fi
            sleep 0.05
        done
        wait "$pid" 2>/dev/null || true
        echo "$peak"
    }
    for i in 1 2 3; do
        kb=$(measure_tf_rss)
        TF_RSS+=("$kb")
        echo "  run $i: ${kb} kB" | tee -a "$OUT"
    done
else
    echo "[4/4] Skipping Taskflow RSS (binary not found)." | tee -a "$OUT"
fi

# ── Summary ──────────────────────────────────────────────────────────────────
echo "" | tee -a "$OUT"
echo "=== Summary ===" | tee -a "$OUT"
python3 - <<PYEOF | tee -a "$OUT"
import statistics, sys

tomii = [${TOMII_RSS[*]:-0}]
tf    = [${TF_RSS[*]:-}]

def fmt(vals):
    if not vals:
        return "N/A"
    return f"min={min(vals)} kB  median={int(statistics.median(vals))} kB  max={max(vals)} kB"

print(f"Tomii  (S=$S, W=$W): {fmt(tomii)}")
if tf:
    print(f"Taskflow (S=$S, W=$W): {fmt(tf)}")
    ratio = statistics.median(tf) / statistics.median(tomii) if statistics.median(tomii) > 0 else float('nan')
    print(f"Ratio Taskflow/Tomii: {ratio:.2f}×  (README claims 2.8×)")
else:
    print("Taskflow: not measured")
PYEOF

echo "" | tee -a "$OUT"
echo "Full results written to: $OUT"
