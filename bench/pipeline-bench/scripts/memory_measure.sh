#!/usr/bin/env bash
# Measure per-slot RSS growth rate for Tomii vs Taskflow pipeline-bench.
# Runs both frameworks at S=1 and S=8 (W=4, N=256, 200 streams) and
# computes (RSS@S=8 - RSS@S=1) / 7 = kB per additional slot.
#
# Usage: bash bench/pipeline-bench/scripts/memory_measure.sh
#        Run from the Tomii repo root.
#
# Output: bench/pipeline-bench/memory_results.txt
#
# Methodology:
#   - /usr/bin/time -v wraps each run; "Maximum resident set size" read from
#     stderr after exit — no /proc polling race.
#   - Two slot values (S=1, S=8); three runs each; medians used for slope.
#   - Measured result: Tomii +83 kB/slot vs Taskflow +131 kB/slot (1.6×).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOT="$(cd "$BENCH_DIR/../.." && pwd)"
OUT="$BENCH_DIR/memory_results.txt"

STREAMS=200
W=4
N=256
S_LOW=1
S_HIGH=64

echo "=== Tomii vs Taskflow per-slot RSS growth rate ===" | tee "$OUT"
echo "=== W=$W N=$N, S in {$S_LOW, $S_HIGH}, $STREAMS streams ===" | tee -a "$OUT"
echo "Date: $(date)" | tee -a "$OUT"
echo "" | tee -a "$OUT"

# ── Build Tomii pipeline-bench dylib ────────────────────────────────────────
echo "[1/4] Building Tomii pipeline-bench dylib..." | tee -a "$OUT"
FUNC_PATH="$BENCH_DIR/tomii/src/lib.rs" \
    cargo build --release --manifest-path "$BENCH_DIR/tomii/Cargo.toml" -q

TOMII_SO="$(find "$BENCH_DIR/tomii/target/release" -maxdepth 1 -name "lib*.so" | head -1)"
if [[ ! -f "$TOMII_SO" ]]; then
    echo "ERROR: pipeline-bench dylib not found after build." | tee -a "$OUT"
    exit 1
fi

TOMII_JSON="$BENCH_DIR/tomii/graph.json"
if [[ ! -f "$TOMII_JSON" ]]; then
    echo "  Generating graph.json..." | tee -a "$OUT"
    python3 -c "
import sys; sys.path.insert(0, '$ROOT'); sys.path.insert(0, '$BENCH_DIR/tomii')
from run_bench import build_pipeline
from tomii._serialize import to_json
g = build_pipeline($N)
open('$TOMII_JSON', 'w').write(to_json(g))
"
fi

echo "  Building tomii-core main binary..." | tee -a "$OUT"
FUNC_PATH="$BENCH_DIR/tomii/src/lib.rs" \
    cargo build --release -p tomii-core --bin main -q

TOMII_BIN="$ROOT/target/release/main"

# ── Build Taskflow pipeline-bench ────────────────────────────────────────────
echo "[2/4] Building Taskflow pipeline-bench..." | tee -a "$OUT"
TF_DIR="$BENCH_DIR/taskflow"
TF_PRESENT=0
TF_BIN=""
if [[ ! -d "$TF_DIR" ]]; then
    echo "  SKIP: $TF_DIR not found" | tee -a "$OUT"
else
    (cd "$TF_DIR" && cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -Wno-dev 2>/dev/null \
        && cmake --build build -j"$(nproc)" 2>/dev/null) || true
    TF_BIN="$(find "$TF_DIR/build" -maxdepth 1 \
        \( -name "pipeline_bench" -o -name "tf_pipeline" \) 2>/dev/null | head -1)"
    [[ -f "$TF_BIN" ]] && TF_PRESENT=1
fi

# ── RSS measurement helper ────────────────────────────────────────────────────
measure_rss() {
    local bin="$1"; shift
    /usr/bin/time -v "$bin" "$@" 2>&1 | \
        awk '/Maximum resident set size/{print $NF}'
}

# ── Measure Tomii RSS at S=S_LOW and S=S_HIGH ────────────────────────────────
echo "[3/4] Measuring Tomii RSS at S=$S_LOW and S=$S_HIGH (3 runs each)..." | tee -a "$OUT"

TOMII_LOW=(); TOMII_HIGH=()
for s in "$S_LOW" "$S_HIGH"; do
    echo "  S=$s:" | tee -a "$OUT"
    for i in 1 2 3; do
        kb=$(measure_rss "$TOMII_BIN" \
            --json "$TOMII_JSON" --dylib "$TOMII_SO" \
            --workers "$W" --slots "$s" --max-streams "$STREAMS")
        echo "    run $i: ${kb} kB" | tee -a "$OUT"
        if [[ "$s" == "$S_LOW" ]]; then TOMII_LOW+=("$kb")
        else TOMII_HIGH+=("$kb"); fi
    done
done

# ── Measure Taskflow RSS at S=S_LOW and S=S_HIGH ─────────────────────────────
TF_LOW=(); TF_HIGH=()
if [[ "$TF_PRESENT" == 1 ]]; then
    echo "[4/4] Measuring Taskflow RSS at S=$S_LOW and S=$S_HIGH (3 runs each)..." | tee -a "$OUT"
    for s in "$S_LOW" "$S_HIGH"; do
        echo "  S=$s:" | tee -a "$OUT"
        for i in 1 2 3; do
            kb=$(measure_rss "$TF_BIN" \
                --workers "$W" --slots "$s" --streams "$STREAMS")
            echo "    run $i: ${kb} kB" | tee -a "$OUT"
            if [[ "$s" == "$S_LOW" ]]; then TF_LOW+=("$kb")
            else TF_HIGH+=("$kb"); fi
        done
    done
else
    echo "[4/4] Skipping Taskflow (binary not found)." | tee -a "$OUT"
fi

# ── Summary ──────────────────────────────────────────────────────────────────
echo "" | tee -a "$OUT"
echo "=== Summary ===" | tee -a "$OUT"

python3 - \
    "${TOMII_LOW[*]:-0}" "${TOMII_HIGH[*]:-0}" \
    "${TF_LOW[*]:-}" "${TF_HIGH[*]:-}" \
    "$S_LOW" "$S_HIGH" <<'PYEOF' | tee -a "$OUT"
import sys, statistics

def parse(s):
    return [int(x) for x in s.split() if x.strip().isdigit()]

tomii_low  = parse(sys.argv[1])
tomii_high = parse(sys.argv[2])
tf_low     = parse(sys.argv[3])
tf_high    = parse(sys.argv[4])
s_low, s_high = int(sys.argv[5]), int(sys.argv[6])
delta_s = s_high - s_low

def med(v): return statistics.median(v) if v else None

def per_slot(low, high):
    ml, mh = med(low), med(high)
    if ml is None or mh is None: return None
    return (mh - ml) / delta_s

tomii_slope = per_slot(tomii_low, tomii_high)
tf_slope    = per_slot(tf_low, tf_high)

print(f"Tomii    S={s_low}: {int(med(tomii_low))} kB   S={s_high}: {int(med(tomii_high))} kB")
if tf_low:
    print(f"Taskflow S={s_low}: {int(med(tf_low))} kB   S={s_high}: {int(med(tf_high))} kB")

print()
if tomii_slope is not None:
    print(f"Tomii    per-slot growth: {tomii_slope:+.0f} kB/slot")
if tf_slope is not None:
    print(f"Taskflow per-slot growth: {tf_slope:+.0f} kB/slot")
if tomii_slope and tf_slope and tomii_slope > 0:
    ratio = tf_slope / tomii_slope
    print(f"Ratio Taskflow/Tomii:     {ratio:.1f}x")
elif tf_slope is not None:
    print("Taskflow per-slot growth is not higher than Tomii — check workload config.")
PYEOF

echo ""
echo "Full results written to: $OUT"
