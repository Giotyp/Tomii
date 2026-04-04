#!/usr/bin/env bash
# Wavefront ablation benchmark: 2x2 (graph backend x resolution) comparison.
# Measures per-sweep latency for each combination on the anti-diagonal wavefront DAG.
#
# Output: results/wavefront_ablation.csv
#   Columns: system,n,workers,iterations,total_s,s_per_iter,mean_ms,stddev_ms
#
# Paper attribution:
#   flat-centralized  vs  pointer-centralized  ->  contribution of Flat Indexed State alone
#   flat-distributed  vs  flat-centralized     ->  contribution of Distributed Resolution alone
#   flat-distributed  vs  pointer-centralized  ->  combined contribution of both techniques

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BIN="$SCRIPT_DIR/target/release/wavefront-ablation"

if [ ! -f "$BIN" ]; then
    echo "Binary not found. Building..."
    cd "$SCRIPT_DIR" && cargo build --release
fi

RESULTS_DIR="$SCRIPT_DIR/results"
mkdir -p "$RESULTS_DIR"
OUT="$RESULTS_DIR/wavefront_ablation.csv"

# Parameters
GRID_SIZES="${GRID_SIZES:-128 256 512 1024}"
WORKER_COUNTS="${WORKER_COUNTS:-1 2 4 8 16}"
ITERATIONS="${ITERATIONS:-20}"
WARMUP="${WARMUP:-3}"
MODES="flat-distributed flat-centralized pointer-distributed pointer-centralized"

# Write CSV header
echo "system,n,workers,iterations,total_s,s_per_iter,mean_ms,stddev_ms" > "$OUT"

total_runs=$(echo "$MODES" | wc -w)
total_runs=$((total_runs * $(echo $GRID_SIZES | wc -w) * $(echo $WORKER_COUNTS | wc -w)))
run=0

for mode in $MODES; do
    for n in $GRID_SIZES; do
        for w in $WORKER_COUNTS; do
            run=$((run + 1))
            echo "[$run/$total_runs] mode=$mode n=$n workers=$w"
            "$BIN" --mode "$mode" --n "$n" --workers "$w" \
                   --iterations "$ITERATIONS" --warmup "$WARMUP" \
                >> "$OUT"
        done
    done
done

echo ""
echo "Results written to $OUT"
echo ""
echo "=== Flat Indexed State contribution (flat vs pointer, distributed resolution) ==="
echo "Comparing flat-distributed vs pointer-distributed (same resolution, different graph storage)"
echo "system,n,workers,mean_ms"
awk -F, 'NR>1 && ($1=="flat-distributed" || $1=="pointer-distributed") { printf "%s,%s,%s,%s\n", $1,$2,$3,$7 }' "$OUT" \
  | sort -t, -k2,2n -k3,3n
echo ""
echo "=== Distributed Resolution contribution (distributed vs centralized, flat graph) ==="
echo "Comparing flat-distributed vs flat-centralized (same graph, different resolution mechanism)"
echo "system,n,workers,mean_ms"
awk -F, 'NR>1 && ($1=="flat-distributed" || $1=="flat-centralized") { printf "%s,%s,%s,%s\n", $1,$2,$3,$7 }' "$OUT" \
  | sort -t, -k2,2n -k3,3n
echo ""
echo "=== Combined overhead: flat-distributed vs pointer-centralized (baseline) ==="
awk -F, 'NR>1 && ($1=="flat-distributed" || $1=="pointer-centralized") { printf "%s,%s,%s,%s\n", $1,$2,$3,$7 }' "$OUT" \
  | sort -t, -k2,2n -k3,3n

# ── Reset-cost microbenchmark (generational O(1) vs eager O(N)) ──────────
RESET_OUT="$RESULTS_DIR/reset_bench.csv"
echo "[$((run + 1))/...] mode=reset-bench"
"$BIN" --mode reset-bench --iterations "${ITERATIONS}" > "$RESET_OUT"
echo ""
echo "Results written to $RESET_OUT"

# ── Corrected ablation: flat-generational (SynStream O(1) reset) ─────────
# Appends flat-generational rows to the existing wavefront_ablation.csv so
# they can be compared directly against flat-distributed and flat-eager.
MODES_NEW="flat-generational flat-eager"
total_new=$(echo "$MODES_NEW" | wc -w)
total_new=$((total_new * $(echo $GRID_SIZES | wc -w) * $(echo $WORKER_COUNTS | wc -w)))
new_run=0

for mode in $MODES_NEW; do
    for n in $GRID_SIZES; do
        for w in $WORKER_COUNTS; do
            new_run=$((new_run + 1))
            echo "[$new_run/$total_new] mode=$mode n=$n workers=$w"
            "$BIN" --mode "$mode" --n "$n" --workers "$w" \
                   --iterations "$ITERATIONS" --warmup "$WARMUP" \
                >> "$OUT"
        done
    done
done

echo ""
echo "=== Generational (O(1) reset) vs Eager (O(N) reset), flat graph ==="
echo "Comparing flat-generational vs flat-eager (same graph + decrement, only reset differs)"
echo "system,n,workers,mean_ms"
awk -F, 'NR>1 && ($1=="flat-generational" || $1=="flat-eager") { printf "%s,%s,%s,%s\n", $1,$2,$3,$7 }' "$OUT" \
  | sort -t, -k2,2n -k3,3n
