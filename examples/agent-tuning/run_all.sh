#!/usr/bin/env bash
set -euo pipefail

# Usage: ./run_all.sh [iterations] [workload] [extra args passed to every arm]
#   ./run_all.sh                          # 50 iterations, stream-analytics
#   ./run_all.sh 50 pipeline
#   ./run_all.sh 30 mimo --streams 200 --warmup 20
ITERATIONS=${1:-50}
WORKLOAD=${2:-stream-analytics}
shift $(( $# > 2 ? 2 : $# )) || true
EXTRA_ARGS=("$@")

RESULTS_DIR="results/${WORKLOAD}_run_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"

# Run from the directory containing this script so harness.py is importable
cd "$(dirname "${BASH_SOURCE[0]}")"

COMMON=(--workload "$WORKLOAD" --results-dir "$RESULTS_DIR" "${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}")

echo "=== Establishing baseline ($WORKLOAD) ==="
python harness.py "${COMMON[@]}"

echo ""
echo "=== Arm 1: Random search ==="
python arms/random_search.py --iterations "$ITERATIONS" "${COMMON[@]}"

echo ""
echo "=== Arm 2: Bayesian (Optuna) ==="
python arms/bayesian.py --iterations "$ITERATIONS" "${COMMON[@]}"

echo ""
echo "=== Arm 3: Grid search ==="
python arms/grid.py --iterations "$ITERATIONS" "${COMMON[@]}"

echo ""
echo "=== Arm 4: Agent (Claude) ==="
python arms/agent.py --iterations "$ITERATIONS" "${COMMON[@]}"

echo ""
echo "=== Results in $RESULTS_DIR ==="
