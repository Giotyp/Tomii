#!/usr/bin/env bash
set -euo pipefail

ITERATIONS=${1:-50}
RESULTS_DIR="results/run_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"

# Run from the directory containing this script so harness.py is importable
cd "$(dirname "${BASH_SOURCE[0]}")"

echo "=== Establishing baseline ==="
python harness.py --results-dir "$RESULTS_DIR"

echo ""
echo "=== Arm 1: Random search ==="
python arms/random_search.py --iterations "$ITERATIONS" --results-dir "$RESULTS_DIR"

echo ""
echo "=== Arm 2: Bayesian (Optuna) ==="
python arms/bayesian.py --iterations "$ITERATIONS" --results-dir "$RESULTS_DIR"

echo ""
echo "=== Arm 3: Grid search ==="
python arms/grid.py --iterations "$ITERATIONS" --results-dir "$RESULTS_DIR"

echo ""
echo "=== Arm 4: Agent (Claude) ==="
python arms/agent.py --iterations "$ITERATIONS" --results-dir "$RESULTS_DIR"

echo ""
echo "=== Results in $RESULTS_DIR ==="
