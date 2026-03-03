#!/bin/bash
# Master benchmark runner.
#
# Runs the full SynStream vs Timely Dataflow comparison:
#   1. STREAM benchmarks (copy, scale, add, triad) across worker counts
#   2. PageRank (COST) benchmarks on LiveJournal and Twitter
#
# Prerequisites:
#   - cargo (Rust toolchain)
#   - Python ≥ 3.9 with synstream installed (source .venv/bin/activate)
#   - Graph datasets in $SNAP_DATA_DIR (run download_datasets.sh first)
#
# Usage:
#   cd /path/to/synstream-sosp
#   source .venv/bin/activate
#   bash benchmarks/run_all_benchmarks.sh
#
# Override defaults:
#   WORKERS_LIST="1 2 4 8 16" SNAP_DATA_DIR=/data/snap bash benchmarks/run_all_benchmarks.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_DIR="${RESULTS_DIR:-$REPO_ROOT/benchmarks/results}"
SNAP_DATA_DIR="${SNAP_DATA_DIR:-/data/snap}"
WORKERS_LIST="${WORKERS_LIST:-1 2 4 8 16}"
STREAM_REPS=20
STREAM_WARMUP=3
PR_ITERS=20
KERNELS="copy scale add triad"

mkdir -p "$RESULTS_DIR"

echo "=========================================="
echo "  SynStream vs Timely Benchmark Suite"
echo "  Results dir: $RESULTS_DIR"
echo "  Workers: $WORKERS_LIST"
echo "=========================================="

# ---------------------------------------------------------------------------
# 1. Build everything
# ---------------------------------------------------------------------------
echo ""
echo "=== Building workspace ==="
cd "$REPO_ROOT"

# Export dummy WRAP_PATH/REG_PATH so synstream-core builds without a real plugin
export WRAP_PATH="$REPO_ROOT/examples/stream-bench/wrappers.rs"
export REG_PATH="$REPO_ROOT/examples/stream-bench/reg.rs"
cargo build -r 2>&1 | tail -5
unset WRAP_PATH REG_PATH

# ---------------------------------------------------------------------------
# 2. SynStream STREAM sweep
# ---------------------------------------------------------------------------
echo ""
echo "=== SynStream STREAM benchmarks ==="
export RESULTS_DIR="$RESULTS_DIR"
python "$REPO_ROOT/examples/stream-bench/run_bench.py" \
    --workers $WORKERS_LIST \
    --kernels $KERNELS \
    --max-streams "$STREAM_REPS" \
    --exclude-streams "$STREAM_WARMUP" \
    --results-dir "$RESULTS_DIR" \
    --no-clean

# ---------------------------------------------------------------------------
# 3. Timely STREAM sweep
# ---------------------------------------------------------------------------
echo ""
echo "=== Timely STREAM benchmarks ==="
for KERNEL in $KERNELS; do
    for W in $WORKERS_LIST; do
        echo "  Timely STREAM $KERNEL workers=$W"
        "$REPO_ROOT/target/release/stream_bench" \
            --kernel "$KERNEL" \
            --workers "$W" \
            --reps "$STREAM_REPS" \
            --warmup "$STREAM_WARMUP" \
            --output "$RESULTS_DIR/timely_stream_${KERNEL}_w${W}.csv"
    done
done

# ---------------------------------------------------------------------------
# 4. TBB STREAM sweep (unpinned + pinned)
# ---------------------------------------------------------------------------
TBB_BIN="$REPO_ROOT/tbb-bench/stream_bench"
if [ ! -x "$TBB_BIN" ]; then
    echo "  [skip] TBB stream_bench not found at $TBB_BIN — run 'make' in tbb-bench/ first"
else
    echo ""
    echo "=== TBB STREAM benchmarks (unpinned) ==="
    for KERNEL in $KERNELS; do
        for W in $WORKERS_LIST; do
            echo "  TBB STREAM $KERNEL workers=$W"
            "$TBB_BIN" \
                --kernel "$KERNEL" \
                --workers "$W" \
                --reps "$STREAM_REPS" \
                --warmup "$STREAM_WARMUP" \
                --output "$RESULTS_DIR/tbb_stream_${KERNEL}_w${W}.csv"
        done
    done

    echo ""
    echo "=== TBB STREAM benchmarks (pinned) ==="
    for KERNEL in $KERNELS; do
        for W in $WORKERS_LIST; do
            echo "  TBB STREAM (pinned) $KERNEL workers=$W"
            "$TBB_BIN" \
                --kernel "$KERNEL" \
                --workers "$W" \
                --reps "$STREAM_REPS" \
                --warmup "$STREAM_WARMUP" \
                --pin \
                --output "$RESULTS_DIR/tbb_pinned_stream_${KERNEL}_w${W}.csv"
        done
    done
fi

# ---------------------------------------------------------------------------
# 6. SynStream PageRank sweep
# ---------------------------------------------------------------------------
echo ""
echo "=== SynStream PageRank benchmarks ==="
for DATASET in livejournal twitter; do
    GRAPH_FILE="$SNAP_DATA_DIR/${DATASET}.txt"
    if [ ! -f "$GRAPH_FILE" ]; then
        echo "  [skip] $GRAPH_FILE not found (run download_datasets.sh)"
        continue
    fi
    SNAP_GRAPH_FILE="$GRAPH_FILE" \
    python "$REPO_ROOT/examples/cost-bench/run_bench.py" \
        --workers $WORKERS_LIST \
        --iterations "$PR_ITERS" \
        --graph-file "$GRAPH_FILE" \
        --results-dir "$RESULTS_DIR" \
        --no-clean
done

# ---------------------------------------------------------------------------
# 7. TBB PageRank sweep (unpinned + pinned)
# ---------------------------------------------------------------------------
TBB_PR_BIN="$REPO_ROOT/tbb-bench/pagerank"
if [ ! -x "$TBB_PR_BIN" ]; then
    echo "  [skip] TBB pagerank not found at $TBB_PR_BIN — run 'make' in tbb-bench/ first"
else
    for DATASET in livejournal twitter; do
        GRAPH_FILE="$SNAP_DATA_DIR/${DATASET}.txt"
        if [ ! -f "$GRAPH_FILE" ]; then
            echo "  [skip] $GRAPH_FILE not found"
            continue
        fi

        echo ""
        echo "=== TBB PageRank $DATASET (unpinned) ==="
        for W in $WORKERS_LIST; do
            echo "  TBB PageRank dataset=$DATASET workers=$W"
            "$TBB_PR_BIN" \
                --graph "$GRAPH_FILE" \
                --dataset "$DATASET" \
                --workers "$W" \
                --iterations "$PR_ITERS" \
                --output "$RESULTS_DIR/tbb_pagerank_${DATASET}_w${W}.csv"
        done

        echo ""
        echo "=== TBB PageRank $DATASET (pinned) ==="
        for W in $WORKERS_LIST; do
            echo "  TBB PageRank (pinned) dataset=$DATASET workers=$W"
            "$TBB_PR_BIN" \
                --graph "$GRAPH_FILE" \
                --dataset "$DATASET" \
                --workers "$W" \
                --iterations "$PR_ITERS" \
                --pin \
                --output "$RESULTS_DIR/tbb_pinned_pagerank_${DATASET}_w${W}.csv"
        done
    done
fi

# ---------------------------------------------------------------------------
# 8. Timely PageRank sweep
# ---------------------------------------------------------------------------
echo ""
echo "=== Timely PageRank benchmarks ==="
for DATASET in livejournal twitter; do
    GRAPH_FILE="$SNAP_DATA_DIR/${DATASET}.txt"
    if [ ! -f "$GRAPH_FILE" ]; then
        echo "  [skip] $GRAPH_FILE not found"
        continue
    fi
    for W in $WORKERS_LIST; do
        echo "  Timely PageRank dataset=$DATASET workers=$W"
        "$REPO_ROOT/target/release/pagerank" \
            --graph-file "$GRAPH_FILE" \
            --iterations "$PR_ITERS" \
            --workers "$W" \
            --dataset "$DATASET" \
            --output "$RESULTS_DIR/timely_pagerank_${DATASET}_w${W}.csv"
    done
done

# ---------------------------------------------------------------------------
# 9. Generate comparison plots
# ---------------------------------------------------------------------------
echo ""
echo "=== Generating comparison plots ==="
python "$REPO_ROOT/benchmarks/compare_results.py" \
    --results-dir "$RESULTS_DIR" \
    --output-dir  "$RESULTS_DIR"

echo ""
echo "All benchmarks complete.  Results in $RESULTS_DIR"
