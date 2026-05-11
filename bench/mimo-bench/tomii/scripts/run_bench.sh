#!/usr/bin/env bash
# MIMO benchmark — Tomii 4-node pipeline (fft → csi → beam → demul)
# Mirrors examples/mimolib/scripts/run_mimo.sh pattern:
#   1. Build plugin + tomii binary
#   2. Start Tomii receiver in background
#   3. Start Agora sender after 5 s delay
#   4. Wait for both; print timing

set -e

SCRIPTS_DIR=$(dirname "$(readlink -f "$0")")
TOMII_DIR=$(readlink -f "$SCRIPTS_DIR/..")
BENCH_ROOT=$(readlink -f "$TOMII_DIR/../..")
SYNSTREAM_DIR=$(readlink -f "$BENCH_ROOT/../..")
BIN_DIR="$SYNSTREAM_DIR/tomii-core"
SENDER_DIR=$(readlink -f ~/Agora)

FUNC_PATH="$TOMII_DIR/src/lib.rs"
DYN_LIB="$BENCH_ROOT/target/release/libmimo_bench_tomii.so"
APP_GRAPH=$(python3 -c "
import sys; sys.path.insert(0, '$SYNSTREAM_DIR')
sys.path.insert(0, '$TOMII_DIR')
from build_graph import build_mimo_graph
import tempfile, os
g = build_mimo_graph()
f = tempfile.NamedTemporaryFile(prefix='mimo_graph_', suffix='.json', delete=False, mode='w')
f.write(g.to_json()); f.close()
print(f.name)
")
echo "Graph JSON: $APP_GRAPH"

# Tunable knobs (override via env vars)
WORKERS=${MIMO_WORKERS:-26}
SLOTS=${MIMO_SLOTS:-10}
EXP_STREAMS=${MIMO_STREAMS:-500}
EXCLUDE_STREAMS=${MIMO_EXCLUDE:-200}
RECEIVER_THREADS=${MIMO_RECEIVER_THREADS:-4}
SYSTEM_THREADS=${MIMO_SYSTEM_THREADS:-8}
BATCHING_SIZE=${MIMO_BATCHING_SIZE:-32}
BATCHING_LIMIT=${MIMO_BATCHING_LIMIT:-10}
SCHED_FLUSH_THRESHOLD=${MIMO_SCHED_FLUSH_THRESHOLD:-32}
SPIN_ITERATIONS=${MIMO_SPIN_ITERATIONS:-32}
SPIN_WAIT_SPIN_ITERS=${MIMO_SPIN_WAIT_SPIN_ITERS:-64}
SPIN_WAIT_YIELD_ITERS=${MIMO_SPIN_WAIT_YIELD_ITERS:-256}
SPIN_WAIT_PARK_NS=${MIMO_SPIN_WAIT_PARK_NS:-100}
CLEANUP=${MIMO_CLEANUP:-1}

OUTPUT="$TOMII_DIR/out.txt"
TIMING_FILE="$TOMII_DIR/timing.txt"
SCHED_FILE="$TOMII_DIR/timing_sched.csv"
REPORT_FILE=${MIMO_REPORT_FILE:-"$TOMII_DIR/report.json"}

cleanup() {
    echo "Terminating background processes..."
    [ -n "$CARGO_PID" ] && kill -0 $CARGO_PID 2>/dev/null && kill $CARGO_PID
    [ -n "$SENDER_PID" ] && kill -0 $SENDER_PID 2>/dev/null && kill $SENDER_PID
    exit 1
}
trap cleanup SIGINT

export FUNC_PATH=$FUNC_PATH
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OMP_NUM_THREADS=1
export GOTO_NUM_THREADS=1

# Build
if [ $CLEANUP -eq 1 ]; then
    cargo build --manifest-path "$SYNSTREAM_DIR/Cargo.toml" -r -p tomii-core
    cargo build --manifest-path "$TOMII_DIR/Cargo.toml" -r
fi

rm -f "$OUTPUT" "$TIMING_FILE" "$SCHED_FILE"

# Start Tomii receiver in background
cargo run --manifest-path "$BIN_DIR/Cargo.toml" -r --bin main -- \
    --json "$APP_GRAPH" \
    --dylib "$DYN_LIB" \
    --timing "$TIMING_FILE" \
    --system-threads $SYSTEM_THREADS \
    --receiver-threads $RECEIVER_THREADS \
    --batching-limit $BATCHING_LIMIT \
    --batching-size $BATCHING_SIZE \
    --workers $WORKERS \
    --output "$OUTPUT" \
    --slots $SLOTS \
    --max-streams $EXP_STREAMS \
    --exclude-streams $EXCLUDE_STREAMS \
    --report "$REPORT_FILE" \
    --sched-flush-threshold $SCHED_FLUSH_THRESHOLD \
    --spin-iterations $SPIN_ITERATIONS \
    --spin-wait-spin-iters $SPIN_WAIT_SPIN_ITERS \
    --spin-wait-yield-iters $SPIN_WAIT_YIELD_ITERS \
    --spin-wait-park-ns $SPIN_WAIT_PARK_NS \
    --use-rdtsc --inits --slot-priority --custom &
CARGO_PID=$!

# Start Agora sender after 5 s (same delay as run_mimo.sh)
# Use the 4x4 config so sender packet format matches the Tomii graph.
SENDER_CONFIG=${MIMO_SENDER_CONFIG:-"files/config/ci/tddconfig-4x4.json"}
SENDER_FRAME_DURATION=${MIMO_FRAME_DURATION:-1000}
SENDER_INTER_FRAME_DELAY=${MIMO_INTER_FRAME_DELAY:-0}
chr="********************"
(sleep 5 && cd "$SENDER_DIR" && echo -e "\n${chr} Sender Output ${chr}\n" && \
    ./build/sender --num_threads=2 --core_offset=55 \
        --frame_duration=$SENDER_FRAME_DURATION \
        --enable_slow_start=0 \
        --inter_frame_delay=$SENDER_INTER_FRAME_DELAY \
        --conf_file=$SENDER_CONFIG) &
SENDER_PID=$!

wait $SENDER_PID
wait $CARGO_PID

echo "RUN COMPLETED"
echo "Timing: $TIMING_FILE"
