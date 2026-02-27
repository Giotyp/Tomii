# Get current script directory (absolute path)
SCRIPTS_DIR=$(dirname "$(readlink -f "$0")")
MIMOLIB_DIR=$(readlink -f "$SCRIPTS_DIR/..")
SYNSTREAM_DIR=$(readlink -f "$SCRIPTS_DIR/../../..")
BIN_DIR="$SYNSTREAM_DIR/synstream-core"
SENDER_DIR=$(readlink -f ~/Agora)
PYTHON_DIR="$SYNSTREAM_DIR/scripts"

# source wrappers and functions
FUNC_PATH="$MIMOLIB_DIR/src/lib.rs"
WRAP_PATH="$MIMOLIB_DIR/wrap/wrappers_ptr.rs"
REG_PATH="$MIMOLIB_DIR/wrap/reg.rs"

# Generated dynamic library
# Library is built in workspace target directory
DYN_LIB="$SYNSTREAM_DIR/target/release/libmimolib.so"

# Json Application graph
APP_GRAPH=$(readlink -f "$MIMOLIB_DIR/graphs/graph_per_symbol.json")

# Configuration Parameters
WORKERS=26
RUNTIME=5000
SLOTS=40
EXP_STREAMS=500
EXCLUDE_STREAMS=200 # Exclude first N streams from statistics (warm-up)
RcvThreads=4  # Number of dedicated network receiver threads
SYSTEM_THREADS=8
SLOT_PRIORITY="--slot-priority"
RECORD_STREAM="--record-stream 499"
INITS="--inits"
RDTSC="--use-rdtsc"
BATCHING_SIZE=32
BATCHING_LIMIT=10
OUTPUT="$MIMOLIB_DIR/out.txt"
TIMING="timing"
TIMING_FILE="$MIMOLIB_DIR/timing.txt"
SCHED_FILE="$MIMOLIB_DIR/timing_sched.csv"
RECORD="--record"
DEBUG=""
CLEANUP=0
RUN_BIN=0


# Function to cleanup background processes on SIGINT
cleanup() {
    echo "Terminating background processes..."
    if [ -n "$CARGO_PID" ] && kill -0 $CARGO_PID 2>/dev/null; then
        kill $CARGO_PID
    fi
    if [ -n "$SENDER_PID" ] && kill -0 $SENDER_PID 2>/dev/null; then
        kill $SENDER_PID
    fi
    exit 1
}

# Trap SIGINT (Ctrl+C) to call cleanup
trap cleanup SIGINT

export FUNC_PATH=$FUNC_PATH
export WRAP_PATH=$WRAP_PATH
export REG_PATH=$REG_PATH

if [ $CLEANUP -eq 1 ]; then
    # Clean and compile SynStream and mimo library
    cargo clean --manifest-path "$BIN_DIR/Cargo.toml"
    cargo build --manifest-path "$SYNSTREAM_DIR/Cargo.toml" -r -p synstream-core
    cargo build --manifest-path "$SYNSTREAM_DIR/Cargo.toml" -r -p synstream-types

    # Compile library with the specific paths
    cargo build --manifest-path "$MIMOLIB_DIR/Cargo.toml" -r
fi

if [ $RUN_BIN -eq 1 ]; then
    # Remove old output and timing files if they exist
    rm -f $OUTPUT
    rm -f $TIMING_FILE
    rm -f $SCHED_FILE

    export OPENBLAS_NUM_THREADS=1
    export MKL_NUM_THREADS=1
    export OMP_NUM_THREADS=1
    export GOTO_NUM_THREADS=1

    # Run the main binary from synstream-core in the background
    cargo run --manifest-path "$BIN_DIR/Cargo.toml" -r --bin main -- \
        --json $APP_GRAPH \
        --dylib $DYN_LIB \
        --timing $TIMING_FILE \
        --system-threads $SYSTEM_THREADS \
        --receiver-threads $RcvThreads \
        --batching-limit $BATCHING_LIMIT \
        --batching-size $BATCHING_SIZE \
        --workers $WORKERS \
        --output $OUTPUT \
        --max-runtime $RUNTIME \
        --slots $SLOTS \
        --max-streams $EXP_STREAMS \
        --exclude-streams $EXCLUDE_STREAMS \
        $DEBUG $RDTSC \
        $RECORD $RECORD_STREAM \
        $INITS $SLOT_PRIORITY --custom &
    CARGO_PID=$!

    # Run the sender script in the background after a short delay to allow the receiver to start
    chr="********************"
    filler="\n${chr} Sender Output ${chr}\n"
    (sleep 5 && cd "$SENDER_DIR" && echo -e $filler && ./run_sender.sh && echo -e $filler) &
    SENDER_PID=$!

    # # Wait for both processes to complete
    wait $SENDER_PID
    wait $CARGO_PID
fi


uv run python $PYTHON_DIR/scheduler_visualize.py $SCHED_FILE \
    --units ms \
    -o "$SCRIPTS_DIR/timing.png" \
    --system-threads $SYSTEM_THREADS \
    --title "MIMO Benchmark ${TIMING} -- Batch Limit $BATCHING_LIMIT, Batch Size $BATCHING_SIZE" \
    --tasks '[fft, csi, beam, demul, decode]' \
    --plot-latency

echo "Generated scheduler visualization"

uv run python $PYTHON_DIR/analyze_sched.py $SCHED_FILE \
    --system-threads $SYSTEM_THREADS \
    --units ms

echo "Completed analysis of scheduling data"