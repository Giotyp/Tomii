#!/bin/sh

# Clear any previously set environment variables from parent shell
unset FUNC_PATH WRAP_PATH REG_PATH

# Get current script directory (absolute path)
SCRIPT_PATH=$(readlink -f "$0")
SCRIPT_DIR=$(dirname "$SCRIPT_PATH")

# source wrappers and functions
FUNC_PATH=$(readlink -f "$SCRIPT_DIR/src/functions.rs")
WRAP_PATH=$(readlink -f "$SCRIPT_DIR/wrappers.rs")
REG_PATH=$(readlink -f "$SCRIPT_DIR/reg.rs")

# Generated dynamic library
# Target directory is 2 levels up from script directory
TARGET_DIR=$(dirname $(dirname $SCRIPT_DIR))
DYN_LIB=$TARGET_DIR/target/release/libmatcomp.so

# Json Application graph
APP_GRAPH=$(readlink -f "$SCRIPT_DIR/graph.json")

# Configuration Parameters
WORKERS=2
RUNTIME=10
SLOTS=2
EXP_STREAMS=1
OUTPUT="$SCRIPT_DIR/out.txt"
TIMING_FILE="$SCRIPT_DIR/timing.txt"
SYSTEM_THREADS=3
BATCHING_SIZE=1
BATCHING_LIMIT=10
DEBUG="" # Set to "--debug" to enable debug mode
RECORD="--record" # Set to "--record" to enable scheduler recording


cargo clean --manifest-path "$SCRIPT_DIR/../../synstream-core/Cargo.toml"

# Compile library with the specific paths
cargo build --manifest-path "$SCRIPT_DIR/Cargo.toml" -r

# Choose profiling mode: set PROFILE=1 to use flamegraph, otherwise use normal cargo run
if [ "${PROFILE:-0}" = "1" ]; then
    echo "Running with flamegraph profiling..."
    FUNC_PATH="$FUNC_PATH" \
    WRAP_PATH="$WRAP_PATH" \
    REG_PATH="$REG_PATH" \
    SCRIPT_DIR="$SCRIPT_DIR" \
    CARGO_PROFILE_RELEASE_DEBUG=true \
    cargo flamegraph -F 5000 --manifest-path "$SCRIPT_DIR/../../synstream-core/Cargo.toml" \
        -r --bin main -o "$SCRIPT_DIR/flamegraph.svg" -- \
        --json $APP_GRAPH \
        --dylib $DYN_LIB \
        --inits \
        --workers $WORKERS \
        --output $OUTPUT \
        --max-runtime $RUNTIME \
        --slots $SLOTS \
        --max-streams $EXP_STREAMS \
        --timing $TIMING_FILE \
    
    echo "Flamegraph saved to: $SCRIPT_DIR/flamegraph.svg"
else
    echo "Running without profiling..."
    # Run the main binary from synstream-core
    FUNC_PATH=$FUNC_PATH \
    WRAP_PATH=$WRAP_PATH \
    REG_PATH=$REG_PATH \
    SCRIPT_DIR=$SCRIPT_DIR \
    cargo run --manifest-path "$SCRIPT_DIR/../../synstream-core/Cargo.toml" -r --bin main -- \
        --json $APP_GRAPH \
        --dylib $DYN_LIB \
        --inits \
        --workers $WORKERS \
        --system-threads $SYSTEM_THREADS \
        --batching-size $BATCHING_SIZE \
        --batching-limit $BATCHING_LIMIT \
        --output $OUTPUT \
        --max-runtime $RUNTIME \
        --slots $SLOTS \
        --max-streams $EXP_STREAMS \
        --timing $TIMING_FILE \
        $DEBUG \
        $RECORD

fi