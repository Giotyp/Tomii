#!/bin/sh

# Clear any previously set environment variables from parent shell
unset FUNC_PATH WRAP_PATH REG_PATH

# Get current script directory (absolute path)
SCRIPT_PATH=$(readlink -f "$0")
SCRIPT_DIR=$(dirname "$SCRIPT_PATH")
BIN_DIR="$SCRIPT_DIR/../../synstream-core"

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

# Cleanup flag: set to 1 to clean and recompile, 0 to skip
CLEANUP=1
# Configuration Parameters
WORKERS=2
RUNTIME=60
SLOTS=2
EXP_STREAMS=1
EXCLUDE_STREAMS=0 # exclude streams from timing stats
OUTPUT="$SCRIPT_DIR/out.txt"
TIMING_FILE="$SCRIPT_DIR/timing.txt"
SYSTEM_THREADS=3
BATCHING_SIZE=1
BATCHING_LIMIT=10
# Set to "--inits" to enable initializations printing
INITS="--inits"
# Set to "--slot-priority" to enable slot priority
SLOT_PRIORITY=""
# Set to "--debug" to enable debug mode
DEBUG=""
# Set to "--record" to enable scheduler recording
RECORD="--record"

export FUNC_PATH=$FUNC_PATH
export WRAP_PATH=$WRAP_PATH
export REG_PATH=$REG_PATH
export SCRIPT_DIR=$SCRIPT_DIR

if [ $CLEANUP -eq 1 ]; then
    # Clean and compile SynStream
    cargo clean -p synstream-core
    cargo build -r -p synstream-core
    cargo build -r -p synstream-types

    # Compile library with the specific paths
    cargo clean --manifest-path "$SCRIPT_DIR/Cargo.toml"
    cargo build --manifest-path "$SCRIPT_DIR/Cargo.toml" -r
fi

# Remove old output and timing files if they exist
rm -f $OUTPUT
rm -f $TIMING_FILE

# Run the main binary from synstream-core
cargo run --manifest-path "$BIN_DIR/Cargo.toml" -r --bin main -- \
    --json $APP_GRAPH \
    --dylib $DYN_LIB \
    --workers $WORKERS \
    --system-threads $SYSTEM_THREADS \
    --batching-size $BATCHING_SIZE \
    --batching-limit $BATCHING_LIMIT \
    --output $OUTPUT \
    --max-runtime $RUNTIME \
    --slots $SLOTS \
    --max-streams $EXP_STREAMS \
    --exclude-streams $EXCLUDE_STREAMS \
    --timing $TIMING_FILE \
    $RECORD \
    $DEBUG \
    $INITS $SLOT_PRIORITY