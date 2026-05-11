#!/usr/bin/env bash
# Oracle Taskflow reference run.
# Usage: bash run.sh <num_threads> <num_streams> <exclude_streams>
# This script builds (if needed) and runs the oracle Taskflow implementation.

set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

THREADS="${1:-4}"
STREAMS="${2:-7}"
EXCLUDE="${3:-3}"

mkdir -p "$HERE/build"
cmake -S "$HERE" -B "$HERE/build" \
    ${TASKFLOW_ROOT:+-DTASKFLOW_DIR="$TASKFLOW_ROOT"} \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_VERBOSE_MAKEFILE=OFF \
    > /dev/null 2>&1
cmake --build "$HERE/build" -- -j4 > /dev/null 2>&1

"$HERE/build/sensor_pipeline" "$THREADS" "$STREAMS" "$EXCLUDE"
