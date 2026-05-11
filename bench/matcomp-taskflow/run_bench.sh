#!/usr/bin/env bash
# Run tf_matcomp under the polyglot regime:
#   N=200 items, buf=100, W=4 workers, S=4 slots, 10 warmup + 30 measured streams
#   Workers pinned to cores 3-6 (NUMA node 0), matching the polyglot run setup.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"
BINARY="${BUILD_DIR}/tf_matcomp"

# Build if needed
if [[ ! -f "${BINARY}" ]]; then
    echo "[run_bench.sh] Building tf_matcomp..."
    cmake -B "${BUILD_DIR}" -S "${SCRIPT_DIR}" -DCMAKE_BUILD_TYPE=Release
    cmake --build "${BUILD_DIR}" -j"$(nproc)"
fi

OUTPUT="${1:-${SCRIPT_DIR}/tf_matcomp_polyglot.csv}"

echo "[run_bench.sh] Running polyglot regime: N=200 buf=100 slots=4 workers=4 streams=30 warmup=10"
"${BINARY}" \
    --n 200 \
    --buf 100 \
    --slots 4 \
    --workers 4 \
    --streams 30 \
    --warmup 10 \
    --pin 3 \
    --output "${OUTPUT}"
