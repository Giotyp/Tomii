#!/usr/bin/env bash
# Thin wrapper — delegates to run_bench.py which handles build + run.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec uv run python "$SCRIPT_DIR/run_bench.py" "$@"
