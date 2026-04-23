#!/usr/bin/env bash
# Thin wrapper — delegates to run_bench.py which owns the full DAG definition.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$(dirname "$SCRIPT_DIR")/$(basename "$SCRIPT_DIR")/../../"   # repo root
uv run python "$SCRIPT_DIR/run_bench.py" "$@"
