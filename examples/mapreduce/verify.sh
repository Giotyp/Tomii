#!/usr/bin/env bash
# Verifier for the mapreduce example.
# Compares result.txt against result.golden.txt byte-for-byte.
# Exits 0 and prints PASS on success, exits 1 and prints FAIL on mismatch.

set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULT="$HERE/result.txt"
GOLDEN="$HERE/result.golden.txt"

if [[ ! -f "$RESULT" ]]; then
    echo "FAIL: result.txt not found (run the benchmark first)"
    exit 1
fi

if [[ ! -f "$GOLDEN" ]]; then
    echo "FAIL: result.golden.txt not found"
    exit 1
fi

if diff -q "$RESULT" "$GOLDEN" > /dev/null 2>&1; then
    echo "PASS"
else
    echo "FAIL: result.txt differs from golden"
    diff "$RESULT" "$GOLDEN" || true
    exit 1
fi
