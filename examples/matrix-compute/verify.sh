#!/usr/bin/env bash
# Verifier for the matrix-compute example.
# Builds the perfval validation binary and runs it to produce validation.txt.
# Exits 0 and prints PASS on success, exits 1 and prints FAIL on any error.

set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$HERE/../.."
VALIDATION_OUT="$HERE/validation.txt"

# Build the validation binary (incremental; fast if nothing changed)
if ! cargo build --release --manifest-path "$HERE/Cargo.toml" --bin perfval \
        2>&1; then
    echo "FAIL: cargo build failed"
    exit 1
fi

# Remove any stale output so we can confirm it was freshly written
rm -f "$VALIDATION_OUT"

# Run; binary writes validation.txt to its CARGO_MANIFEST_DIR (HERE)
if ! "$REPO_ROOT/target/release/perfval" 2>&1; then
    echo "FAIL: perfval exited non-zero"
    exit 1
fi

if [[ ! -s "$VALIDATION_OUT" ]]; then
    echo "FAIL: validation.txt was not created or is empty"
    exit 1
fi

echo "PASS"
