#!/bin/bash
# Download SNAP graph datasets for the COST benchmark.
#
# Downloads:
#   - Twitter (twitter-2010.txt): 41.7M nodes, 1.47B edges (~5.5 GB uncompressed)
#     Previously twitter_rv.net.gz (removed from SNAP); same dataset at new path.
#   - LiveJournal (soc-LiveJournal1.txt): 3.9M nodes, 68.9M edges (~700 MB)
#
# Set SNAP_DATA_DIR to override the default download location.
# The processed edge lists (one "src dst" per line, no comments) are written to:
#   $DATA_DIR/twitter.txt
#   $DATA_DIR/livejournal.txt

set -euo pipefail

DATA_DIR="${SNAP_DATA_DIR:-/data/snap}"
mkdir -p "$DATA_DIR"

TWITTER_URL="https://snap.stanford.edu/data/twitter-2010.txt.gz"
LJ_URL="https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz"

download_and_strip() {
    local url="$1"
    local out="$2"
    local gz="${out}.gz"

    if [ -f "$out" ]; then
        echo "[skip] $out already exists"
        return
    fi

    echo "Downloading $url ..."
    curl -fL --progress-bar "$url" -o "$gz"

    echo "Decompressing to $out ..."
    gunzip -c "$gz" | grep -v '^#' > "$out"
    rm -f "$gz"

    echo "Done: $out ($(wc -l < "$out") edges)"
}

download_and_strip "$TWITTER_URL"   "$DATA_DIR/twitter.txt"
download_and_strip "$LJ_URL"        "$DATA_DIR/livejournal.txt"

echo ""
echo "Datasets ready in $DATA_DIR:"
ls -lh "$DATA_DIR"/*.txt 2>/dev/null || true
