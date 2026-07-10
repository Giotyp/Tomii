#!/usr/bin/env bash
# Regenerate static/img/favicon.ico from the favicon source SVGs.
#
# Frames: 16 px uses favicon-mark-16.svg (simplified 4-node diamond — the
# full mark smears at tab size); 32/48/64 px use favicon-mark.svg (the
# 6-node DAG with solid nodes). Requires ImageMagick (`convert`).
set -euo pipefail
cd "$(dirname "$0")/.."
IMG=static/img
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

convert -density 400 -background none "$IMG/favicon-mark-16.svg" \
    -resize 16x16 -gravity center -extent 16x16 "$TMP/16.png"
for s in 32 48 64; do
    convert -density 400 -background none "$IMG/favicon-mark.svg" \
        -resize ${s}x${s} -gravity center -extent ${s}x${s} "$TMP/$s.png"
done
convert "$TMP/16.png" "$TMP/32.png" "$TMP/48.png" "$TMP/64.png" "$IMG/favicon.ico"
echo "wrote $IMG/favicon.ico"
