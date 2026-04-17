#!/usr/bin/env bash
# install-skills.sh — install Τομί skills as Claude Code slash commands
#
# Usage:
#   ./SKILLS/install-skills.sh            # installs to .claude/skills/ in CWD
#   ./SKILLS/install-skills.sh /some/dir  # installs to /some/dir/.claude/skills/

set -euo pipefail

SKILLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEST_ROOT="${1:-.}"
TARGET_DIR="$DEST_ROOT/.claude/skills"

# Collect skills (all .md files except README.md and this script's dir)
skills=()
for f in "$SKILLS_DIR"/*.md; do
    name="$(basename "$f" .md)"
    [[ "$name" == "README" ]] && continue
    skills+=("$name")
done

echo "Τομί skill installer"
echo "========================="
echo "Install location: $TARGET_DIR"
echo ""
echo "Skills to install:"
for name in "${skills[@]}"; do
    if [[ -f "$TARGET_DIR/$name/SKILL.md" ]]; then
        echo "  /$name  (will overwrite existing)"
    else
        echo "  /$name"
    fi
done
echo ""
read -r -p "Proceed? [y/N] " confirm
[[ "$confirm" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 0; }

echo ""
mkdir -p "$TARGET_DIR"
for name in "${skills[@]}"; do
    mkdir -p "$TARGET_DIR/$name"
    cp "$SKILLS_DIR/$name.md" "$TARGET_DIR/$name/SKILL.md"
    echo "  installed: /$name"
done

echo ""
echo "Done. Start or restart Claude Code, then type / to see the installed skills."
