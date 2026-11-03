#!/usr/bin/env bash
# install-skills.sh — copy Τομί skills into a project's .claude/skills/
#
# Prefer installing the plugin instead (see SKILLS/README.md):
#   /plugin install tomii-skills --marketplace Giotyp/Tomii
# This script is for agents/harnesses that read .claude/skills/ directly.
#
# Usage:
#   ./SKILLS/install-skills.sh            # installs to .claude/skills/ in CWD
#   ./SKILLS/install-skills.sh /some/dir  # installs to /some/dir/.claude/skills/

set -euo pipefail

SKILLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEST_ROOT="${1:-.}"
TARGET_DIR="$DEST_ROOT/.claude/skills"

# Collect skills: every SKILLS/skills/<name>/ directory with a SKILL.md
skills=()
for d in "$SKILLS_DIR"/skills/*/; do
    [[ -f "$d/SKILL.md" ]] || continue
    skills+=("$(basename "$d")")
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
    rm -rf "${TARGET_DIR:?}/$name"
    cp -r "$SKILLS_DIR/skills/$name" "$TARGET_DIR/$name"
    echo "  installed: /$name"
done

echo ""
echo "Done. Start or restart Claude Code, then type / to see the installed skills."
