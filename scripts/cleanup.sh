#!/usr/bin/env bash
# Cleanup helper for TraderMate repo
# Usage:
#   ./scripts/cleanup.sh        # dry-run (lists candidates)
#   ./scripts/cleanup.sh --apply  # actually delete candidates

set -euo pipefail

APPLY=false
if [[ "${1-}" == "--apply" ]]; then
  APPLY=true
fi

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

echo "Cleanup root: $ROOT_DIR"
echo "Mode: $( $APPLY && echo apply || echo dry-run )"

TMPFILE=$(mktemp /tmp/tradermate-cleanup.XXXXXX)
trap 'rm -f "$TMPFILE"' EXIT

echo "Scanning for candidate files and directories..."

# File patterns
find . \( -name '*.pyc' -o -name '*.pyo' -o -name '*~' -o -name '*.tmp' -o -name '*.bak' -o -name '*.orig' -o -name '*.swp' -o -name '.DS_Store' \) -type f -print >> "$TMPFILE" 2>/dev/null || true

# Common directories to remove
find . \( -name '__pycache__' -o -name '.pytest_cache' -o -name '.ipynb_checkpoints' -o -name 'coverage' -o -name 'dist' -o -name 'build' \) -type d -print >> "$TMPFILE" 2>/dev/null || true

# Editor / assistant temp files
find . -type f -name '.*.swp' -print >> "$TMPFILE" 2>/dev/null || true

sort -u "$TMPFILE" -o "$TMPFILE"

COUNT=$(wc -l < "$TMPFILE" || echo 0)
if [[ "$COUNT" -eq 0 ]]; then
  echo "No candidate files or directories found. Nothing to do."
  exit 0
fi

echo "Found $COUNT candidate(s):"
sed -n '1,200p' "$TMPFILE"

if ! $APPLY; then
  echo "\nDry-run complete. To remove these files, run:" 
  echo "  ./scripts/cleanup.sh --apply"
  exit 0
fi

echo "\nApplying cleanup..."
while IFS= read -r path; do
  if [[ -e "$path" ]]; then
    if [[ -d "$path" ]]; then
      echo "Removing directory: $path"
      rm -rf "$path"
    else
      echo "Removing file: $path"
      rm -f "$path"
    fi
  fi
done < "$TMPFILE"

echo "Cleanup applied. Removed $COUNT items."
exit 0
