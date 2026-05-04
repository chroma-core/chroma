#!/usr/bin/env bash
# Source foundation_notes/.env then run notion_internal_dump.py.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
if [[ -f ../.env ]]; then
  set -a
  # shellcheck disable=SC1091
  source ../.env
  set +a
fi
exec python3 "$SCRIPT_DIR/notion_internal_dump.py" "$@"
