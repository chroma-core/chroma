#!/usr/bin/env bash
# Source foundation_notes/.env then run the Notion bulk dumper.
# Pass through any args, e.g.:
#   ./notion_bulk_dump.sh setup-token
#   ./notion_bulk_dump.sh dump --target-seconds 60
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
if [[ -f ../.env ]]; then
  set -a
  # shellcheck disable=SC1091
  source ../.env
  set +a
fi
exec python3 "$SCRIPT_DIR/notion_bulk_dump.py" "$@"
