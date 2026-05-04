#!/usr/bin/env bash
# Source foundation_notes/.env then run the Notion CDC tool.
# Pass through any subcommand + args, e.g.:
#   ./notion_cdc.sh listen --port 8787 --fetch-on-event
#   ./notion_cdc.sh register --url https://your-tunnel.example/notion
#   ./notion_cdc.sh reconcile --data-sources "ds_abc,ds_def"
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
if [[ -f ../.env ]]; then
  set -a
  # shellcheck disable=SC1091
  source ../.env
  set +a
fi
exec python3 "$SCRIPT_DIR/notion_cdc.py" "$@"
