#!/usr/bin/env bash
set -euo pipefail
r=/home/ubuntu/benchmark-data/wikipedia-10m-scaling-baseline-20261008
while [[ ! -f "$r/exit-status" ]]; do sleep 10; done
test "$(cat "$r/exit-status")" = 0
bash "$r/read-baseline-scaling.sh"
echo "$(date -u +%FT%TZ) Reader scaling complete"
bash "$r/audit.sh"
echo "$(date -u +%FT%TZ) Exact persisted posting audit complete"
