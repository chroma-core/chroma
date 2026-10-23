#!/usr/bin/env bash
set -euo pipefail
out=/home/ubuntu/benchmark-data/reassignment-experiments-20261012
# The build releases the machine lock before these sequential replays acquire it.
while [[ ! -f "$out/build.status" ]]; do sleep 15; done
test "$(cat "$out/build.status")" = 0
export TARGET_CHECKPOINT=10
export RETAIN_REPLAYS=baseline-3,deferred-3,batched-3
exec bash "$out/run-replays.sh"
