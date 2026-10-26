#!/bin/bash
# Phase 0 adaptive-beam sweep: distance-ratio (tau) beam at the leaf level only,
# internal levels searched in full, at FineWeb 1M, 3M and 10M.
set -u
cd "$(dirname "$0")"
B=/home/ubuntu/spann-two-machine-v1/reader-dev/bin/phase0-8ff6251651a2
C=8ff625165
V2=(--generation rebuilds/v2-10m-c/generation.json --storage rebuilds/v2-10m-c/storage.json)
run() { label=$1; shift; echo "== $label $(date -u +%H:%M:%S)"; python3 run-bench.py --label "$label" --a $B --commit-a $C --order A --reader owners --hold-cpu-latency 10 "$@" || echo "!! $label failed"; }
run p0-tau-1m --fixture 1m --config configs/phase0-tau-1m.json
run p0-tau-3m --fixture 3m --config configs/phase0-tau-3m.json
run p0-tau-10m --fixture 10m "${V2[@]}" --config configs/phase0-tau-10m.json
echo "== done $(date -u +%H:%M:%S)"
