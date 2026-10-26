#!/bin/bash
# Phase 0 of the 100B scaling study: matched-recall grids at FineWeb 1M, 3M and
# 10M, level-cap sweeps, and a 10M fan-out run with recorded leaves. One reader
# process per run on the query machine, all owners local, CPU latency held.
set -u
cd "$(dirname "$0")"
B=/home/ubuntu/spann-two-machine-v1/reader-dev/bin/phase0-6f278224b0e2
C=6f278224b
V2=(--generation rebuilds/v2-10m-c/generation.json --storage rebuilds/v2-10m-c/storage.json)
run() { label=$1; shift; echo "== $label $(date -u +%H:%M:%S)"; python3 run-bench.py --label "$label" --a $B --commit-a $C --order A --reader owners --hold-cpu-latency 10 "$@" || echo "!! $label failed"; }
run p0-fanout-10m --fixture 10m "${V2[@]}" --config configs/phase0-fanout.json --bench-arg=--record-leaves --bench-arg=--dump-tree '--bench-arg={out}/tree.jsonl'
run p0-grid-1m --fixture 1m --config configs/phase0-grid.json
run p0-grid-3m --fixture 3m --config configs/phase0-grid.json
run p0-grid-10m --fixture 10m "${V2[@]}" --config configs/phase0-grid.json
run p0-levelcap-1m --fixture 1m --config configs/phase0-levelcap-1m.json
run p0-levelcap-3m --fixture 3m --config configs/phase0-levelcap-3m.json
run p0-levelcap-10m --fixture 10m "${V2[@]}" --config configs/phase0-levelcap-10m.json
echo "== done $(date -u +%H:%M:%S)"
