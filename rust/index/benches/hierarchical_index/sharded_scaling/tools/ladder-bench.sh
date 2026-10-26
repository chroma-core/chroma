#!/bin/bash
# Run the matched-recall grid on every FineWeb 100M ladder size as its saved
# generation appears on the builder machine. One reader process per size,
# pinned to 28 cores of one socket, all owners local, CPU latency held, so
# results compare with Phase 0. The 10M, 30M and 100M sizes also record each
# query's leaves for fan-out counts.
# Usage: ./ladder-bench.sh [SIZES_IN_MILLIONS...]
set -u
cd "$(dirname "$0")"
B=/home/ubuntu/spann-two-machine-v1/reader-dev/bin/ladder-reader-1633b24ad2a7
C=1633b24ad
SIZES=${*:-1 2 3 5 10 20 30 40 50 60 70 80 90 100}
for m in $SIZES; do
  n=$(( m * 1000000 ))
  until ./bsh "test -f /data/ladder/save-$n/owner1/result.json" 2>/dev/null; do sleep 60; done
  status=$(./bsh "python3 -c \"import json;print(json.load(open('/data/ladder/save-$n/owner1/result.json'))['status'])\"")
  if [ "$status" != construction_save_and_fresh_reader_validated ]; then echo "!! size $n save status $status"; continue; fi
  mkdir -p ladder/$n
  ./bsh "python3 -c \"import json;print(json.dumps(json.load(open('/data/ladder/save-$n/owner1/result.json'))['saved_descriptor'],indent=2))\"" > ladder/$n/generation.json
  ./bsh "cat /data/ladder/save-$n/storage.json" > ladder/$n/storage.json
  run() { label=$1; config=$2; shift 2
    echo "== $label $(date -u +%H:%M:%S)"
    python3 run-bench.py --pair builder --label $label --fixture ladder-${m}m \
      --generation ladder/$n/generation.json --storage ladder/$n/storage.json \
      --remote-input queries.jsonl=/data/queries/dev1000.jsonl \
      --remote-input groundtruth.jsonl=/data/gt/merged/dev-$n.exact.jsonl \
      --config $config --a $B --commit-a $C --order A --reader owners \
      --taskset 0-26,27 --hold-cpu-latency 10 --bench-arg=--posting-cache-bytes=68719476736 "$@" \
      || echo "!! $label failed"
  }
  run lad-grid-${m}m configs/phase0-grid.json
  case $m in 10|30|100) run lad-fanout-${m}m configs/phase0-fanout.json --bench-arg=--record-leaves \
    --bench-arg=--dump-tree "--bench-arg={out}/tree.jsonl";; esac
done
echo "== done $(date -u +%H:%M:%S)"
