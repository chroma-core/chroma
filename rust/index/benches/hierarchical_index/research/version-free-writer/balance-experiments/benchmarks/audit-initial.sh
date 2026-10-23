#!/bin/bash
set -euo pipefail
r=/mnt/msmarco/balance-experiments-20261020
exec 8>/mnt/data/chroma-benchmark.lock
flock 8
[ -s "$r/artifacts/wikipedia-scheduling-1m-1.result.json" ]
flock -u 8
for variant in reference transfers scheduling; do bash "$r/audit.sh" "$variant"; done
