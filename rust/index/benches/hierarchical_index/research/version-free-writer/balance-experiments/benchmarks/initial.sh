#!/bin/bash
set -euo pipefail
r=/mnt/msmarco/balance-experiments-20261020
exec 8>/mnt/data/chroma-benchmark.lock
flock 8
[ -s "$r/artifacts/binaries.sha256" ]
flock -u 8
p=/mnt/msmarco/version-free-writer-7927/venv/bin/python
for variant in reference transfers scheduling; do
"$p" "$r/run.py" "$variant" 1 1
"$p" "$r/summarize.py" "$r/artifacts"
done
