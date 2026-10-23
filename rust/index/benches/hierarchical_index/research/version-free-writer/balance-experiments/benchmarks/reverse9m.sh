#!/bin/bash
set -euo pipefail
r=/mnt/msmarco/balance-experiments-20261020
p=/mnt/msmarco/version-free-writer-7927/venv/bin/python
export BALANCE_BENCH_OUTPUT_ROOT=/mnt/data/balance-experiments-repeats-20261020
for variant in scheduling reference; do
"$p" "$r/run.py" "$variant" 9 2
flock /mnt/data/chroma-benchmark.lock "$p" "$r/audit.py" "$variant"
"$p" "$r/summarize.py" "$r/artifacts"
"$p" "$r/analyze.py" "$r/artifacts"
"$p" "$r/resources9m.py" "$r/artifacts"
done
