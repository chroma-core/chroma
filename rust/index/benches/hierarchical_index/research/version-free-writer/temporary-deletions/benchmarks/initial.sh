#!/bin/bash
set -euo pipefail
r=/mnt/data/temporary-deletions-20261020
p=/mnt/msmarco/version-free-writer-7927/venv/bin/python
exec 8>/mnt/data/chroma-benchmark.lock
flock 8
[ -s "$r/artifacts/binaries.sha256" ]
flock -u 8
"$p" "$r/run.py" reference 1 1
"$p" "$r/run.py" candidate 1 1
