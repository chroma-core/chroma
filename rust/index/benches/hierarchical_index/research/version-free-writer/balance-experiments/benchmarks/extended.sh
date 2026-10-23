#!/bin/bash
set -euo pipefail
r=/mnt/msmarco/balance-experiments-20261020
p=/mnt/msmarco/version-free-writer-7927/venv/bin/python
for n in $(seq 1 120); do
if [ -s "$r/artifacts/wikipedia-scheduling-1m-1.audit-result.json" ]; then break; fi
sleep 5
done
"$p" -c 'import json; from pathlib import Path; a=Path("/mnt/msmarco/balance-experiments-20261020/artifacts"); assert all(json.loads((a/f"wikipedia-{v}-1m-1.audit-result.json").read_text())["returncode"]==0 for v in ["reference","transfers","scheduling"])'
for variant in transfers reference; do
"$p" "$r/run.py" "$variant" 3 1
flock /mnt/data/chroma-benchmark.lock "$p" "$r/audit.py" "$variant"
done
"$p" "$r/summarize.py" "$r/artifacts"
"$p" "$r/analyze.py" "$r/artifacts"
"$p" "$r/prepare9m.py"
for variant in reference scheduling; do
"$p" "$r/run.py" "$variant" 9 1
flock /mnt/data/chroma-benchmark.lock "$p" "$r/audit.py" "$variant"
"$p" "$r/summarize.py" "$r/artifacts"
"$p" "$r/analyze.py" "$r/artifacts"
done
