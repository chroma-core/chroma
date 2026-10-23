#!/bin/bash
set -euo pipefail
r=/mnt/msmarco/balance-experiments-20261020
p=/mnt/msmarco/version-free-writer-7927/venv/bin/python
for n in $(seq 1 120); do
if [ -s "$r/artifacts/wikipedia-reference-9m-2.audit-result.json" ]; then break; fi
sleep 5
done
"$p" "$r/audit-extended.py"
"$p" "$r/summarize.py" "$r/artifacts"
"$p" "$r/analyze.py" "$r/artifacts"
"$p" "$r/resources9m.py" "$r/artifacts"
{ date -u; free -h; df -h /mnt/data /mnt/msmarco; sha256sum /mnt/msmarco/version-free-followup-20261020/wikipedia-diagnostic-9m-1/snapshots/8000000/checkpoint.json; } > "$r/artifacts/final-host.txt"
