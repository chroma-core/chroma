#!/bin/bash
set -euo pipefail
r=/mnt/data/temporary-deletions-20261020
p=/mnt/msmarco/version-free-writer-7927/venv/bin/python
"$p" "$r/run.py" reference 9 1
"$p" "$r/run.py" candidate 9 1
bash "$r/audit-initial.sh"
"$p" "$r/audit-extended.py" 9
python3 "$r/summarize.py" "$r/artifacts"
python3 "$r/analyze.py" "$r/artifacts"
python3 "$r/resources9m.py" "$r/artifacts"
