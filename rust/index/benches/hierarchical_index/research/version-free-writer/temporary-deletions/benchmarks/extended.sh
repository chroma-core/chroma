#!/bin/bash
set -euo pipefail
r=/mnt/data/temporary-deletions-20261020
p=/mnt/msmarco/version-free-writer-7927/venv/bin/python
"$p" "$r/run.py" candidate 3 1
"$p" "$r/run.py" reference 3 1
bash "$r/audit-initial.sh"
python3 "$r/summarize.py" "$r/artifacts"
python3 "$r/analyze.py" "$r/artifacts"
