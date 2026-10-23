#!/bin/bash
set -euo pipefail
r=/mnt/data/versioned-finer-20261020
while [ ! -f "$r/artifacts/wikipedia-versioned-3m-2.result.json" ]; do sleep 5; done
/mnt/msmarco/version-free-writer-7927/venv/bin/python "$r/audit.py" 3
