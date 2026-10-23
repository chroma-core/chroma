#!/bin/bash
set -euo pipefail
r=/mnt/data/versioned-finer-20261020
p=/mnt/msmarco/version-free-writer-7927/venv/bin/python
"$p" "$r/run.py" versioned 3 1
"$p" "$r/run.py" versionfree 3 1
"$p" "$r/run.py" versionfree 3 2
"$p" "$r/run.py" versioned 3 2
