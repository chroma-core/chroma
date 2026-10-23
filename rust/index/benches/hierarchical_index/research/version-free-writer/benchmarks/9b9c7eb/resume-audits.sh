#!/bin/bash
set -euo pipefail
r=/mnt/msmarco/version-free-writer-7927-9b9c7eb
exec 9>/mnt/data/chroma-benchmark.lock
flock -n 9
date -u +%FT%TZ > "$r/artifacts/audit-lock-acquired"
python3 "$r/resume-audits.py"
