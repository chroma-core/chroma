#!/bin/bash
set -euo pipefail
r=/mnt/data/temporary-deletions-20261020
exec 9>/mnt/data/chroma-benchmark.lock
flock -n 9
cp /mnt/msmarco/balance-experiments-20261020/artifacts/scheduling-audit-binary "$r/artifacts/reference-audit-binary"
sha256sum "$r/artifacts/reference-audit-binary" > "$r/artifacts/reference-audit-binary.sha256"
p=/mnt/msmarco/version-free-writer-7927/venv/bin/python
"$p" "$r/audit.py" reference
"$p" "$r/audit.py" candidate
