#!/usr/bin/env bash
set -euo pipefail
readonly control=/mnt/spann/full-record-million-runtime-v3/epoch7
cd "$control"
# Closed scalar reports only. Never traverse store blocks or source vectors.
files=(build/result.json build/reuse.json spec.json plan.json shutdown-proof.json source-manifest.json)
for name in input-binding.json supervisor-process.json measurement/process.json measurement/summary.json measurement/stderr measurement/stdout; do
    if test -f "$name"; then files+=("$name"); fi
done
for name in "${files[@]}"; do
    test ! -L "$name"
    test "$(stat -c %s "$name")" -le 16777216
done
tar -cf - "${files[@]}"
