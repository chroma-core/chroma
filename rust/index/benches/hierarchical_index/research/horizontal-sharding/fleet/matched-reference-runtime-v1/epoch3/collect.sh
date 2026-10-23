#!/usr/bin/env bash
set -euo pipefail
root=/home/ubuntu/spann-matched-reference-v1/epoch1
cd "$root/control"
files=(integrity-summary.json native-command.json source-manifest.json baseline-source-manifest.json SOURCE-CONTRACT.json PROTOCOL.json build/result.json build/process.json build/stdout.jsonl build/stderr)
while IFS= read -r -d '' file; do files+=("$file"); done < <(find measurement -maxdepth 2 -type f -size -16M -print0)
for file in "${files[@]}"; do test -f "$file"; test "$(stat -c %s "$file")" -le 16777216; done
tar -cf "$root/small-evidence.tar" "${files[@]}"
stat -c %s "$root/small-evidence.tar"
cat integrity-summary.json
