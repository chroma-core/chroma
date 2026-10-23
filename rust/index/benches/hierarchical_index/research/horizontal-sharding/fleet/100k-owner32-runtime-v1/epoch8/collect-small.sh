#!/usr/bin/env bash
set -euo pipefail
readonly control=/home/ubuntu/spann-owner32-build-v6/queue-cache4-epoch1
readonly native=/home/ubuntu/spann-owner32-queue-cache-science-v1/epoch1/index
cd "$control"
files=()
for name in reuse-result.json reuse-provenance.json source-manifest.json PLAN-TEMPLATE.json SPEC-TEMPLATE.json input-binding.json host-admission.json spec.json plan.json build/result.json supervisor-process.json measurement/process.json measurement/summary.json measurement/stderr measurement/stdout; do
 if test -f "$name"; then test ! -L "$name"; test "$(stat -c %s "$name")" -le 16777216; files+=("$name"); fi
done
mkdir closed-native-metadata
for name in result.json snapshot64.json; do
 if test -f "$native/$name"; then test ! -L "$native/$name"; test "$(stat -c %s "$native/$name")" -le 16777216; cp "$native/$name" "closed-native-metadata/$name"; files+=("closed-native-metadata/$name"); fi
done
df -B1 / > storage-after.txt
files+=(storage-after.txt)
ps -eo pid,pgid,args | grep -E '[r]un_queue.py|[s]upervise.py|[h]ierarchical_spann_shard|[b]uild/native' > survivors.txt || true
files+=(survivors.txt)
tar -cf - "${files[@]}"
