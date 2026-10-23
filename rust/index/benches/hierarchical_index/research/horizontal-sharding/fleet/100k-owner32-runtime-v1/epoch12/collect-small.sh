#!/usr/bin/env bash
set -euo pipefail
readonly root=/home/ubuntu/spann-owner32-build-v6/local-reassignment-epoch1
cd "$root"
files=(host-admission.json shutdown-proof.json)
for arm in build-control off on; do
 for name in source-manifest.json PLAN-TEMPLATE.json SPEC-TEMPLATE.json paired-result.json paired-build-validity.json shared-build-provenance.json input-binding.json spec.json plan.json build/result.json build/process.json build/stderr build/source-refresh.json build/stdout.jsonl supervisor-process.json measurement/process.json measurement/summary.json measurement/stderr measurement/stdout; do
  path="$arm/$name"
  if test -f "$path"; then test ! -L "$path"; test "$(stat -c %s "$path")" -le 16777216; files+=("$path"); fi
 done
 native=/home/ubuntu/spann-owner32-local-reassignment-science-v1/epoch1/$arm/index
 if test -d "$native"; then
  mkdir "$arm/closed-native-metadata"
  for name in result.json snapshot64.json; do
   if test -f "$native/$name"; then test ! -L "$native/$name"; test "$(stat -c %s "$native/$name")" -le 16777216; cp "$native/$name" "$arm/closed-native-metadata/$name"; files+=("$arm/closed-native-metadata/$name"); fi
  done
 fi
done
df -B1 / > storage-after.txt
files+=(storage-after.txt)
ps -eo pid,pgid,args | grep -E '[r]un_pair.py|[s]upervise.py|[h]ierarchical_spann_shard|[b]uild/native' > survivors.txt || true
files+=(survivors.txt)
tar -cf - "${files[@]}"
