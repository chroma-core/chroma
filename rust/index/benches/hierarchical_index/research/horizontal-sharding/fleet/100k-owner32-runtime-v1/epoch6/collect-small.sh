#!/usr/bin/env bash
set -euo pipefail
readonly root=/home/ubuntu/spann-owner32-build-v6/epoch6
cd "$root"
files=(paired-result.json host-admission.json shutdown-proof.json)
for arm in baseline batched; do
 for name in source-manifest.json PLAN-TEMPLATE.json SPEC-TEMPLATE.json baseline-reuse-provenance.json input-binding.json spec.json plan.json build/result.json build/process.json build/stderr supervisor-process.json measurement/process.json measurement/summary.json measurement/stderr measurement/stdout; do
  path="$arm/$name"
  if test -f "$path"; then test ! -L "$path"; test "$(stat -c %s "$path")" -le 16777216; files+=("$path"); fi
 done
 native=/home/ubuntu/spann-owner32-paired-science-v1/epoch6/$arm/index
 if test -d "$native"; then
  mkdir "$arm/closed-native-metadata"
  for name in result.json snapshot64.json; do
   if test -f "$native/$name"; then test ! -L "$native/$name"; test "$(stat -c %s "$native/$name")" -le 16777216; cp "$native/$name" "$arm/closed-native-metadata/$name"; files+=("$arm/closed-native-metadata/$name"); fi
  done
 fi
done
ps -eo pid,pgid,args | grep -E '[r]un_pair.py|[s]upervise.py|[h]ierarchical_spann_shard|[b]uild/native' > survivors.txt || true
files+=(survivors.txt)
for name in "${files[@]}"; do test -f "$name"; test ! -L "$name"; done
tar -cf - "${files[@]}"
