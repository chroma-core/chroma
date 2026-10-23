#!/usr/bin/env bash
set -euo pipefail
readonly control=/home/ubuntu/spann-owner32-build-v6/epoch4
readonly native=/mnt/spann-owner32-scratch/epoch4/index
mkdir "$control/closed-native-metadata"
for name in result.json snapshot64.json; do
    if test -f "$native/$name"; then
        test ! -L "$native/$name"
        test "$(stat -c %s "$native/$name")" -le 16777216
        cp "$native/$name" "$control/closed-native-metadata/$name"
    fi
done
du -s -B1 /home/ubuntu/spann-owner32-build-v6/target > "$control/storage-after.txt"
df -B1 / /mnt/spann-owner32-scratch >> "$control/storage-after.txt"
cd "$control"
files=(input-binding.json host-admission.json spec.json plan.json shutdown-proof.json source-manifest.json build/result.json build/process.json supervisor-process.json measurement/process.json measurement/summary.json measurement/stderr measurement/stdout storage-after.txt)
for name in closed-native-metadata/result.json closed-native-metadata/snapshot64.json; do
    if test -f "$name"; then files+=("$name"); fi
done
for name in "${files[@]}"; do test ! -L "$name"; test "$(stat -c %s "$name")" -le 16777216; done
tar -cf - "${files[@]}"
