#!/usr/bin/env bash
set -euo pipefail
r=/home/ubuntu/benchmark-data/large-index-balancing-investigation
exec 9>/mnt/data/chroma-benchmark.lock
flock 9
cd /home/ubuntu/chroma-large-index-balancing
binary=$(find /home/ubuntu/chroma-benchmark/target/release/deps -maxdepth 1 -name 'hierarchical_spann_profile_quantized-*' -type f -executable | head -1)
cp "$binary" "$r/component-probe-binary"
sha256sum "$r/component-probe-binary" > "$r/component-probe-binary.sha256"
git diff > "$r/component-probe.diff"
for size in 9 40; do
  for cache in 1073741824 8589934592; do
    export CHROMA_BALANCE_COMPONENT_FIXTURE=/mnt/data/large-index-investigation-components-${size}m
    export CHROMA_COMPONENT_CACHE_BYTES=$cache
    export CHROMA_COMPONENT_LEAF_COUNT=200
    /usr/bin/time -v -o "$r/components-${size}m-${cache}.time" timeout 300 "$r/component-probe-binary" > "$r/components-${size}m-${cache}.log" 2>&1
  done
done
date -u +%FT%TZ > "$r/components-finished"
