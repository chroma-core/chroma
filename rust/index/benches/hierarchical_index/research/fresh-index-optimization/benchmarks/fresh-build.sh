#!/usr/bin/env bash
set -euo pipefail
exec 9>/mnt/data/chroma-benchmark.lock
flock -n 9
r=/home/ubuntu/benchmark-data/fresh-index-20261008
mkdir -p "$r"
export PATH=/home/ubuntu/.cargo/bin:$PATH CARGO_TARGET_DIR=/home/ubuntu/chroma-benchmark/target
for variant in baseline parallel cadence; do
 cd "/home/ubuntu/chroma-fresh-$variant"
 git rev-parse HEAD > "$r/$variant.commit"
 cargo clean -p chroma-index --release
 cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run > "$r/$variant.build.log" 2>&1
 binary=$(find "$CARGO_TARGET_DIR/release/deps" -maxdepth 1 -name 'hierarchical_spann_profile_quantized-*' -type f -executable | head -1)
 test -n "$binary"
 cp "$binary" "$r/$variant-binary"
 sha256sum "$r/$variant-binary" > "$r/$variant.sha256"
 echo "$variant built $(date -u +%FT%TZ)"
done
echo 0 > "$r/build-exit-status"
