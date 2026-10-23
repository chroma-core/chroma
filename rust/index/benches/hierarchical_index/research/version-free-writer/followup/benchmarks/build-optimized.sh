#!/bin/bash
set -euo pipefail
r=/mnt/msmarco/version-free-followup-20261020
exec 9>/mnt/data/chroma-benchmark.lock
flock -n 9
export PATH=/home/ubuntu/.cargo/bin:$PATH CARGO_HOME=/mnt/msmarco/version-free-writer-7927/cargo-home CARGO_TARGET_DIR=/mnt/msmarco/version-free-writer-7927/target TMPDIR="$r/tmp" RUSTUP_TOOLCHAIN=1.92.0 RUSTFLAGS='-C target-cpu=native'
mkdir "$r/optimized"
tar -xf "$r/optimized.tar" -C "$r/optimized"
{ date -u; rustc --version; cargo --version; uname -a; lscpu; free -h; df -h; } > "$r/artifacts/environment-optimized.txt"
printf '%s\n' fc39a953806ef1d9cb02f1985b2fcf1228e5cdb8 > "$r/artifacts/optimized.sha"
sha256sum "$r/optimized.tar" > "$r/artifacts/optimized-archive.sha256"
cd "$r/optimized"
/usr/bin/time -v -o "$r/artifacts/build-optimized.time" cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run -j32 > "$r/artifacts/build-optimized.log" 2>&1
binary=$(find "$CARGO_TARGET_DIR/release/deps" -maxdepth 1 -name 'hierarchical_spann_profile_quantized-*' -type f -executable | head -1)
cp "$binary" "$r/artifacts/optimized-binary"
sha256sum "$r/artifacts/optimized-binary" > "$r/artifacts/optimized-binary.sha256"
