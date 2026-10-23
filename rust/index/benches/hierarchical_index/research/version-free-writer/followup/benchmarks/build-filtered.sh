#!/bin/bash
set -euo pipefail
r=/mnt/msmarco/version-free-followup-20261020
exec 9>/mnt/data/chroma-benchmark.lock
flock -n 9
export PATH=/home/ubuntu/.cargo/bin:$PATH CARGO_HOME=/mnt/msmarco/version-free-writer-7927/cargo-home CARGO_TARGET_DIR=/mnt/msmarco/version-free-writer-7927/target TMPDIR="$r/tmp" RUSTUP_TOOLCHAIN=1.92.0 RUSTFLAGS='-C target-cpu=native'
mkdir "$r/filtered"
tar -xf "$r/filtered.tar" -C "$r/filtered"
{ date -u; rustc --version; cargo --version; uname -a; lscpu; free -h; df -h; } > "$r/artifacts/environment-filtered.txt"
printf '%s\n' eea4ada6f21554adb535d1ffdea95f74cb76650b > "$r/artifacts/filtered.sha"
sha256sum "$r/filtered.tar" > "$r/artifacts/filtered-archive.sha256"
cd "$r/filtered"
/usr/bin/time -v -o "$r/artifacts/build-filtered.time" cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run -j32 > "$r/artifacts/build-filtered.log" 2>&1
binary=$(find "$CARGO_TARGET_DIR/release/deps" -maxdepth 1 -name 'hierarchical_spann_profile_quantized-*' -type f -executable | head -1)
cp "$binary" "$r/artifacts/filtered-binary"
sha256sum "$r/artifacts/filtered-binary" > "$r/artifacts/filtered-binary.sha256"
