#!/bin/bash
set -euo pipefail
r=/mnt/msmarco/version-free-followup-20261020
exec 9>/mnt/data/chroma-benchmark.lock
flock -n 9
export PATH=/home/ubuntu/.cargo/bin:$PATH CARGO_HOME=/mnt/msmarco/version-free-writer-7927/cargo-home CARGO_TARGET_DIR=/mnt/msmarco/version-free-writer-7927/target TMPDIR="$r/tmp" RUSTUP_TOOLCHAIN=1.92.0 RUSTFLAGS='-C target-cpu=native'
mkdir "$r/instrumented"
tar -xf "$r/instrumented.tar" -C "$r/instrumented"
{ date -u; rustc --version; cargo --version; uname -a; lscpu; free -h; df -h; } > "$r/artifacts/environment.txt"
printf '%s\n' 8581e80920036247715da0b40a4d0a4702118122 > "$r/artifacts/instrumented.sha"
sha256sum "$r/instrumented.tar" > "$r/artifacts/instrumented-archive.sha256"
cd "$r/instrumented"
/usr/bin/time -v -o "$r/artifacts/build-instrumented.time" cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run -j32 > "$r/artifacts/build-instrumented.log" 2>&1
binary=$(find "$CARGO_TARGET_DIR/release/deps" -maxdepth 1 -name 'hierarchical_spann_profile_quantized-*' -type f -executable | head -1)
cp "$binary" "$r/artifacts/instrumented-binary"
sha256sum "$r/artifacts/instrumented-binary" > "$r/artifacts/instrumented-binary.sha256"
