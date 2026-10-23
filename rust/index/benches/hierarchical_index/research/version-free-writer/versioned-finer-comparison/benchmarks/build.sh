#!/bin/bash
set -euo pipefail
r=/mnt/data/versioned-finer-20261020
exec 9>/mnt/data/chroma-benchmark.lock
flock -n 9
export PATH=/home/ubuntu/.cargo/bin:$PATH CARGO_HOME=/mnt/msmarco/version-free-writer-7927/cargo-home CARGO_TARGET_DIR=/mnt/msmarco/version-free-writer-7927/target TMPDIR="$r/tmp" RUSTUP_TOOLCHAIN=1.92.0 RUSTFLAGS='-C target-cpu=native'
mkdir -p "$r/artifacts" "$r/tmp" "$r/versioned"
{ date -u; rustc --version; cargo --version; uname -a; lscpu; free -h; df -h; } > "$r/artifacts/environment.txt"
tar -xf "$r/versioned.tar" -C "$r/versioned"
sha256sum "$r/versioned.tar" > "$r/artifacts/versioned-archive.sha256"
printf '%s\n' 97fa32d84be264c42db836c72f32512ff811f0fc > "$r/artifacts/versioned.sha"
printf '%s\n' 14dc9698eb6e400ba6f677575eff7a5659deeed6 > "$r/artifacts/versionfree.sha"
cp /mnt/data/temporary-deletions-20261020/artifacts/reference-binary "$r/artifacts/versionfree-binary"
cp /mnt/data/temporary-deletions-20261020/artifacts/reference-audit-binary "$r/artifacts/versionfree-audit-binary"
cd "$r/versioned"
/usr/bin/time -v -o "$r/artifacts/build-versioned.time" cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run -j32 > "$r/artifacts/build-versioned.log" 2>&1
binary=$(find "$CARGO_TARGET_DIR/release/deps" -maxdepth 1 -name 'hierarchical_spann_profile_quantized-*' -type f -executable | head -1)
cp "$binary" "$r/artifacts/versioned-binary"
sha256sum "$r/artifacts/"*-binary > "$r/artifacts/binaries.sha256"
cp /mnt/data/temporary-deletions-20261020/artifacts/wikipedia-gt.parquet "$r/artifacts/"
cp /mnt/data/temporary-deletions-20261020/artifacts/gt-verification.json "$r/artifacts/"
