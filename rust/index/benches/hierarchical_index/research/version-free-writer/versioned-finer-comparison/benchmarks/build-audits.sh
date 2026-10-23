#!/bin/bash
set -euo pipefail
r=/mnt/data/versioned-finer-20261020
exec 9>/mnt/data/chroma-benchmark.lock
flock -n 9
export PATH=/home/ubuntu/.cargo/bin:$PATH CARGO_HOME=/mnt/msmarco/version-free-writer-7927/cargo-home CARGO_TARGET_DIR=/mnt/msmarco/version-free-writer-7927/target TMPDIR="$r/tmp" RUSTUP_TOOLCHAIN=1.92.0 RUSTFLAGS='-C target-cpu=native'
mkdir "$r/versionfree"
tar -xf "$r/versionfree.tar.gz" -C "$r/versionfree"
for variant in versioned versionfree; do
 python3 "$r/audit-overlay.py" "$variant"
 cd "$r/$variant"
 /usr/bin/time -v -o "$r/artifacts/build-$variant-audit.time" cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run -j32 > "$r/artifacts/build-$variant-audit.log" 2>&1
 binary=$(find "$CARGO_TARGET_DIR/release/deps" -maxdepth 1 -name 'hierarchical_spann_profile_quantized-*' -type f -executable | head -1)
 cp "$binary" "$r/artifacts/$variant-audit-binary"
done
sha256sum "$r/artifacts/"*-binary > "$r/artifacts/binaries.sha256"
