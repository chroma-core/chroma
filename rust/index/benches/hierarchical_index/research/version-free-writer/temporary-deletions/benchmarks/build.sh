#!/bin/bash
set -euo pipefail
r=/mnt/data/temporary-deletions-20261020
exec 9>/mnt/data/chroma-benchmark.lock
flock -n 9
export PATH=/home/ubuntu/.cargo/bin:$PATH CARGO_HOME=/mnt/msmarco/version-free-writer-7927/cargo-home CARGO_TARGET_DIR=/mnt/msmarco/version-free-writer-7927/target TMPDIR="$r/tmp" RUSTUP_TOOLCHAIN=1.92.0 RUSTFLAGS='-C target-cpu=native'
mkdir -p "$r/artifacts" "$r/tmp" "$r/candidate"
{ date -u; rustc --version; uname -a; lscpu; free -h; df -h; } > "$r/artifacts/environment.txt"
tar -xf /mnt/data/tombstone-experiment.tar -C "$r/candidate"
sha256sum /mnt/data/tombstone-experiment.tar > "$r/artifacts/candidate-archive.sha256"
printf '%s\n' 14dc9698eb6e400ba6f677575eff7a5659deeed6 > "$r/artifacts/reference.sha"
printf '%s\n' 1d659e171b8456d4e0bbd6c77dbf6cbb40674470 > "$r/artifacts/candidate.sha"
cp /mnt/msmarco/balance-experiments-20261020/artifacts/scheduling-binary "$r/artifacts/reference-binary"
cd "$r/candidate"
/usr/bin/time -v -o "$r/artifacts/build-candidate.time" cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run -j32 > "$r/artifacts/build-candidate.log" 2>&1
binary=$(find "$CARGO_TARGET_DIR/release/deps" -maxdepth 1 -name 'hierarchical_spann_profile_quantized-*' -type f -executable | head -1)
cp "$binary" "$r/artifacts/candidate-binary"
sha256sum "$r/artifacts/"*-binary > "$r/artifacts/binaries.sha256"
cp /mnt/msmarco/balance-experiments-20261020/artifacts/wikipedia-gt.parquet "$r/artifacts/"
cp /mnt/msmarco/balance-experiments-20261020/artifacts/gt-verification.json "$r/artifacts/"
cp /mnt/msmarco/balance-experiments-20261020/artifacts/gt9-source-manifest.json "$r/artifacts/"
