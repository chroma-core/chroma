#!/bin/bash
set -euo pipefail
r=/mnt/msmarco/balance-experiments-20261020
exec 9>/mnt/data/chroma-benchmark.lock
flock -n 9
export PATH=/home/ubuntu/.cargo/bin:$PATH CARGO_HOME=/mnt/msmarco/version-free-writer-7927/cargo-home CARGO_TARGET_DIR=/mnt/msmarco/version-free-writer-7927/target TMPDIR="$r/tmp" RUSTUP_TOOLCHAIN=1.92.0 RUSTFLAGS='-C target-cpu=native'
mkdir -p "$r/artifacts" "$r/tmp"
{ date -u; rustc --version; uname -a; lscpu; free -h; df -h; } > "$r/artifacts/environment.txt"
for variant in reference transfers scheduling; do
mkdir "$r/$variant"
tar -xf "/mnt/msmarco/$variant-experiment.tar" -C "$r/$variant"
sha256sum "/mnt/msmarco/$variant-experiment.tar" > "$r/artifacts/$variant-archive.sha256"
done
printf '%s\n' eea4ada6f21554adb535d1ffdea95f74cb76650b > "$r/artifacts/reference.sha"
printf '%s\n' f76e8c6541c363c1068d864eb6ff9835093167e1 > "$r/artifacts/transfers.sha"
printf '%s\n' 14dc9698eb6e400ba6f677575eff7a5659deeed6 > "$r/artifacts/scheduling.sha"
cp /mnt/msmarco/version-free-followup-20261020/artifacts/filtered-binary "$r/artifacts/reference-binary"
for variant in transfers scheduling; do
cd "$r/$variant"
/usr/bin/time -v -o "$r/artifacts/build-$variant.time" cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run -j32 > "$r/artifacts/build-$variant.log" 2>&1
binary=$(find "$CARGO_TARGET_DIR/release/deps" -maxdepth 1 -name 'hierarchical_spann_profile_quantized-*' -type f -executable | head -1)
cp "$binary" "$r/artifacts/$variant-binary"
done
sha256sum "$r/artifacts/"*-binary > "$r/artifacts/binaries.sha256"
cp /mnt/msmarco/version-free-followup-20261020/artifacts/wikipedia-gt.parquet "$r/artifacts/"
cp /mnt/msmarco/version-free-followup-20261020/artifacts/gt-verification.json "$r/artifacts/"
