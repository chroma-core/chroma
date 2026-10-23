#!/bin/bash
set -euo pipefail
r=/mnt/msmarco/version-free-writer-7927
exec 9>/mnt/data/chroma-benchmark.lock
flock -n 9
printf '%s pid=%s\n' "$(date -u +%FT%TZ)" "$$" > "$r/artifacts/lock-acquired.txt"
export PATH=/home/ubuntu/.cargo/bin:$PATH
export CARGO_HOME="$r/cargo-home" CARGO_TARGET_DIR="$r/target" TMPDIR="$r/tmp"
export RUSTUP_TOOLCHAIN=1.92.0 RUSTFLAGS='-C target-cpu=native'
mkdir -p "$CARGO_HOME" "$TMPDIR"
cp -a /mnt/msmarco/cargo-home/registry "$CARGO_HOME/"
cp -a /mnt/msmarco/cargo-home/git "$CARGO_HOME/"
{ date -u; rustc --version; cargo --version; uname -a; lscpu; free -h; df -h; env | sort | grep -E '^(CARGO|RUST|TMPDIR)'; } > "$r/artifacts/environment.txt"
sha256sum "$r/"*.tar > "$r/artifacts/archive.sha256"
for variant in baseline candidate; do
 tar -xf "$r/vf-$variant.tar" -C "$r/$variant"
 cd "$r/$variant"
 /usr/bin/time -v -o "$r/artifacts/build-$variant.time" cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run -j32 > "$r/artifacts/build-$variant.log" 2>&1
 binary=$(find "$CARGO_TARGET_DIR/release/deps" -maxdepth 1 -name 'hierarchical_spann_profile_quantized-*' -type f -executable | head -1)
 cp "$binary" "$r/artifacts/$variant-binary"
 "$r/artifacts/$variant-binary" --help > "$r/artifacts/$variant-help.txt"
 sha256sum "$r/artifacts/$variant-binary" >> "$r/artifacts/binary.sha256"
done
date -u +%FT%TZ > "$r/artifacts/builds-complete"
while [[ ! -f "$r/run-ready" ]]; do sleep 10; done
python3 "$r/run.py"
