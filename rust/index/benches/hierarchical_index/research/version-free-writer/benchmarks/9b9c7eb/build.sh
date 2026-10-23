#!/bin/bash
set -euo pipefail
r=/mnt/msmarco/version-free-writer-7927-9b9c7eb
old=/mnt/msmarco/version-free-writer-7927
exec 9>/mnt/data/chroma-benchmark.lock
flock -n 9
printf '%s pid=%s\n' "$(date -u +%FT%TZ)" "$$" > "$r/artifacts/lock-acquired.txt"
export PATH=/home/ubuntu/.cargo/bin:$PATH CARGO_HOME="$r/cargo-home" CARGO_TARGET_DIR="$r/target" TMPDIR="$r/tmp" RUSTUP_TOOLCHAIN=1.92.0 RUSTFLAGS='-C target-cpu=native'
ln -s "$old/cargo-home" "$CARGO_HOME"
ln -s "$old/target" "$CARGO_TARGET_DIR"
ln -s "$old/venv" "$r/venv"
ln -s "$old/wikipedia-baseline-1" "$r/wikipedia-baseline-1"
cp "$old/artifacts/baseline-binary" "$old/artifacts/"wikipedia-baseline-1.* "$old/artifacts/"*-gt* "$old/artifacts/"gt-verification* "$r/artifacts/"
tar -xf "$old/vf-baseline.tar" -C "$r/baseline"
tar -xf "$r/vf-candidate-9b9c7eb.tar" -C "$r/candidate"
{ date -u; rustc --version; cargo --version; uname -a; lscpu; free -h; df -h; env | sort | grep -E '^(CARGO|RUST|TMPDIR)'; } > "$r/artifacts/environment.txt"
sha256sum "$r/vf-candidate-9b9c7eb.tar" "$old/vf-baseline.tar" > "$r/artifacts/archive.sha256"
cd "$r/candidate"
/usr/bin/time -v -o "$r/artifacts/build-candidate.time" cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run -j32 > "$r/artifacts/build-candidate.log" 2>&1
binary=$(find "$CARGO_TARGET_DIR/release/deps" -maxdepth 1 -name 'hierarchical_spann_profile_quantized-*' -type f -executable | head -1)
cp "$binary" "$r/artifacts/candidate-binary"
sha256sum "$r/artifacts/"*-binary > "$r/artifacts/binary.sha256"
"$r/artifacts/candidate-binary" --help > "$r/artifacts/candidate-help.txt"
date -u +%FT%TZ > "$r/artifacts/builds-complete"
python3 "$r/run.py"
