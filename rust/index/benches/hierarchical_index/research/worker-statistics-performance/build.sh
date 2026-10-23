#!/usr/bin/env bash
set -euo pipefail
results=/home/ubuntu/benchmark-data/worker-statistics-performance
source_dir=/home/ubuntu/chroma-worker-statistics
mkdir -p "$results" "$source_dir"
tar -xf /home/ubuntu/benchmark-data/worker-statistics-source.tar -C "$source_dir"
export PATH=/home/ubuntu/.cargo/bin:$PATH
export CARGO_TARGET_DIR=/home/ubuntu/chroma-benchmark/target
cd "$source_dir"
{ rustc --version; cargo --version; uname -a; lscpu; free -h; df -h /mnt/data; } > "$results/environment.txt"
find rust/index -type f -name '*.rs' -print0 | sort -z | xargs -0 sha256sum > "$results/source.sha256"
cargo test -p chroma-index --release --test hierarchical_policy_cache --test hierarchical_posting_delta > "$results/tests.log" 2>&1
cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run > "$results/build.log" 2>&1
binary=$(find "$CARGO_TARGET_DIR/release/deps" -maxdepth 1 -name 'hierarchical_spann_profile_quantized-*' -type f -executable | head -1)
cp "$binary" "$results/candidate-binary"
cp /home/ubuntu/benchmark-data/read-scoring-10m-merged-22eb704/candidate-binary "$results/baseline-binary"
sha256sum "$results/"*-binary > "$results/binary.sha256"
printf '%s\n' 0 > "$results/build-status"
