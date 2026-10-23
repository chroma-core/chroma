#!/usr/bin/env bash
set -euo pipefail
r=/home/ubuntu/benchmark-data/continuous-scheduler
cd /home/ubuntu/chroma-continuous-scheduler
mv "$r/candidate-binary" "$r/exploratory-full-npa-binary"
tar -xf /home/ubuntu/benchmark-data/continuous-scheduler-overlay.tar
export PATH=/home/ubuntu/.cargo/bin:$PATH CARGO_TARGET_DIR=/home/ubuntu/chroma-benchmark/target
cargo clean -p chroma-index --release
cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run
binary=$(find "$CARGO_TARGET_DIR/release/deps" -maxdepth 1 -name 'hierarchical_spann_profile_quantized-*' -type f -executable | head -1)
cp "$binary" "$r/candidate-binary"
sha256sum "$r/candidate-binary" > "$r/candidate.sha256"
find rust/index/benches/hierarchical_index -type f -print0 | sort -z | xargs -0 sha256sum > "$r/candidate-source.sha256"
