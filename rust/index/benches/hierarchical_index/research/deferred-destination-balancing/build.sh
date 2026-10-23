#!/usr/bin/env bash
set -euo pipefail
r=/home/ubuntu/benchmark-data/deferred-destination-balancing
mkdir -p /home/ubuntu/chroma-deferred-destination-balancing "$r"
cd /home/ubuntu/chroma-deferred-destination-balancing
tar -xf /home/ubuntu/benchmark-data/deferred-base.tar
tar -xf /home/ubuntu/benchmark-data/deferred-overlay.tar
export PATH=/home/ubuntu/.cargo/bin:$PATH CARGO_TARGET_DIR=/home/ubuntu/chroma-benchmark/target
cargo clean -p chroma-index --release
cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run
binary=$(find "$CARGO_TARGET_DIR/release/deps" -maxdepth 1 -name 'hierarchical_spann_profile_quantized-*' -type f -executable | head -1)
cp "$binary" "$r/candidate-binary"
cp /home/ubuntu/benchmark-data/continuous-scheduler/baseline-binary "$r/baseline-binary"
sha256sum "$r/"*-binary > "$r/binary.sha256"
find rust/index/benches/hierarchical_index -type f -print0 | sort -z | xargs -0 sha256sum > "$r/candidate-source.sha256"
