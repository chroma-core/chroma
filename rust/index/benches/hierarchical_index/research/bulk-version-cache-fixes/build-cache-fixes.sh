#!/bin/bash
set -e
export PATH=/home/ubuntu/.cargo/bin:$PATH CARGO_TARGET_DIR=/home/ubuntu/chroma-benchmark/target
out=/home/ubuntu/benchmark-data/bulk-version-cache-fixes
cp "$CARGO_TARGET_DIR/release/deps/hierarchical_spann_profile_quantized-141263a897e45414" "$out/page-binary"
cd /home/ubuntu/chroma-metadata-page-version-fill
cargo test -p chroma-index --test hierarchical_posting_delta --test hierarchical_policy_cache > "$out/page-tests.log" 2>&1
cd /home/ubuntu/chroma-bulk-version-cache-fill
cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run > "$out/bulk-build.log" 2>&1
cp "$CARGO_TARGET_DIR/release/deps/hierarchical_spann_profile_quantized-141263a897e45414" "$out/bulk-binary"
cargo test -p chroma-index --test hierarchical_posting_delta --test hierarchical_policy_cache > "$out/bulk-tests.log" 2>&1
echo 0 > "$out/build-both.status"
