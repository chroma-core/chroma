#!/bin/bash
set -euo pipefail
r=/mnt/msmarco/version-free-followup-20261020
variant=$1
case "$variant" in instrumented|optimized|filtered) ;; *) exit 2;; esac
exec 9>/mnt/data/chroma-benchmark.lock
flock -n 9
export PATH=/home/ubuntu/.cargo/bin:$PATH CARGO_HOME=/mnt/msmarco/version-free-writer-7927/cargo-home CARGO_TARGET_DIR=/mnt/msmarco/version-free-writer-7927/target TMPDIR="$r/tmp" RUSTUP_TOOLCHAIN=1.92.0 RUSTFLAGS='-C target-cpu=native'
tar -xf "$r/$variant.tar" -C "$r/$variant" rust/index/benches/hierarchical_index/writer/diagnostics.rs rust/index/benches/hierarchical_spann_profile_quantized.rs
python3 "$r/audit-overlay.py" "$variant"
cd "$r/$variant"
cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run -j32 > "$r/artifacts/build-audit-$variant.log" 2>&1
binary=$(find "$CARGO_TARGET_DIR/release/deps" -maxdepth 1 -name 'hierarchical_spann_profile_quantized-*' -type f -executable | head -1)
cp "$binary" "$r/artifacts/$variant-audit-binary"
sha256sum "$r/artifacts/$variant-audit-binary" > "$r/artifacts/$variant-audit-binary.sha256"
"/mnt/msmarco/version-free-writer-7927/venv/bin/python" "$r/audit.py" "$variant"
