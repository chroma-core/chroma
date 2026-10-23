#!/bin/bash
set -euo pipefail
r=/mnt/msmarco/version-free-writer-7927
export PATH=/home/ubuntu/.cargo/bin:$PATH CARGO_HOME="$r/cargo-home" CARGO_TARGET_DIR="$r/target" TMPDIR="$r/tmp" RUSTUP_TOOLCHAIN=1.92.0 RUSTFLAGS='-C target-cpu=native'
for variant in baseline candidate; do
 python3 "$r/audit-overlay.py" "$variant"
 cd "$r/$variant"
 cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run -j32 > "$r/artifacts/build-audit-$variant.log" 2>&1
 cp "$CARGO_TARGET_DIR/release/deps/$(find "$CARGO_TARGET_DIR/release/deps" -maxdepth 1 -name 'hierarchical_spann_profile_quantized-*' -type f -executable -printf '%f\n' | head -1)" "$r/artifacts/$variant-audit-binary"
 sha256sum "$r/artifacts/$variant-audit-binary" >> "$r/artifacts/audit-binary.sha256"
done
"$r/venv/bin/python" "$r/audit.py"
