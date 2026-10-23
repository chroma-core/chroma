#!/bin/bash
set -euo pipefail
exec 9>/mnt/data/chroma-benchmark.lock
flock 9
export PATH=/home/ubuntu/.cargo/bin:$PATH CARGO_TARGET_DIR=/home/ubuntu/chroma-benchmark/target
out=/home/ubuntu/benchmark-data/bulk-version-cache-fixes
python3 "$out/install-cache-component-probes.py"
for spec in page:metadata-page-version-fill bulk:bulk-version-cache-fill; do
 mode=${spec%%:*}; branch=${spec#*:}
 cd /home/ubuntu/chroma-$branch
 cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run > "$out/$mode-probe-build.log" 2>&1
 cp "$CARGO_TARGET_DIR/release/deps/hierarchical_spann_profile_quantized-141263a897e45414" "$out/$mode-probe-binary"
 git diff > "$out/$mode-probe.diff"
done
export CHROMA_BALANCE_COMPONENT_FIXTURE=/mnt/data/large-index-investigation-components-40m CHROMA_COMPONENT_CACHE_BYTES=1073741824 CHROMA_COMPONENT_LEAF_COUNT=200
for spec in baseline-1 page-1 bulk-1 bulk-2 page-2 baseline-2; do
 mode=${spec%-*}
 binary="$out/$mode-probe-binary"
 if [[ $mode == baseline ]]; then binary=/home/ubuntu/benchmark-data/large-index-balancing-investigation/component-io-binary; fi
 /usr/bin/time -v -o "$out/component-$spec.time" timeout 600 "$binary" > "$out/component-$spec.log" 2>&1
 echo 0 > "$out/component-$spec.status"
done
sha256sum "$out/"*-binary > "$out/binaries.sha256"
echo 0 > "$out/probes.status"
