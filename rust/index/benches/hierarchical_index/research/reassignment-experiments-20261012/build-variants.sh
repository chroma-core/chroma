#!/usr/bin/env bash
set -euo pipefail

out=/home/ubuntu/benchmark-data/reassignment-experiments-20261012
sources=/home/ubuntu/chroma-reassignment-experiments-20261012
mkdir -p "$out" "$sources"
exec 9>/mnt/data/chroma-benchmark.lock
flock -n 9 || { echo 'Another benchmark owns the machine; retry after it finishes.' >&2; exit 75; }
trap 'printf "%s\n" "$?" > "$out/build.status"' EXIT
export PATH=/home/ubuntu/.cargo/bin:$PATH
export CARGO_TARGET_DIR=/home/ubuntu/chroma-benchmark/target

cd "$out"
sha256sum -c source-archives.sha256
rustc --version > compiler.txt
cargo --version >> compiler.txt
for variant in raw baseline deferred batched; do
    test ! -e "$sources/$variant"
    tar -xzf "$out/$variant.tar.gz" -C "$sources"
    cd "$sources/$variant"
    targets=(--bench hierarchical_spann_profile_quantized)
    if [[ $variant == batched ]]; then targets+=(--bench hierarchical_spann_audit); fi
    cargo bench --locked -p chroma-index "${targets[@]}" --no-run --message-format=json \
        > "$out/$variant.build.jsonl" 2> "$out/$variant.build.log"
    python3 - "$out" "$variant" <<'PY'
import json, shutil, sys
from pathlib import Path
out, variant = Path(sys.argv[1]), sys.argv[2]
artifacts = {}
for line in (out / f'{variant}.build.jsonl').read_text().splitlines():
    if not line.startswith('{'):
        continue
    item = json.loads(line)
    if item.get('reason') == 'compiler-artifact' and item.get('executable'):
        artifacts[item['target']['name']] = Path(item['executable'])
shutil.copy2(artifacts['hierarchical_spann_profile_quantized'], out / f'{variant}-binary')
if variant == 'batched':
    shutil.copy2(artifacts['hierarchical_spann_audit'], out / 'audit-binary')
PY
done
cd "$out"
sha256sum ./*-binary > binary.sha256
echo 'All variants are built; no measured runs have started.'
