#!/bin/bash
set -euo pipefail
exec 9>/mnt/data/chroma-benchmark.lock
flock 9
out=/home/ubuntu/benchmark-data/wikipedia-40m-new-baseline
trap 'echo $? > "$out/audit.status"' EXIT
test "$(cat "$out/benchmark.status")" = 0
export PATH=/home/ubuntu/.cargo/bin:$PATH CARGO_TARGET_DIR=/home/ubuntu/chroma-benchmark/target HF_HOME=/mnt/data/huggingface HF_HUB_OFFLINE=1 CHROMA_VALIDATE_SAVED_PREFIX=1
cd /home/ubuntu/chroma-wikipedia-snapshot-validation
git rev-parse HEAD > "$out/audit-source.sha"
cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run > "$out/audit-build.log" 2>&1
cp "$CARGO_TARGET_DIR/release/deps/hierarchical_spann_profile_quantized-141263a897e45414" "$out/audit-binary"
sha256sum "$out/audit-binary" > "$out/audit-binary.sha256"
for cp in 9 19 29 39 40; do
 fixture=/mnt/data/wikipedia-40m-baseline-20261009-snapshots/${cp}000000
 if [[ $cp == 40 ]]; then fixture=/mnt/data/wikipedia-40m-baseline-20261009; fi
 args=(--dataset wikipedia-en --metric l2 --checkpoint "$cp" --checkpoint-size 1000000 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0 --num-queries 0 --compute-gt-clusters false --resume --save --save-dir "$fixture" --max-cache-bytes 1073741824)
 printf '%q ' "$out/audit-binary" "${args[@]}" > "$out/audit-$cp.command.txt"
 /usr/bin/time -v -o "$out/audit-$cp.time" "$out/audit-binary" "${args[@]}" > "$out/audit-$cp.log" 2>&1
 rg '\[saved checkpoint validation\]' "$out/audit-$cp.log" >> "$out/audit-summary.txt"
done
python3 "$out/validate-snapshot-manifests.py" > "$out/snapshot-validation-after-audit.log" 2>&1
