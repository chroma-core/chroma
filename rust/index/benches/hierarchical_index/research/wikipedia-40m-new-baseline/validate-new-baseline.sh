#!/bin/bash
set -euo pipefail
exec 9>/mnt/data/chroma-benchmark.lock
flock 9
out=/home/ubuntu/benchmark-data/wikipedia-40m-new-baseline
trap 'echo $? > "$out/preparation.status"' EXIT
export PATH=/home/ubuntu/.cargo/bin:$PATH CARGO_TARGET_DIR=/home/ubuntu/chroma-benchmark/target HF_HOME=/mnt/data/huggingface HF_HUB_OFFLINE=1
cd /home/ubuntu/chroma-wikipedia-40m-new-baseline
git rev-parse HEAD > "$out/source.sha"
git status --porcelain > "$out/source.status"
test ! -s "$out/source.status"
cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run > "$out/build.log" 2>&1
cp "$CARGO_TARGET_DIR/release/deps/hierarchical_spann_profile_quantized-141263a897e45414" "$out/binary"
sha256sum "$out/binary" > "$out/binary.sha256"
base=(--dataset wikipedia-en --metric l2 --checkpoint-size 1000000 --branching-factor 100 --split-threshold 2048 --merge-threshold 512 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-beam-min 10 --write-beam-max 16 --write-level-taus _,_,0.30 --max-replicas 1 --write-rng-epsilon 0 --write-rng-factor 0 --read-beam-min 10 --read-beam-max 256 --read-level-taus _,_,_ --read-level-min-pcts 50,5,0 --recall-rerank-vectors 1,8 --recall-tau-values 0.7,1,2 --max-cache-bytes 1073741824 --compute-gt-clusters false)
"$out/binary" "${base[@]}" --checkpoint 2 --num-queries 0 --save --save-dir /mnt/data/new-baseline-snapshot-smoke --checkpoint-snapshots-dir /mnt/data/new-baseline-snapshot-smoke-snapshots > "$out/smoke-2.log" 2>&1
find /mnt/data/new-baseline-snapshot-smoke-snapshots -type f -print0 | sort -z | xargs -0 sha256sum > "$out/smoke-before.sha256"
"$out/binary" "${base[@]}" --checkpoint 3 --num-queries 0 --resume --save --save-dir /mnt/data/new-baseline-snapshot-smoke --checkpoint-snapshots-dir /mnt/data/new-baseline-snapshot-smoke-snapshots > "$out/smoke-3.log" 2>&1
sha256sum -c "$out/smoke-before.sha256" > "$out/smoke-preservation.log"
for checkpoint in 1 2; do
 "$out/binary" "${base[@]}" --checkpoint "$checkpoint" --num-queries 0 --resume --save --save-dir /mnt/data/new-baseline-snapshot-smoke-snapshots/${checkpoint}000000 > "$out/smoke-reopen-$checkpoint.log" 2>&1
 rg 'Unique vectors:|orphaned' "$out/smoke-reopen-$checkpoint.log" >> "$out/smoke-validation.txt"
done
python3 - <<'PY'
import pathlib,json
p=pathlib.Path('/mnt/data/new-baseline-snapshot-smoke-snapshots')
for n in (1,2):
 cp=p/f'{n}000000'; assert json.loads((cp/'checkpoint.json').read_text())['total_vectors']==n*1000000
 assert (cp/'checkpoint.json').stat().st_nlink==1
PY
echo 0 > "$out/snapshot-validation.status"
