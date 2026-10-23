#!/usr/bin/env bash
set -euo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
repo=/home/ubuntu/chroma-reassignment-profile
results=/home/ubuntu/benchmark-data/reassignment-profile-7842
cd "$repo"
export CARGO_TARGET_DIR=/home/ubuntu/chroma-benchmark/target
cargo build --release -p chroma-index --bench hierarchical_spann_profile_quantized --message-format=json > "$results/build.jsonl" 2> "$results/build.log"
binary=$(python3 - "$results/build.jsonl" <<'PY'
import json,sys
for line in open(sys.argv[1]):
 try: a=json.loads(line)
 except json.JSONDecodeError: continue
 if a.get('reason')=='compiler-artifact' and a.get('target',{}).get('name')=='hierarchical_spann_profile_quantized' and a.get('executable'): print(a['executable'])
PY
)
cp "$binary" "$results/profile-binary"
binary="$results/profile-binary"
git rev-parse HEAD > "$results/base-revision"
git diff > "$results/instrumentation.patch"
source_checkpoint=/mnt/data/balance-diagnostics-source
sha256sum "$source_checkpoint/checkpoint.json" > "$results/source-before.sha256"
common=(--metric l2 --checkpoint 10 --checkpoint-size 100000 --write-navigation fp --fp-npa --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false)
run() {
 name=$1; shift
 printf '%q ' "$binary" "$@" > "$results/$name.command"
 printf '\n' >> "$results/$name.command"
 /usr/bin/time -v -o "$results/$name.time" "$binary" "$@" > "$results/$name.log" 2>&1
 printf 'Finished %s\n' "$name"
}
for repetition in 1 2; do
 replay=/mnt/data/reassignment-profile-7842-$repetition
 test ! -e "$replay"
 cp -al "$source_checkpoint" "$replay"
 run replay-$repetition "${common[@]}" --dataset ms-marco --threads 64 --balance-threads 64 --write-beam-tau 1.5 --write-rng-factor 4 --resume --save-dir "$replay"
done
sha256sum "$source_checkpoint/checkpoint.json" > "$results/source-after.sha256"
cmp "$results/source-before.sha256" "$results/source-after.sha256"
run full-wikipedia "${common[@]}" --dataset wikipedia-en --threads 32 --balance-threads 32 --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0
run full-msmarco "${common[@]}" --dataset ms-marco --threads 64 --balance-threads 64 --write-beam-tau 1.5 --write-rng-factor 4
printf 'All profiles completed\n'
