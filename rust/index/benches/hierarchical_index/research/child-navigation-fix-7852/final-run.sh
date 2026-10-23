#!/usr/bin/env bash
set -euo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
results=/home/ubuntu/benchmark-data/child-navigation-fix-7852
source_checkpoint=/mnt/data/child-navigation-source-800k
export CARGO_TARGET_DIR=/home/ubuntu/chroma-benchmark/target
cd /home/ubuntu/chroma-child-navigation-fix-candidate
git fetch "$results/final-source.bundle" refs/heads/codex/balance-child-navigation
git checkout --detach FETCH_HEAD
cargo build --release -p chroma-index --bench hierarchical_spann_profile_quantized --message-format=json > "$results/final-build.jsonl" 2> "$results/final-build.log"
binary=$(python3 - "$results/final-build.jsonl" <<'BUILD_PY'
import json,sys
for line in open(sys.argv[1]):
 try: a=json.loads(line)
 except json.JSONDecodeError: continue
 if a.get('reason')=='compiler-artifact' and a.get('target',{}).get('name')=='hierarchical_spann_profile_quantized' and a.get('executable'): print(a['executable'])
BUILD_PY
)
cp "$binary" "$results/final-binary"
git rev-parse HEAD > "$results/final-revision"
common=(--metric l2 --checkpoint 10 --checkpoint-size 100000 --write-navigation fp --fp-npa --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false)
run() {
    name=$1; variant=$2; shift 2
    binary="$results/$variant-binary"
    printf '%q ' "$binary" "${common[@]}" "$@" > "$results/$name.command"
    printf '\n' >> "$results/$name.command"
    /usr/bin/time -v -o "$results/$name.time" "$binary" "${common[@]}" "$@" > "$results/$name.log" 2>&1
    echo "Finished $name"
}
for number in 1 2; do
 replay=/mnt/data/child-navigation-final-7852-candidate-$number
 test ! -e "$replay"
 cp -a --reflink=auto "$source_checkpoint" "$replay"
 run "final-replay-candidate-$number" final --dataset ms-marco --threads 64 --balance-threads 64 --write-beam-tau 1.5 --write-rng-factor 4 --resume --save-dir "$replay"
done
run final-full-wikipedia-candidate final --dataset wikipedia-en --threads 32 --balance-threads 32 --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0
run final-full-msmarco-candidate final --dataset ms-marco --threads 64 --balance-threads 64 --write-beam-tau 1.5 --write-rng-factor 4
run final-validate-msmarco final --dataset ms-marco --threads 64 --balance-threads 64 --write-beam-tau 1.5 --write-rng-factor 4 --validate-postings
for number in 1 2; do
 run "final-validate-wikipedia-$number" final --dataset wikipedia-en --threads 32 --balance-threads 32 --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0 --validate-postings
done
sha256sum "$source_checkpoint/checkpoint.json" > "$results/source-final.sha256"
cmp "$results/source-before.sha256" "$results/source-final.sha256"
echo 'Final commit timing and validation completed'
