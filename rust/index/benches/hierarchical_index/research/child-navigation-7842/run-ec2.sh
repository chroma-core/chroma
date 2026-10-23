#!/usr/bin/env bash
set -euo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
results=/home/ubuntu/benchmark-data/child-navigation-7842
repo=/home/ubuntu/chroma-balance-child-navigation
base_repo=/home/ubuntu/chroma-benchmark
source_checkpoint=/mnt/data/child-navigation-source-800k
export CARGO_TARGET_DIR=/home/ubuntu/chroma-benchmark/target

git -C "$base_repo" fetch "$results/source.bundle" refs/heads/codex/balance-child-navigation-payload
test ! -e "$repo"
git -C "$base_repo" worktree add --detach "$repo" cc6a16140
cd "$repo"
cargo build --release -p chroma-index --bench hierarchical_spann_profile_quantized --message-format=json > "$results/candidate-build.jsonl" 2> "$results/candidate-build.log"
binary=$(python3 - "$results/candidate-build.jsonl" <<'PY'
import json,sys
for line in open(sys.argv[1]):
 try: a=json.loads(line)
 except json.JSONDecodeError: continue
 if a.get('reason')=='compiler-artifact' and a.get('target',{}).get('name')=='hierarchical_spann_profile_quantized' and a.get('executable'): print(a['executable'])
PY
)
cp "$binary" "$results/candidate-binary"
git rev-parse HEAD > "$results/candidate-revision"
cp /home/ubuntu/benchmark-data/destination-deltas-7842/control-revision "$results/control-revision"
while ! rg -q 'Recall duration:|no ground truth for 0M boundary' "$results/source-build.log"; do
    if ! kill -0 211096 2>/dev/null; then echo 'Source checkpoint build did not finish'; exit 1; fi
    sleep 10
done
rg -q 'after reopen: 800000 root-reachable valid ids' "$results/source-build.log"
common=(--metric l2 --checkpoint 10 --checkpoint-size 100000 --write-navigation fp --fp-npa --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false)
run() {
    name=$1; variant=$2; shift 2
    binary="$results/$variant-binary"
    printf '%q ' "$binary" "${common[@]}" "$@" > "$results/$name.command"
    printf '\n' >> "$results/$name.command"
    /usr/bin/time -v -o "$results/$name.time" "$binary" "${common[@]}" "$@" > "$results/$name.log" 2>&1
    echo "Finished $name"
}
sha256sum "$source_checkpoint/checkpoint.json" > "$results/source-before.sha256"
for name in control-1 candidate-1 candidate-2 control-2; do
    variant=${name%-*}
    replay=/mnt/data/child-navigation-7842-$name
    test ! -e "$replay"
    cp -a --reflink=auto "$source_checkpoint" "$replay"
    run "replay-$name" "$variant" --dataset ms-marco --threads 64 --balance-threads 64 --write-beam-tau 1.5 --write-rng-factor 4 --resume --save-dir "$replay"
done
sha256sum "$source_checkpoint/checkpoint.json" > "$results/source-after.sha256"
cmp "$results/source-before.sha256" "$results/source-after.sha256"
for variant in control candidate; do
    run "full-wikipedia-$variant" "$variant" --dataset wikipedia-en --threads 32 --balance-threads 32 --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0
done
for variant in candidate control; do
    run "full-msmarco-$variant" "$variant" --dataset ms-marco --threads 64 --balance-threads 64 --write-beam-tau 1.5 --write-rng-factor 4
done
run validate-msmarco candidate --dataset ms-marco --threads 64 --balance-threads 64 --write-beam-tau 1.5 --write-rng-factor 4 --validate-postings
run validate-wikipedia candidate --dataset wikipedia-en --threads 32 --balance-threads 32 --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0 --validate-postings
echo 'Child-navigation benchmarks and validation completed'
