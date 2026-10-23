#!/usr/bin/env bash
set -u
export PATH="$HOME/.cargo/bin:$PATH"
results=/home/ubuntu/benchmark-data/merge-posting-preservation/final
repo=/home/ubuntu/chroma-merge-posting-preservation
export CARGO_TARGET_DIR=/home/ubuntu/chroma-benchmark/target
cd /home/ubuntu/chroma-benchmark
git -C "$repo" fetch "$results/source.bundle" refs/heads/codex/preserve-postings-during-merge || exit 1
git -C "$repo" checkout --detach FETCH_HEAD || exit 1
cd "$repo"
cargo build --release -p chroma-index --bench hierarchical_spann_profile_quantized --message-format=json > "$results/build.jsonl" 2> "$results/build.log" || exit 1
binary=$(python3 - "$results/build.jsonl" <<'BUILD_PY'
import json,sys
for line in open(sys.argv[1]):
 try: a=json.loads(line)
 except json.JSONDecodeError: continue
 if a.get('reason')=='compiler-artifact' and a.get('target',{}).get('name')=='hierarchical_spann_profile_quantized' and a.get('executable'): print(a['executable'])
BUILD_PY
)
cp "$binary" "$results/candidate-binary"
git rev-parse HEAD > "$results/revision"
common=(--metric l2 --checkpoint 10 --checkpoint-size 100000 --write-navigation fp --fp-npa --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false --validate-postings --save)
for name in msmarco-1 wikipedia-1 msmarco-2 wikipedia-2; do
 if [[ "$name" == msmarco-* ]]; then
  settings=(--dataset ms-marco --threads 64 --balance-threads 64 --write-beam-tau 1.5 --write-rng-factor 4)
 else
  settings=(--dataset wikipedia-en --threads 32 --balance-threads 32 --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0)
 fi
 command=("$results/candidate-binary" "${common[@]}" "${settings[@]}" --save-dir "/mnt/data/merge-preservation-final-$name")
 printf '%q ' "${command[@]}" > "$results/$name.command"
 /usr/bin/time -v -o "$results/$name.time" "${command[@]}" > "$results/$name.log" 2>&1
 status=$?
 echo "Finished $name exit=$status"
done
echo 'Merge-posting preservation validation completed'
