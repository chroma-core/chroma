#!/usr/bin/env bash
set -euo pipefail
exec 9>/mnt/data/chroma-benchmark.lock
flock -n 9
r=/home/ubuntu/benchmark-data/fresh-index-20261008
export HF_HOME=/mnt/data/huggingface HF_HUB_OFFLINE=1
run() {
 dataset=$1; variant=$2; label=$3; shift 3
 name="$dataset-$label"
 save="/mnt/data/fresh-index-20261008-$name"
 test ! -e "$save"
 args=(--dataset "$dataset" --metric l2 --checkpoint 10 --checkpoint-size 100000 --write-navigation fp --fp-npa --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false --save --save-dir "$save")
 if [[ "$dataset" == wikipedia-en ]]; then
  args+=(--threads 32 --balance-threads 32 --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0)
 else
  args+=(--threads 64 --balance-threads 64 --write-beam-tau 1.5 --write-rng-factor 4)
 fi
 args+=("$@")
 printf '%q ' "$r/$variant-binary" "${args[@]}" > "$r/$name.command"
 echo "START $name $(date -u +%FT%TZ)"
 /usr/bin/time -v -o "$r/$name.time" "$r/$variant-binary" "${args[@]}" > "$r/$name.log" 2>&1
 cp "$save/checkpoint.json" "$r/$name.checkpoint.json"
 python3 - "$save/checkpoint.json" <<'PY'
import json,sys
m=json.load(open(sys.argv[1])); assert m['checkpoint_idx']==9 and m['total_vectors']==1000000
PY
 echo "FINISH $name $(date -u +%FT%TZ)"
}
for dataset in wikipedia-en ms-marco; do
 run "$dataset" baseline baseline-1
 threads=32; [[ "$dataset" == ms-marco ]] && threads=64
 run "$dataset" parallel parallel-1 --initial-split-threads "$threads"
 run "$dataset" cadence fixed-50000-1 --startup-balance-policy fixed --startup-balance-size 50000
 run "$dataset" cadence fixed-10000-1 --startup-balance-policy fixed --startup-balance-size 10000
 run "$dataset" cadence adaptive-1 --startup-balance-policy adaptive --startup-balance-min-size 5000 --startup-balance-size 100000
 run "$dataset" cadence fixed-5000-1 --startup-balance-policy fixed --startup-balance-size 5000
 run "$dataset" cadence fixed-100000-1 --startup-balance-policy fixed --startup-balance-size 100000
 run "$dataset" baseline baseline-2
done
echo 0 > "$r/screen-exit-status"
