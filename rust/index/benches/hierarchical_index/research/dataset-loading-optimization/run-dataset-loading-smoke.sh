#!/usr/bin/env bash
set -euo pipefail
results=/home/ubuntu/benchmark-data/dataset-loading-20261008
cd /home/ubuntu/chroma-dataset-loading-probe
binary=
for path in /home/ubuntu/chroma-benchmark/target/release/deps/hierarchical_spann_profile_quantized-*; do
 if [[ -x "$path" && ! -d "$path" ]] && [[ -z "$binary" || "$path" -nt "$binary" ]]; then binary="$path"; fi
done
for dataset in ms-marco wikipedia-en; do
 if [[ "$dataset" == ms-marco ]]; then
  extra=(--threads 64 --balance-threads 64 --write-beam-tau 1.5 --write-rng-factor 4)
 else
  extra=(--threads 32 --balance-threads 32 --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0)
 fi
 command=("$binary" --dataset "$dataset" --metric l2 --checkpoint 2 --checkpoint-size 100000 --write-navigation fp --fp-npa --num-queries 1 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false --save --save-dir "/mnt/data/dataset-loading-smoke-$dataset" "${extra[@]}")
 printf '%q ' "${command[@]}" > "$results/$dataset-smoke.command"
 printf '\n' >> "$results/$dataset-smoke.command"
 /usr/bin/time -v -o "$results/$dataset-smoke.time" "${command[@]}" > "$results/$dataset-smoke.log" 2>&1
done
