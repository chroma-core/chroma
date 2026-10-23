#!/usr/bin/env bash
set -euo pipefail
results=/home/ubuntu/benchmark-data/pr7856-performance-20261006
mkdir -p "$results"
cp /home/ubuntu/benchmark-data/child-navigation-fix-7852/final-binary "$results/baseline-binary"
cp /home/ubuntu/benchmark-data/merge-posting-preservation/final/candidate-binary "$results/candidate-binary"
cp /home/ubuntu/benchmark-data/child-navigation-fix-7852/final-revision "$results/baseline-revision"
cp /home/ubuntu/benchmark-data/merge-posting-preservation/final/revision "$results/candidate-revision"
sha256sum "$results/"*-binary > "$results/binaries.sha256"
cd /home/ubuntu/chroma-merge-posting-preservation
common=(--metric l2 --checkpoint 10 --checkpoint-size 100000 --write-navigation fp --fp-npa --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false --save)
for round in 1 2; do
 for dataset in msmarco wikipedia; do
  if [[ "$dataset" == msmarco ]]; then
   settings=(--dataset ms-marco --threads 64 --balance-threads 64 --write-beam-tau 1.5 --write-rng-factor 4)
  else
   settings=(--dataset wikipedia-en --threads 32 --balance-threads 32 --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0)
  fi
  if [[ "$round" == 1 ]]; then variants=(baseline candidate); else variants=(candidate baseline); fi
  for variant in "${variants[@]}"; do
   name="$dataset-$variant-$round"
   checkpoint_dir="/mnt/data/pr7856-perf-20261006-$name"
   test ! -e "$checkpoint_dir"
   command=("$results/$variant-binary" "${common[@]}" "${settings[@]}" --save-dir "$checkpoint_dir")
   printf '%q ' "${command[@]}" > "$results/$name.command"
   printf '\n' >> "$results/$name.command"
   echo "$(date -u +%FT%TZ) Started $name"
   /usr/bin/time -v -o "$results/$name.time" "${command[@]}" > "$results/$name.log" 2>&1
   echo "$(date -u +%FT%TZ) Finished $name"
  done
 done
done
echo "$(date -u +%FT%TZ) All eight runs complete"
