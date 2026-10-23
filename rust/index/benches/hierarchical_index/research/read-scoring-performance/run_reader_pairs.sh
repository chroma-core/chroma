#!/usr/bin/env bash
set -euo pipefail
cd /home/ubuntu/chroma-read-scoring
binary=/home/ubuntu/chroma-benchmark/target/release/deps/hierarchical_spann_profile_quantized-141263a897e45414
results=/mnt/data/read-scoring-results
common=(--metric l2 --checkpoint 10 --checkpoint-size 100000 --write-navigation fp --fp-npa --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false --resume --no-commit)
for workers in 32 1; do
  repeats=3
  if [[ $workers == 1 ]]; then repeats=1; fi
  for ((repeat=1;repeat<=repeats;repeat++)); do
    for dataset in wikipedia msmarco; do
      extra=(--dataset wikipedia-en --threads 32 --balance-threads 32 --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0)
      if [[ $dataset == msmarco ]]; then
        extra=(--dataset ms-marco --threads 64 --balance-threads 64 --write-beam-tau 1.5 --write-rng-factor 4)
      fi
      modes=(baseline candidate)
      if ((repeat%2 == 0)); then modes=(candidate baseline); fi
      for mode in "${modes[@]}"; do
        label="$dataset-$workers-$mode-$repeat"
        cmd=(env READ_SCORING_THREADS="$workers")
        if [[ $mode == baseline ]]; then cmd+=(READ_SCORING_BASELINE=1); fi
        cmd+=("$binary" "${common[@]}" "${extra[@]}" --save-dir "/mnt/data/pr7856-perf-20261006-$dataset-candidate-1")
        printf '%q ' "${cmd[@]}" > "$results/$label.command"
        printf '\n' >> "$results/$label.command"
        "${cmd[@]}" > "$results/$label.log" 2> "$results/$label.stderr"
        echo "Finished $label"
      done
    done
  done
done
