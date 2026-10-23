#!/usr/bin/env bash
set -euo pipefail
r=/home/ubuntu/benchmark-data/deferred-destination-balancing
export HF_HOME=/mnt/data/huggingface HF_HUB_OFFLINE=1 SCALING_EXACT_POSTING_AUDIT=1
binary=/home/ubuntu/benchmark-data/wikipedia-10m-scaling-baseline-20261008/audit-binary
sha256sum "$binary" > "$r/audit-binary.sha256"
for name in candidate-2 candidate-3 candidate-4; do
 "$binary" --dataset wikipedia-en --metric l2 --checkpoint 10 --checkpoint-size 1000000 --resume --no-commit --save-dir "/mnt/data/deferred-destination-balancing-$name" --write-navigation fp --fp-npa --threads 32 --balance-threads 32 --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0 --num-queries 0 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false > "$r/$name.audit.log" 2>&1
done
