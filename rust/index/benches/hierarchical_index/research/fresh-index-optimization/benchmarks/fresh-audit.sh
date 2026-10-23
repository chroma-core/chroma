#!/usr/bin/env bash
set -euo pipefail
exec 9>/mnt/data/chroma-benchmark.lock
flock -n 9
r=/home/ubuntu/benchmark-data/fresh-index-20261008
binary=/home/ubuntu/benchmark-data/wikipedia-10m-scaling-baseline-20261008/audit-binary
export HF_HOME=/mnt/data/huggingface HF_HUB_OFFLINE=1 SCALING_EXACT_POSTING_AUDIT=1
sha256sum "$binary" > "$r/audit-binary.sha256"
for meta in "$r/"*.checkpoint.json; do
 name=$(basename "$meta" .checkpoint.json)
 dataset=wikipedia-en; [[ "$name" == ms-marco-* ]] && dataset=ms-marco
 args=(--dataset "$dataset" --metric l2 --checkpoint 10 --checkpoint-size 100000 --resume --no-commit --save-dir "/mnt/data/fresh-index-20261008-$name" --write-navigation fp --fp-npa --num-queries 0 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false)
 if [[ "$dataset" == wikipedia-en ]]; then
  args+=(--threads 32 --balance-threads 32 --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0)
 else
  args+=(--threads 64 --balance-threads 64 --write-beam-tau 1.5 --write-rng-factor 4)
 fi
 printf '%q ' "$binary" "${args[@]}" > "$r/$name.audit.command"
 /usr/bin/time -v -o "$r/$name.audit.time" "$binary" "${args[@]}" > "$r/$name.audit.log" 2>&1
 rg -q '^AUDIT PASSED: 1000000 persisted root-reachable valid IDs equal' "$r/$name.audit.log"
 echo "AUDIT $name passed $(date -u +%FT%TZ)"
done
echo 0 > "$r/audit-exit-status"
