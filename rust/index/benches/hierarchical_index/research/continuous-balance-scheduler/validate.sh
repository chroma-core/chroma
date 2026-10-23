#!/usr/bin/env bash
set -euo pipefail
exec 9>/mnt/data/chroma-benchmark.lock
flock -n 9 || { echo 'Benchmark host is reserved'; exit 1; }
r=/home/ubuntu/benchmark-data/continuous-scheduler
export HF_HOME=/mnt/data/huggingface HF_HUB_OFFLINE=1
bash /home/ubuntu/benchmark-data/deferred-destination-balancing/audit.sh
for n in 1 2; do
 command=(/home/ubuntu/benchmark-data/wikipedia-10m-scaling-baseline-20261008/audit-binary --dataset wikipedia-en --metric l2 --checkpoint 10 --checkpoint-size 1000000 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0 --num-queries 0 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false --resume --no-commit --save-dir "/mnt/data/continuous-scheduler-final-$n")
 printf '%q ' "${command[@]}" > "$r/audit-final-$n.command"
 SCALING_EXACT_POSTING_AUDIT=1 /usr/bin/time -v -o "$r/audit-final-$n.time" "${command[@]}" > "$r/audit-final-$n.log" 2>&1
 rg -q '^AUDIT PASSED: 10000000 persisted root-reachable valid IDs equal' "$r/audit-final-$n.log"
 echo "audit-final-$n complete $(date -u +%FT%TZ)"
done
new=/mnt/data/continuous-scheduler-msmarco-validation
 test ! -e "$new"
command=("$r/final-binary" --dataset ms-marco --metric l2 --checkpoint 10 --checkpoint-size 100000 --threads 64 --balance-threads 64 --write-navigation fp --fp-npa --write-beam-tau 1.5 --write-rng-factor 4 --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false --validate-postings --save --save-dir "$new")
printf '%q ' "${command[@]}" > "$r/msmarco-validation.command"
/usr/bin/time -v -o "$r/msmarco-validation.time" "${command[@]}" > "$r/msmarco-validation.log" 2>&1
rg -q '\[posting validation\] after reopen: 1000000 root-reachable valid ids; all child links resolve' "$r/msmarco-validation.log"
python3 - "$new/checkpoint.json" <<'PY'
import json,sys
m=json.load(open(sys.argv[1])); assert m['checkpoint_idx']==9 and m['total_vectors']==1000000
PY
sha256sum "$r/final-binary" "$r/baseline-binary" "$r/candidate-binary" "$r/exploratory-full-npa-binary" /home/ubuntu/benchmark-data/wikipedia-10m-scaling-baseline-20261008/audit-binary > "$r/all-binaries.sha256"
echo 0 > "$r/validation-exit-status"
echo "validation complete $(date -u +%FT%TZ)"
