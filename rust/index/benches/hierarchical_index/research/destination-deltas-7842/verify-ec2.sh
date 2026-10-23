#!/usr/bin/env bash
set -euo pipefail
results=/home/ubuntu/benchmark-data/destination-deltas-7842
while ! rg -q '^Destination-delta benchmarks completed$' "$results/progress.log"; do
    if ! kill -0 193857 2>/dev/null; then echo 'Timing runner stopped before completion'; exit 1; fi
    sleep 15
done
for variant in control candidate; do
    replay=/mnt/data/destination-deltas-7842-verify-$variant
    test ! -e "$replay"
    cp -a --reflink=auto /mnt/data/balance-diagnostics-source "$replay"
    "$results/$variant-binary" --metric l2 --checkpoint 10 --checkpoint-size 100000 --write-navigation fp --fp-npa --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false --dataset ms-marco --threads 64 --balance-threads 64 --write-beam-tau 1.5 --write-rng-factor 4 --resume --save-dir "$replay" --verify-valid-postings > "$results/verify-$variant.log" 2>&1
    python3 - "$results/verify-$variant.log" <<'PY'
import re,sys
s=open(sys.argv[1]).read()
pre=re.findall(r'Pre-commit reachable valid IDs: ([\d,]+) \| indexed embeddings without one: ([\d,]+) \| missing child nodes: ([\d,]+)',s)
post=re.findall(r'Post-reopen reachable valid IDs: ([\d,]+) \| missing child nodes: ([\d,]+)',s)
n=lambda x:int(x.replace(',',''))
assert [tuple(map(n,row)) for row in pre]==[(900000,0,0),(1000000,0,0)],pre
assert [tuple(map(n,row)) for row in post]==[(1000000,0)],post
print(sys.argv[1], 'passed reachable-posting checks')
PY
done
echo 'Posting validity checks completed'
