#!/usr/bin/env bash
set -euo pipefail
r=/home/ubuntu/benchmark-data/continuous-scheduler
fixture=/mnt/data/wikipedia-10m-scaling-snapshots-20261008/9000000
old=/home/ubuntu/benchmark-data/read-scoring-10m-merged-22eb704
cp "$old/candidate-binary" "$r/baseline-binary"
cp /home/ubuntu/.cache/wikipedia_en/ground_truth.parquet "$r/default-ground-truth-before.parquet"
cp "$old/home/.cache/wikipedia_en/ground_truth.parquet" /home/ubuntu/.cache/wikipedia_en/ground_truth.parquet
sha256sum "$r/"*-binary > "$r/binary.sha256"
sha256sum "$fixture/checkpoint.json" > "$r/fixture-before.sha256"
export HF_HOME=/mnt/data/huggingface HF_HUB_OFFLINE=1
settings=(--dataset wikipedia-en --metric l2 --checkpoint 10 --checkpoint-size 1000000 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0 --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false)
for name in baseline-1 candidate-1; do
 variant=${name%-*}
 destination=/mnt/data/continuous-scheduler-$name
 test ! -e "$destination"
 cp -a --reflink=auto "$fixture" "$destination"
 python3 - "$fixture" "$destination" <<'PY'
import os,sys
from pathlib import Path
source,dest=map(Path,sys.argv[1:])
for p in dest.rglob('*'):
 if p.is_file(): assert os.stat(p).st_ino != os.stat(source/p.relative_to(dest)).st_ino
PY
 command=("$r/$variant-binary" "${settings[@]}" --save --resume --save-dir "$destination")
 printf '%q ' "${command[@]}" > "$r/$name.command"
 /usr/bin/time -v -o "$r/$name.time" "${command[@]}" > "$r/$name.log" 2>&1
 cp "$destination/checkpoint.json" "$r/$name.checkpoint.json"
 echo "$name complete $(date -u +%FT%TZ)"
done
sha256sum "$fixture/checkpoint.json" > "$r/fixture-after.sha256"
cmp "$r/fixture-before.sha256" "$r/fixture-after.sha256"
echo 0 > "$r/exit-status"
