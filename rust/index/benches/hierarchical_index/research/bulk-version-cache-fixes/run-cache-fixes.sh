#!/bin/bash
set -euo pipefail
exec 9>/mnt/data/chroma-benchmark.lock
flock 9
export PATH=/home/ubuntu/.cargo/bin:$PATH HF_HOME=/mnt/data/huggingface HF_HUB_OFFLINE=1
out=/home/ubuntu/benchmark-data/bulk-version-cache-fixes
cd /home/ubuntu/chroma-metadata-page-version-fill
args=(--dataset wikipedia-en --metric l2 --checkpoint 10 --checkpoint-size 1000000 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0 --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --compute-gt-clusters false --resume --no-commit --save-dir /mnt/data/large-index-investigation-components-9m --max-cache-bytes 1073741824)
for spec in baseline-1 page-1 bulk-1 bulk-2 page-2 baseline-2; do
 mode=${spec%-*}
 binary="$out/$mode-binary"
 if [[ $mode == baseline ]]; then binary=/home/ubuntu/benchmark-data/wikipedia-40m-current/binary; fi
 printf '%q ' "$binary" "${args[@]}" > "$out/$spec.command.txt"
 /usr/bin/time -v -o "$out/$spec.time" timeout 600 "$binary" "${args[@]}" > "$out/$spec.log" 2>&1
 echo 0 > "$out/$spec.status"
done
echo 0 > "$out/replays.status"
