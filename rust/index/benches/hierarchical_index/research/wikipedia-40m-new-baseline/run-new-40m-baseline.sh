#!/bin/bash
set -euo pipefail
exec 9>/mnt/data/chroma-benchmark.lock
flock 9
out=/home/ubuntu/benchmark-data/wikipedia-40m-new-baseline
test "$(cat "$out/snapshot-validation.status")" = 0
test "$(cat "$out/cleanup.status")" = 0
test ! -e /mnt/data/wikipedia-40m-baseline-20261009
test ! -e /mnt/data/wikipedia-40m-baseline-20261009-snapshots
export PATH=/home/ubuntu/.cargo/bin:$PATH HF_HOME=/mnt/data/huggingface HF_HUB_OFFLINE=1
cd /home/ubuntu/chroma-wikipedia-40m-new-baseline
gt=/home/ubuntu/.cache/wikipedia_en/ground_truth.parquet
cp "$gt" "$out/ground-truth-before.parquet"
trap 'code=$?; cp "$out/ground-truth-before.parquet" "$gt"; echo "$code" > "$out/benchmark.status"; date -u +%FT%TZ > "$out/benchmark-finished-at"' EXIT
cp /home/ubuntu/benchmark-data/wikipedia-40m-current/wikipedia_1m_40m_ground_truth.parquet "$gt"
args=(--dataset wikipedia-en --metric l2 --checkpoint 40 --checkpoint-size 1000000 --branching-factor 100 --split-threshold 2048 --merge-threshold 512 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-beam-min 10 --write-beam-max 16 --write-level-taus _,_,0.30 --max-replicas 1 --write-rng-epsilon 0 --write-rng-factor 0 --read-beam-min 10 --read-beam-max 256 --read-level-taus _,_,_ --read-level-min-pcts 50,5,0 --recall-rerank-vectors 1,8 --recall-tau-values 0.7,1,2 --num-queries 100 --max-cache-bytes 1073741824 --compute-gt-clusters false --save --save-dir /mnt/data/wikipedia-40m-baseline-20261009 --checkpoint-snapshots-dir /mnt/data/wikipedia-40m-baseline-20261009-snapshots)
printf '%q ' "$out/binary" "${args[@]}" > "$out/command.txt"
date -u +%FT%TZ > "$out/benchmark-started-at"
/usr/bin/time -v -o "$out/benchmark.time" python3 "$out/observe-baseline.py" "$out/process.io.json" "$out/binary" "${args[@]}" > "$out/full-cli-output.txt" 2>&1
python3 "$out/validate-snapshot-manifests.py" > "$out/snapshot-validation.log" 2>&1
