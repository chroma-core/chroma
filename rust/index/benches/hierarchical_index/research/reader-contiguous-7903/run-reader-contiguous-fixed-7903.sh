#!/bin/bash
set -euo pipefail
root=/mnt/data/reader-contiguous-7903
out=$root/results
exec 9>/mnt/data/chroma-benchmark.lock
flock 9
test "$(cat "$out/exit-code")" = 0
trap 'code=$?; printf "%s\n" "$code" > "$out/fixed-exit-code"; date -u +%FT%TZ > "$out/fixed-finished-at"' EXIT
export HF_HOME=/mnt/data/huggingface HF_HUB_OFFLINE=1
snapshot=$root/index-baseline-1
sha256sum "$snapshot/checkpoint.json" > "$out/fixed-input-before.sha256"
for run in baseline-1 candidate-1; do
  variant=${run%-*}
  dir=$root/fixed-$run
  test ! -e "$dir"
  cp -al "$snapshot" "$dir"
  cp "$snapshot/checkpoint.json" "$dir/checkpoint.independent.json"
  mv "$dir/checkpoint.independent.json" "$dir/checkpoint.json"
  printf 'running-fixed-%s\n' "$run" > "$out/fixed-status"
  args=(--dataset wikipedia-en --metric l2 --checkpoint 40 --checkpoint-size 1000000 --branching-factor 100 --split-threshold 2048 --merge-threshold 512 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-beam-min 10 --write-beam-max 16 --write-level-taus _,_,0.30 --max-replicas 1 --write-rng-epsilon 0 --write-rng-factor 0 --read-beam-min 10 --read-beam-max 256 --read-level-taus _,_,_ --read-level-min-pcts 50,5,0 --recall-rerank-vectors 1,8 --recall-tau-values 0.7,1,2 --num-queries 100 --max-cache-bytes 1073741824 --compute-gt-clusters false --resume --save-dir "$dir")
  printf '%q ' "$out/binary-$variant" "${args[@]}" > "$out/fixed-$run.command"
  date -u +%FT%TZ > "$out/fixed-$run.started-at"
  /usr/bin/time -v -o "$out/fixed-$run.time" "$out/binary-$variant" "${args[@]}" > "$out/fixed-$run.log" 2>&1
  printf '0\n' > "$out/fixed-$run.exit-code"
  date -u +%FT%TZ > "$out/fixed-$run.finished-at"
done
sha256sum "$snapshot/checkpoint.json" > "$out/fixed-input-after.sha256"
cmp "$out/fixed-input-before.sha256" "$out/fixed-input-after.sha256"
printf 'complete\n' > "$out/fixed-status"
