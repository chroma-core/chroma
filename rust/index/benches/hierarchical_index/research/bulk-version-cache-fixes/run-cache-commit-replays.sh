#!/bin/bash
set -euo pipefail
exec 9>/mnt/data/chroma-benchmark.lock
flock 9
export PATH=/home/ubuntu/.cargo/bin:$PATH HF_HOME=/mnt/data/huggingface HF_HUB_OFFLINE=1
out=/home/ubuntu/benchmark-data/bulk-version-cache-fixes
fixture=/mnt/data/large-index-investigation-components-9m
original=/mnt/data/wikipedia-10m-scaling-snapshots-20261008/9000000
cd /home/ubuntu/chroma-metadata-page-version-fill
args=(--dataset wikipedia-en --metric l2 --checkpoint 10 --checkpoint-size 1000000 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0 --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --compute-gt-clusters false --resume --save --save-dir "$fixture" --max-cache-bytes 1073741824)
for mode in baseline page bulk; do
 # This directory is the investigator-owned independent copy, explicitly released for reuse.
 test "$fixture" = /mnt/data/large-index-investigation-components-9m
 test -f "$original/checkpoint.json"
 rm -rf -- "$fixture"
 cp -a --reflink=auto "$original" "$fixture"
 python3 - "$original" "$fixture" <<'PY'
import os,sys
from pathlib import Path
s,d=map(Path,sys.argv[1:])
for p in s.rglob('*'):
 if p.is_file():
  q=d/p.relative_to(s)
  assert p.stat().st_ino != q.stat().st_ino,(p,q)
assert (s/'checkpoint.json').read_bytes()==(d/'checkpoint.json').read_bytes()
PY
 binary="$out/$mode-binary"
 if [[ $mode == baseline ]]; then binary=/home/ubuntu/benchmark-data/wikipedia-40m-current/binary; fi
 printf '%q ' "$binary" "${args[@]}" > "$out/commit-$mode.command.txt"
 /usr/bin/time -v -o "$out/commit-$mode.time" timeout 600 "$binary" "${args[@]}" > "$out/commit-$mode.log" 2>&1
 echo 0 > "$out/commit-$mode.status"
 cp "$fixture/checkpoint.json" "$out/commit-$mode.checkpoint.json"
done
echo 0 > "$out/commits.status"
