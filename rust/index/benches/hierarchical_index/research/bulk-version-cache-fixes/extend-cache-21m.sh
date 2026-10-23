#!/bin/bash
set -euo pipefail
exec 9>/mnt/data/chroma-benchmark.lock
flock 9
out=/home/ubuntu/benchmark-data/bulk-version-cache-fixes
fixture=/mnt/data/bulk-version-cache-fixes-source-21m
source=/mnt/data/large-index-investigation-components-9m
python3 - "$source" <<'PY'
import json,sys,pathlib
m=json.loads((pathlib.Path(sys.argv[1])/'checkpoint.json').read_text())
assert m['total_vectors']==10000000 and m['checkpoint_idx']==9,m
PY
mv "$out/generate-21m.log" "$out/generate-fresh-prefix.log"
mv "$out/generate-21m.time" "$out/generate-fresh-prefix.time"
# This is only the new fixture created by this task's stopped preparation process.
test "$fixture" = /mnt/data/bulk-version-cache-fixes-source-21m
rm -rf -- "$fixture"
cp -a --reflink=auto "$source" "$fixture"
python3 - "$source" "$fixture" <<'PY'
import sys,pathlib
s,d=map(pathlib.Path,sys.argv[1:])
for p in s.rglob('*'):
 if p.is_file(): assert p.stat().st_ino!=(d/p.relative_to(s)).stat().st_ino
assert (s/'checkpoint.json').read_bytes()==(d/'checkpoint.json').read_bytes()
PY
export PATH=/home/ubuntu/.cargo/bin:$PATH HF_HOME=/mnt/data/huggingface HF_HUB_OFFLINE=1
cd /home/ubuntu/chroma-metadata-page-version-fill
args=(--dataset wikipedia-en --metric l2 --checkpoint 21 --checkpoint-size 1000000 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0 --num-queries 0 --compute-gt-clusters false --resume --save --save-dir "$fixture" --max-cache-bytes 1073741824)
printf '%q ' "$out/page-binary" "${args[@]}" > "$out/generate-21m.command.txt"
/usr/bin/time -v -o "$out/generate-21m.time" "$out/page-binary" "${args[@]}" > "$out/generate-21m.log" 2>&1
cp "$fixture/checkpoint.json" "$out/source-21m.checkpoint.json"
find "$fixture" -type f -printf '%P %s %T@ %i\n' | sort > "$out/source-21m-files.before.txt"
echo 0 > "$out/generate-21m.status"
