#!/bin/bash
set -euo pipefail
exec 9>/mnt/data/chroma-benchmark.lock
flock 9
out=/home/ubuntu/benchmark-data/version-cache-25m26m
trap 'code=$?; echo "$code" > "$out/preparation.status"' EXIT
export PATH=/home/ubuntu/.cargo/bin:$PATH CARGO_TARGET_DIR=/home/ubuntu/chroma-benchmark/target HF_HOME=/mnt/data/huggingface HF_HUB_OFFLINE=1
for spec in page:chroma-page-version-fill-pr7874 bulk:chroma-bulk-version-fill-pr7875; do
 mode=${spec%%:*}; checkout=${spec#*:}
 cd /home/ubuntu/$checkout
 git status --porcelain > "$out/$mode-source.status"
 test ! -s "$out/$mode-source.status"
 git rev-parse HEAD > "$out/$mode-source.sha"
 cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run > "$out/$mode-build.log" 2>&1
 cp "$CARGO_TARGET_DIR/release/deps/hierarchical_spann_profile_quantized-141263a897e45414" "$out/$mode-binary"
 sha256sum "$out/$mode-binary" > "$out/$mode-binary.sha256"
done
source=/mnt/data/bulk-version-cache-fixes-source-21m
fixture=/mnt/data/version-cache-source-25m
test ! -e "$fixture"
cp -a --reflink=auto "$source" "$fixture"
python3 - "$source" "$fixture" <<'PY'
import sys,pathlib,json
s,d=map(pathlib.Path,sys.argv[1:]);m=json.loads((s/'checkpoint.json').read_text());assert m['total_vectors']==21000000
for p in s.rglob('*'):
 if p.is_file():assert p.stat().st_ino!=(d/p.relative_to(s)).stat().st_ino
assert (s/'checkpoint.json').read_bytes()==(d/'checkpoint.json').read_bytes()
PY
cd /home/ubuntu/chroma-page-version-fill-pr7874
args=(--dataset wikipedia-en --metric l2 --checkpoint 25 --checkpoint-size 1000000 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0 --num-queries 0 --compute-gt-clusters false --resume --save --save-dir "$fixture" --max-cache-bytes 1073741824)
printf '%q ' "$out/page-binary" "${args[@]}" > "$out/generate-25m.command.txt"
/usr/bin/time -v -o "$out/generate-25m.time" "$out/page-binary" "${args[@]}" > "$out/generate-25m.full-cli-output.txt" 2>&1
cp "$fixture/checkpoint.json" "$out/source-25m.checkpoint.json"
find "$fixture" -type f -printf '%P %s %T@ %i\n' | sort > "$out/source-25m-files.before.txt"
echo 0 > "$out/source-25m.status"
