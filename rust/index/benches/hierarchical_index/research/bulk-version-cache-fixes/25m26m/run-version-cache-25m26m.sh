#!/bin/bash
set -euo pipefail
exec 9>/mnt/data/chroma-benchmark.lock
flock 9
out=/home/ubuntu/benchmark-data/version-cache-25m26m
source=/mnt/data/version-cache-source-25m
test "$(cat "$out/source-25m.status")" = 0
export PATH=/home/ubuntu/.cargo/bin:$PATH HF_HOME=/mnt/data/huggingface HF_HUB_OFFLINE=1
gt=/home/ubuntu/.cache/wikipedia_en/ground_truth.parquet
cp "$gt" "$out/ground-truth-before.parquet"
trap 'code=$?; cp "$out/ground-truth-before.parquet" "$gt"; echo "$code" > "$out/runs.status"' EXIT
cp /home/ubuntu/benchmark-data/wikipedia-40m-current/wikipedia_1m_40m_ground_truth.parquet "$gt"
for spec in page:7874 bulk:7875; do
 mode=${spec%%:*}; pr=${spec#*:}; copy=/mnt/data/version-cache-pr${pr}-26m-verified
 test ! -e "$copy"
 cp -a --reflink=auto "$source" "$copy"
 python3 - "$source" "$copy" <<'PY'
import sys,pathlib,json
s,d=map(pathlib.Path,sys.argv[1:]);m=json.loads((s/'checkpoint.json').read_text());assert m['total_vectors']==25000000
for p in s.rglob('*'):
 if p.is_file():assert p.stat().st_ino!=(d/p.relative_to(s)).stat().st_ino
assert (s/'checkpoint.json').read_bytes()==(d/'checkpoint.json').read_bytes()
PY
 sync
 cd /home/ubuntu/chroma-${mode}-version-fill-pr${pr}
 args=(--dataset wikipedia-en --metric l2 --checkpoint 26 --checkpoint-size 1000000 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0 --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --compute-gt-clusters false --resume --save --save-dir "$copy" --max-cache-bytes 1073741824)
 printf '%q ' "$out/$mode-binary" "${args[@]}" > "$out/$mode-26m.command.txt"
 /usr/bin/time -v -o "$out/$mode-26m.time" python3 "$out/sample-process-io.py" "$out/$mode-26m.io.json" "$out/$mode-binary" "${args[@]}" > "$out/$mode-26m.full-cli-output.txt" 2>&1
 echo 0 > "$out/$mode-26m.status"
 cp "$copy/checkpoint.json" "$out/$mode-26m.checkpoint.json"
done
find "$source" -type f -printf '%P %s %T@ %i\n' | sort > "$out/source-25m-files.after.txt"
cmp "$out/source-25m-files.before.txt" "$out/source-25m-files.after.txt"
python3 - <<'PY'
import pathlib,json,hashlib
out=pathlib.Path('/home/ubuntu/benchmark-data/version-cache-25m26m');p=pathlib.Path('/mnt/data/wikipedia-40m-current')
files=sorted((str(f.relative_to(p)),f.stat().st_size,f.stat().st_mtime_ns,f.stat().st_ino) for f in p.rglob('*') if f.is_file())
after={'checkpoint':json.loads((p/'checkpoint.json').read_text()),'files':files}
assert json.loads(json.dumps(after))==json.loads(pathlib.Path('/home/ubuntu/benchmark-data/bulk-version-cache-fixes/40m-before-higher.json').read_text())
(out/'original-40m-preservation.json').write_text(json.dumps({'unchanged':True,'file_count':len(files),'checkpoint_sha256':hashlib.sha256((p/'checkpoint.json').read_bytes()).hexdigest()},indent=2))
PY
