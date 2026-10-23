#!/bin/bash
set -euo pipefail
exec 9>/mnt/data/chroma-benchmark.lock
flock 9
out=/home/ubuntu/benchmark-data/bulk-version-cache-fixes
source=/mnt/data/bulk-version-cache-fixes-source-21m
copy=/mnt/data/bulk-version-cache-fixes-replay-21m
python3 - "$source" <<'PY'
import json,sys,pathlib
m=json.loads((pathlib.Path(sys.argv[1])/'checkpoint.json').read_text())
assert m['total_vectors']==21000000 and m['checkpoint_idx']==20,m
PY
export PATH=/home/ubuntu/.cargo/bin:$PATH HF_HOME=/mnt/data/huggingface HF_HUB_OFFLINE=1
cd /home/ubuntu/chroma-metadata-page-version-fill
gt=/home/ubuntu/.cache/wikipedia_en/ground_truth.parquet
cp "$gt" "$out/ground-truth-before-higher.parquet"
trap 'code=$?; cp "$out/ground-truth-before-higher.parquet" "$gt"; echo "$code" > "$out/higher.status"' EXIT
cp /home/ubuntu/benchmark-data/wikipedia-40m-current/wikipedia_1m_40m_ground_truth.parquet "$gt"
args=(--dataset wikipedia-en --metric l2 --checkpoint 22 --checkpoint-size 1000000 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0 --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --compute-gt-clusters false --resume --save --save-dir "$copy" --max-cache-bytes 1073741824)
for spec in baseline-1 page-1 bulk-1 bulk-2 page-2 baseline-2; do
 # Only this task's replay copy is recycled; the shared21M fixture stays immutable.
 test "$copy" = /mnt/data/bulk-version-cache-fixes-replay-21m
 if [[ -e "$copy" ]]; then rm -rf -- "$copy"; fi
 cp -a --reflink=auto "$source" "$copy"
 python3 - "$source" "$copy" <<'PY'
import sys,pathlib
s,d=map(pathlib.Path,sys.argv[1:])
for p in s.rglob('*'):
 if p.is_file(): assert p.stat().st_ino!=(d/p.relative_to(s)).stat().st_ino
assert (s/'checkpoint.json').read_bytes()==(d/'checkpoint.json').read_bytes()
PY
 sync
 mode=${spec%-*}
 binary="$out/$mode-binary"
 if [[ $mode == baseline ]]; then binary=/home/ubuntu/benchmark-data/wikipedia-40m-current/binary; fi
 printf '%q ' "$binary" "${args[@]}" > "$out/higher-$spec.command.txt"
 /usr/bin/time -v -o "$out/higher-$spec.time" timeout 900 "$binary" "${args[@]}" > "$out/higher-$spec.log" 2>&1
 echo 0 > "$out/higher-$spec.status"
 cp "$copy/checkpoint.json" "$out/higher-$spec.checkpoint.json"
done
find "$source" -type f -printf '%P %s %T@ %i\n' | sort > "$out/source-21m-files.after.txt"
cmp "$out/source-21m-files.before.txt" "$out/source-21m-files.after.txt"
python3 - <<'PY'
import pathlib,json,hashlib
out=pathlib.Path('/home/ubuntu/benchmark-data/bulk-version-cache-fixes')
p=pathlib.Path('/mnt/data/wikipedia-40m-current')
files=sorted((str(f.relative_to(p)),f.stat().st_size,f.stat().st_mtime_ns,f.stat().st_ino) for f in p.rglob('*') if f.is_file())
after={'checkpoint':json.loads((p/'checkpoint.json').read_text()),'files':files}
assert json.loads(json.dumps(after))==json.loads((out/'40m-before-higher.json').read_text())
(out/'40m-after-higher-preservation.json').write_text(json.dumps({'unchanged':True,'file_count':len(files),'checkpoint_sha256':hashlib.sha256((p/'checkpoint.json').read_bytes()).hexdigest()},indent=2))
PY
