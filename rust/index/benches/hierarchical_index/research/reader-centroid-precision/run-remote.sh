#!/usr/bin/env bash
set -euo pipefail
export PATH=/home/ubuntu/.cargo/bin:$PATH
export CARGO_TARGET_DIR=/home/ubuntu/chroma-benchmark/target
RESULTS=/home/ubuntu/benchmark-data/reader-centroid-precision
SOURCE=/home/ubuntu/reader-centroid-precision-d4d8c7b6e
INDEX=/mnt/data/wikipedia-40m-baseline-20261009
GT=/mnt/benchmark-source/wikipedia-40m-current/wikipedia_1m_40m_ground_truth.parquet
mkdir -p "$RESULTS"
exec 9>/mnt/data/chroma-benchmark.lock
if ! flock -n 9; then
  printf '%s\n' 'Benchmark node is busy; no competing job was started.' > "$RESULTS/status"
  exit 75
fi
trap 'code=$?; printf "%s\n" "$code" > "$RESULTS/exit-code"' EXIT
printf '%s\n' 'Preparing pinned source and verifying inputs' > "$RESULTS/status"
test -f "$INDEX/checkpoint.json"
test -f "$GT"
printf '%s  %s\n' d4863b7df4213fb44f007180c99f5f65c14d19573d0af6473e2028602dcbdfdf "$GT" | sha256sum --check
if test -e "$SOURCE"; then
  printf '%s\n' 'Isolated source directory already exists; refusing to replace it.'
  exit 1
fi
mkdir "$SOURCE"
tar -xzf /home/ubuntu/reader-centroid-precision-d4d8c7b6e.tar.gz -C "$SOURCE"
printf '%s\n' d4d8c7b6ea7aa1b2d99313733622caae8fbcc02d > "$RESULTS/source.sha"
sha256sum /home/ubuntu/reader-centroid-precision-d4d8c7b6e.tar.gz > "$RESULTS/source-archive.sha256"
cp "$INDEX/checkpoint.json" "$RESULTS/checkpoint.json"
python3 - "$INDEX" "$RESULTS/input-manifest-before.json" <<'PY'
import os,sys,json
root=sys.argv[1]
files=[]
for parent,dirs,names in os.walk(root):
 for name in names:
  p=os.path.join(parent,name); s=os.stat(p)
  files.append((os.path.relpath(p,root),s.st_ino,s.st_size,s.st_mtime_ns))
json.dump(sorted(files),open(sys.argv[2],'w'))
PY
cd "$SOURCE"
cargo +stable --version > "$RESULTS/toolchain.txt"
rustc +stable -vV >> "$RESULTS/toolchain.txt"
printf '%s\n' 'Compiling reader precision benchmark' > "$RESULTS/status"
cargo +stable build --release -p chroma-index --bench hierarchical_reader_precision -j 32 --message-format=json > "$RESULTS/build-artifacts.jsonl" 2> "$RESULTS/build.log"
BINARY=$(python3 - "$RESULTS/build-artifacts.jsonl" <<'PYCODE'
import json,sys
artifacts=[json.loads(line) for line in open(sys.argv[1]) if line.startswith('{')]
paths=[item['executable'] for item in artifacts if item.get('reason')=='compiler-artifact' and item.get('target',{}).get('name')=='hierarchical_reader_precision' and item.get('executable')]
assert paths, 'Cargo did not report benchmark executable'
print(paths[-1])
PYCODE
)
test -n "$BINARY"
cp "$BINARY" "$RESULTS/binary"
sha256sum "$RESULTS/binary" > "$RESULTS/binary.sha256"
"$RESULTS/binary" --help > "$RESULTS/help.txt"
printf '%s\n' 'Running 40M read-only precision and beam sweep' > "$RESULTS/status"
date -u +%FT%TZ > "$RESULTS/started-at"
COMMAND=("$RESULTS/binary" --index "$INDEX" --ground-truth "$GT" --output "$RESULTS/results.jsonl" --internal-beams 64,256 --leaf-beams 8,16,32,64,128,256 --rerank 1,2,4,8,16 --queries 100 --threads 32 --max-cache-bytes 1073741824)
printf '%q ' "${COMMAND[@]}" > "$RESULTS/command.txt"
printf '\n' >> "$RESULTS/command.txt"
/usr/bin/time -v -o "$RESULTS/process.time" "${COMMAND[@]}" > "$RESULTS/full-output.txt" 2>&1
date -u +%FT%TZ > "$RESULTS/finished-at"
printf '%s\n' 'Verifying unchanged input checkpoint' > "$RESULTS/status"
python3 - "$INDEX" "$RESULTS/input-manifest-before.json" "$RESULTS/input-manifest-after.json" <<'PY'
import os,sys,json
root=sys.argv[1]; files=[]
for parent,dirs,names in os.walk(root):
 for name in names:
  p=os.path.join(parent,name); s=os.stat(p)
  files.append((os.path.relpath(p,root),s.st_ino,s.st_size,s.st_mtime_ns))
after=sorted(files); json.dump(after,open(sys.argv[3],'w'))
assert json.load(open(sys.argv[2]))==[list(row) for row in after], 'Input index changed'
print('Input index file manifest unchanged')
PY
printf '%s\n' 'Completed; checkpoint unchanged' > "$RESULTS/status"
