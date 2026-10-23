#!/usr/bin/env bash
set -euo pipefail
export PATH=/home/ubuntu/.cargo/bin:$PATH
export CARGO_TARGET_DIR=/home/ubuntu/chroma-benchmark/target
R=/home/ubuntu/benchmark-data/prepared-four-bit-navigation-b635eee6f
S=/home/ubuntu/prepared-four-bit-navigation-b635eee6f
INDEX=/mnt/data/wikipedia-40m-baseline-20261009
GT=/mnt/benchmark-source/wikipedia-40m-current/wikipedia_1m_40m_ground_truth.parquet
exec 9>/mnt/data/chroma-benchmark.lock
flock -n 9 || exit 75
mkdir "$R"
trap 'printf "%s\n" "$?" > "$R/exit-code"' EXIT
mkdir "$S"
tar -xzf /home/ubuntu/source-b635eee6f.tar.gz -C "$S"
printf '%s\n' b635eee6f83e56ecf6c8e0c1eada25b7b66b4bd3 > "$R/source.sha"
cp /home/ubuntu/benchmark-data/prepared-four-bit-navigation-7767a4c1b/input-manifest-before.json "$R/input-manifest-before.json"
cd "$S"
printf '%s\n' 'Building query benchmark' > "$R/status"
cargo +stable build --release -p chroma-index --bench hierarchical_reader_precision -j32 --message-format=json > "$R/build-artifacts.jsonl" 2> "$R/build.log"
BINARY=$(python3 - "$R/build-artifacts.jsonl" <<'PY'
import json,sys
rows=[json.loads(l) for l in open(sys.argv[1]) if l.startswith('{')]
print([r['executable'] for r in rows if r.get('reason')=='compiler-artifact' and r.get('target',{}).get('name')=='hierarchical_reader_precision' and r.get('executable')][-1])
PY
)
cp "$BINARY" "$R/binary"
sha256sum "$R/binary" > "$R/binary.sha256"
COMMON=(--index "$INDEX" --ground-truth "$GT" --queries 100 --threads 32 --max-cache-bytes 1073741824 --configurations 64:112:4,64:128:8,256:192:8,256:240:4,256:256:8)
for RUN in 0 1 2; do
  if test "$RUN" = 1; then
    MODES=4bit-packed-fp,4bit-packed-4bit,4bit-planes-4bit,1bit-4bit
  else
    MODES=1bit-4bit,4bit-planes-4bit,4bit-packed-4bit,4bit-packed-fp
  fi
  printf 'Running query comparison %s of 3\n' "$((RUN+1))" > "$R/status"
  printf '%q ' "$R/binary" "${COMMON[@]}" --navigation "$MODES" --output "$R/run-$RUN.jsonl" > "$R/command-$RUN.txt"
  printf '\n' >> "$R/command-$RUN.txt"
  /usr/bin/time -v -o "$R/run-$RUN.time" "$R/binary" "${COMMON[@]}" --navigation "$MODES" --output "$R/run-$RUN.jsonl" > "$R/run-$RUN-output.txt" 2>&1
done
python3 - "$INDEX" "$R/input-manifest-before.json" <<'PY'
import os,json,sys
files=[]
for parent,dirs,names in os.walk(sys.argv[1]):
 for name in names:
  p=os.path.join(parent,name);s=os.stat(p);files.append([os.path.relpath(p,sys.argv[1]),s.st_ino,s.st_size,s.st_mtime_ns])
assert sorted(files)==json.load(open(sys.argv[2]))
print('All input checkpoint files unchanged')
PY
printf '%s\n' 'Completed; checkpoint unchanged' > "$R/status"
