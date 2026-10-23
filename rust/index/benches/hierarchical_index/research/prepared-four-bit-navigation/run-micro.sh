#!/usr/bin/env bash
set -euo pipefail
export PATH=/home/ubuntu/.cargo/bin:$PATH
export CARGO_TARGET_DIR=/home/ubuntu/chroma-benchmark/target
R=/home/ubuntu/benchmark-data/prepared-four-bit-navigation-7767a4c1b
S=/home/ubuntu/prepared-four-bit-navigation-7767a4c1b
INDEX=/mnt/data/wikipedia-40m-baseline-20261009
GT=/mnt/benchmark-source/wikipedia-40m-current/wikipedia_1m_40m_ground_truth.parquet
exec 9>/mnt/data/chroma-benchmark.lock
flock -n 9 || exit 75
mkdir "$R"
trap 'printf "%s\n" "$?" > "$R/exit-code"' EXIT
mkdir "$S"
tar -xzf /home/ubuntu/source-7767a4c1b.tar.gz -C "$S"
printf '%s\n' 7767a4c1b4fc8441e7fa8cfed1405b9d891be8a4 > "$R/source.sha"
printf '%s  %s\n' d4863b7df4213fb44f007180c99f5f65c14d19573d0af6473e2028602dcbdfdf "$GT" | sha256sum --check > "$R/ground-truth-verification.txt"
lscpu > "$R/cpu.txt"
cargo +stable --version > "$R/toolchain.txt"
rustc +stable -vV >> "$R/toolchain.txt"
python3 - "$INDEX" "$R/input-manifest-before.json" <<'PY'
import os,json,sys
files=[]
for parent,dirs,names in os.walk(sys.argv[1]):
 for name in names:
  p=os.path.join(parent,name);s=os.stat(p);files.append([os.path.relpath(p,sys.argv[1]),s.st_ino,s.st_size,s.st_mtime_ns])
json.dump(sorted(files),open(sys.argv[2],'w'))
PY
cd "$S"
printf '%s\n' 'Testing x86 SIMD kernels' > "$R/status"
cargo +stable test --release -p chroma-index --lib prepared_four_bit -j32 > "$R/x86-tests.log" 2>&1
printf '%s\n' 'Checking x86 kernels with Clippy' > "$R/status"
cargo +stable clippy -p chroma-index --lib -j32 -- -D warnings -D clippy::all > "$R/x86-clippy.log" 2>&1
printf '%s\n' 'Building scoring benchmark' > "$R/status"
cargo +stable build --release -p chroma-index --bench hierarchical_reader_precision -j32 --message-format=json > "$R/build-artifacts.jsonl" 2> "$R/build.log"
BINARY=$(python3 - "$R/build-artifacts.jsonl" <<'PY'
import json,sys
rows=[json.loads(l) for l in open(sys.argv[1]) if l.startswith('{')]
print([r['executable'] for r in rows if r.get('reason')=='compiler-artifact' and r.get('target',{}).get('name')=='hierarchical_reader_precision' and r.get('executable')][-1])
PY
)
cp "$BINARY" "$R/binary"
sha256sum "$R/binary" > "$R/binary.sha256"
printf '%s\n' 'Running real-centroid scoring measurements' > "$R/status"
/usr/bin/time -v -o "$R/micro.time" "$R/binary" --index "$INDEX" --ground-truth "$GT" --output "$R/micro.jsonl" --navigation 1bit-4bit,4bit-4bit,4bit-planes-4bit,4bit-packed-4bit,4bit-fp,4bit-packed-fp --queries 100 --threads 32 --repeats 3 --micro-only --micro-centroids 4096 > "$R/micro-output.txt" 2>&1
printf '%s\n' 'Microbenchmark completed' > "$R/status"
