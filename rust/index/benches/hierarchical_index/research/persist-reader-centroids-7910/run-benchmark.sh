#!/usr/bin/env bash
set -euo pipefail
root=/mnt/data/persist-reader-centroids-7910
out=$root/results
snapshot=/mnt/data/wikipedia-40m-baseline-20261009-snapshots/39000000
fixed=/mnt/data/wikipedia-40m-baseline-20261009
gt=/mnt/benchmark-source/wikipedia-40m-current/wikipedia_1m_40m_ground_truth.parquet
mkdir -p "$out"
printf 'waiting-for-lock\n' > "$out/status"
exec 9>/mnt/data/chroma-benchmark.lock
flock 9
trap 'code=$?; printf "%s\n" "$code" > "$out/exit-code"; date -u +%FT%TZ > "$out/finished-at"' EXIT
export PATH=/home/ubuntu/.cargo/bin:$PATH
export HF_HOME=/mnt/data/huggingface HF_HUB_OFFLINE=1
export CARGO_TARGET_DIR=$root/target CARGO_BUILD_JOBS=32
mkdir -p "$CARGO_TARGET_DIR"
# Copy compiled dependencies into a task-owned target directory; the shared
# checkout and its build artifacts remain untouched.
if test -d /home/ubuntu/chroma-benchmark/target/release; then
  cp -a --reflink=auto /home/ubuntu/chroma-benchmark/target/release "$CARGO_TARGET_DIR/"
fi
date -u +%FT%TZ > "$out/started-at"
lscpu > "$out/cpu.txt"
rustup run stable rustc --version > "$out/rustc.txt"
cp "$root/provenance.json" "$out/provenance.json"
df -h /mnt/data > "$out/disk-before.txt"
# This runner creates fresh task-owned directories and shares only immutable data
# files. Metadata is copied independently; blockstore forks use new UUIDs.
snapshot_manifest() {
  python3 - "$snapshot" "$fixed" "$gt" <<'MANIFEST'
import hashlib,json,sys
from pathlib import Path
result={}
for raw in sys.argv[1:3]:
 p=Path(raw); rows=[(str(f.relative_to(p)),f.stat().st_size,f.stat().st_mtime_ns) for f in sorted(p.rglob('*')) if f.is_file()]
 result[raw]={'manifest':hashlib.sha256(json.dumps(rows).encode()).hexdigest(),'metadata':hashlib.sha256((p/'checkpoint.json').read_bytes()).hexdigest(),'files':len(rows)}
result['gt']=hashlib.sha256(Path(sys.argv[3]).read_bytes()).hexdigest()
print(json.dumps(result,sort_keys=True))
MANIFEST
}
snapshot_manifest > "$out/protected-before.json"
copy_index() {
  python3 - "$1" "$2" <<'COPY'
import os,shutil,sys
from pathlib import Path
src,dst=map(Path,sys.argv[1:]); assert not dst.exists(); assert str(dst).startswith('/mnt/data/persist-reader-centroids-7910/')
dst.mkdir(); shutil.copy2(src/'checkpoint.json',dst/'checkpoint.json'); shutil.copytree(src/'data',dst/'data',copy_function=os.link)
assert (src/'checkpoint.json').stat().st_ino != (dst/'checkpoint.json').stat().st_ino
COPY
}
run_helper() {
  local variant=$1 label=$2 action=$3 dir=$4 rounds=$5
  printf 'running-%s\n' "$label" > "$out/status"
  /usr/bin/time -v -o "$out/$label.time" "$out/profile-$variant" "$action" "$dir" "$rounds" > "$out/$label.jsonl" 2> "$out/$label.stderr"
}
for variant in baseline candidate; do
  src=$root/source-$variant
  mkdir -p "$src"; tar -xzf "$root/source-$variant.tar.gz" -C "$src"
  cp "$root/hierarchical_centroid_persistence_profile.rs" "$src/rust/index/benches/"
  python3 - "$src" <<'PATCH'
from pathlib import Path
import sys
p=Path(sys.argv[1])/'rust/index/Cargo.toml'; p.write_text(p.read_text()+'\n[[bench]]\nname = "hierarchical_centroid_persistence_profile"\nharness = false\n')
p=Path(sys.argv[1])/'rust/index/benches/datasets/wikipedia.rs'
s=p.read_text(); old='cache_dir().join("ground_truth.parquet")'; assert s.count(old)==1
p.write_text(s.replace(old,'PathBuf::from("/mnt/benchmark-source/wikipedia-40m-current/wikipedia_1m_40m_ground_truth.parquet")'))
PATCH
  printf 'building-%s\n' "$variant" > "$out/status"
  cd "$src"
  cargo +stable bench -p chroma-index --bench hierarchical_spann_profile_quantized --bench hierarchical_centroid_persistence_profile --no-run --message-format=json > "$out/build-$variant.jsonl" 2> "$out/build-$variant.log"
  python3 - "$out/build-$variant.jsonl" "$out" "$variant" <<'BINARIES'
import json,shutil,sys
wanted={'hierarchical_spann_profile_quantized':'binary','hierarchical_centroid_persistence_profile':'profile'}; found=set()
for line in open(sys.argv[1]):
 try:r=json.loads(line)
 except ValueError:continue
 name=r.get('target',{}).get('name')
 if r.get('reason')=='compiler-artifact' and name in wanted and r.get('executable'):
  shutil.copy2(r['executable'],f'{sys.argv[2]}/{wanted[name]}-{sys.argv[3]}');found.add(name)
assert found==set(wanted),found
BINARIES
  sha256sum "$out/binary-$variant" "$out/profile-$variant" > "$out/binaries-$variant.sha256"
done
# Convert identical 40M/39M trees without adding vectors. The first candidate
# commit is the format upgrade; following no-op commits measure inherited data.
copy_index "$fixed" "$root/fixed-baseline"
copy_index "$fixed" "$root/fixed-candidate"
run_helper baseline commit-baseline commit "$root/fixed-baseline" 3
run_helper candidate commit-candidate commit "$root/fixed-candidate" 3
copy_index "$snapshot" "$root/prepared-39m"
run_helper candidate prepare-39m commit "$root/prepared-39m" 1
# Alternate opening fresh providers on exactly matching logical 40M indexes.
for pair in 1 2 3; do
  run_helper baseline "open-baseline-$pair" open "$root/fixed-baseline" 3
  run_helper candidate "open-candidate-$pair" open "$root/fixed-candidate" 3
done
# Compare first-upgrade and steady-format incremental commit costs separately.
for run in baseline-1 candidate-upgrade-1 candidate-steady-1 candidate-steady-2 candidate-upgrade-2 baseline-2; do
  variant=baseline; source=$snapshot
  case "$run" in candidate-*) variant=candidate;; esac
  case "$run" in candidate-steady-*) source=$root/prepared-39m;; esac
  dir=$root/index-$run; copy_index "$source" "$dir"
  printf 'running-%s\n' "$run" > "$out/status"
  args=(--dataset wikipedia-en --metric l2 --checkpoint 40 --checkpoint-size 1000000 --branching-factor 100 --split-threshold 2048 --merge-threshold 512 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-beam-min 10 --write-beam-max 16 --write-level-taus _,_,0.30 --max-replicas 1 --write-rng-epsilon 0 --write-rng-factor 0 --read-beam-min 10 --read-beam-max 256 --read-level-taus _,_,_ --read-level-min-pcts 50,5,0 --recall-rerank-vectors 1,8 --recall-tau-values 0.7,1,2 --num-queries 100 --max-cache-bytes 1073741824 --compute-gt-clusters false --verify-valid-postings --resume --save --save-dir "$dir")
  printf '%q ' "$out/binary-$variant" "${args[@]}" > "$out/$run.command"
  /usr/bin/time -v -o "$out/$run.time" "$out/binary-$variant" "${args[@]}" > "$out/$run.log" 2>&1
  printf '0\n' > "$out/$run.exit-code"
  cp "$dir/checkpoint.json" "$out/$run.checkpoint.json"
done
snapshot_manifest > "$out/protected-after.json"
cmp "$out/protected-before.json" "$out/protected-after.json"
df -h /mnt/data > "$out/disk-after.txt"
printf 'complete\n' > "$out/status"
