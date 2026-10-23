#!/bin/bash
set -euo pipefail
root=/mnt/data/reader-contiguous-7903
out=$root/results
snapshot=/mnt/data/wikipedia-40m-baseline-20261009-snapshots/39000000
exec 9>/mnt/data/chroma-benchmark.lock
flock 9
trap 'code=$?; printf "%s\n" "$code" > "$out/exit-code"; date -u +%FT%TZ > "$out/finished-at"' EXIT
export PATH=/home/ubuntu/.cargo/bin:$PATH
export HF_HOME=/mnt/data/huggingface HF_HUB_OFFLINE=1
export CARGO_TARGET_DIR=$root/target CARGO_BUILD_JOBS=32
mkdir -p "$CARGO_TARGET_DIR"
printf '%s\n' preparing > "$out/status"
date -u +%FT%TZ > "$out/started-at"
sha256sum "$snapshot/checkpoint.json" /mnt/data/wikipedia-40m-baseline-20261009/checkpoint.json /mnt/benchmark-source/wikipedia-40m-current/wikipedia_1m_40m_ground_truth.parquet > "$out/protected-inputs-before.sha256"
find "$snapshot/data" -type f -printf '%P %s\n' | sort | sha256sum > "$out/snapshot-files-before.sha256"
cp -a /home/ubuntu/chroma-benchmark/target/release "$CARGO_TARGET_DIR/"
cp -a /home/ubuntu/chroma-benchmark/target/.rustc_info.json "$CARGO_TARGET_DIR/" || true
for variant in baseline candidate; do
  src=$root/source-$variant
  tar -xzf "$root/reader-contiguous-$variant.tar.gz" -C "$src"
  # Identical benchmark-only path override avoids changing the shared GT cache.
  python3 - "$src" <<'PY'
import sys
from pathlib import Path
p=Path(sys.argv[1])/'rust/index/benches/datasets/wikipedia.rs'
s=p.read_text()
s=s.replace('cache_dir().join("ground_truth.parquet")', 'PathBuf::from("/mnt/benchmark-source/wikipedia-40m-current/wikipedia_1m_40m_ground_truth.parquet")')
p.write_text(s)
PY
  printf 'building-%s\n' "$variant" > "$out/status"
  cd "$src"
  cargo +stable bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run --message-format=json > "$out/build-$variant.jsonl" 2> "$out/build-$variant.log"
  python3 - "$out/build-$variant.jsonl" "$out/binary-$variant" <<'PY'
import json, sys, shutil
paths=[]
for line in open(sys.argv[1]):
    try: item=json.loads(line)
    except ValueError: continue
    if item.get('reason')=='compiler-artifact' and item.get('target',{}).get('name')=='hierarchical_spann_profile_quantized' and item.get('executable'):
        paths.append(item['executable'])
assert len(paths)==1, paths
shutil.copy2(paths[0],sys.argv[2])
PY
  sha256sum "$out/binary-$variant" > "$out/binary-$variant.sha256"
done
for run in baseline-1 candidate-1 candidate-2 baseline-2; do
  variant=${run%-*}
  dir=$root/index-$run
  test ! -e "$dir"
  cp -al "$snapshot" "$dir"
  cp "$snapshot/checkpoint.json" "$dir/checkpoint.independent.json"
  mv "$dir/checkpoint.independent.json" "$dir/checkpoint.json"
  printf 'running-%s\n' "$run" > "$out/status"
  args=(--dataset wikipedia-en --metric l2 --checkpoint 40 --checkpoint-size 1000000 --branching-factor 100 --split-threshold 2048 --merge-threshold 512 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-beam-min 10 --write-beam-max 16 --write-level-taus _,_,0.30 --max-replicas 1 --write-rng-epsilon 0 --write-rng-factor 0 --read-beam-min 10 --read-beam-max 256 --read-level-taus _,_,_ --read-level-min-pcts 50,5,0 --recall-rerank-vectors 1,8 --recall-tau-values 0.7,1,2 --num-queries 100 --max-cache-bytes 1073741824 --compute-gt-clusters false --resume --save-dir "$dir")
  printf '%q ' "$out/binary-$variant" "${args[@]}" > "$out/$run.command"
  date -u +%FT%TZ > "$out/$run.started-at"
  /usr/bin/time -v -o "$out/$run.time" "$out/binary-$variant" "${args[@]}" > "$out/$run.log" 2>&1
  printf '0\n' > "$out/$run.exit-code"
  cp "$dir/checkpoint.json" "$out/$run.checkpoint.json"
  date -u +%FT%TZ > "$out/$run.finished-at"
done
sha256sum "$snapshot/checkpoint.json" /mnt/data/wikipedia-40m-baseline-20261009/checkpoint.json /mnt/benchmark-source/wikipedia-40m-current/wikipedia_1m_40m_ground_truth.parquet > "$out/protected-inputs-after.sha256"
find "$snapshot/data" -type f -printf '%P %s\n' | sort | sha256sum > "$out/snapshot-files-after.sha256"
cmp "$out/protected-inputs-before.sha256" "$out/protected-inputs-after.sha256"
cmp "$out/snapshot-files-before.sha256" "$out/snapshot-files-after.sha256"
printf 'complete\n' > "$out/status"
