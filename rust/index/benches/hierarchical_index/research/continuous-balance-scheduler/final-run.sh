#!/usr/bin/env bash
set -euo pipefail
exec 9>/mnt/data/chroma-benchmark.lock
flock -n 9 || { echo 'Benchmark host is reserved'; exit 1; }
r=/home/ubuntu/benchmark-data/continuous-scheduler
source=/home/ubuntu/chroma-continuous-scheduler-final
fixture=/mnt/data/wikipedia-10m-scaling-snapshots-20261008/9000000
mkdir -p "$source"
tar -xf /home/ubuntu/benchmark-data/continuous-scheduler-final.tar -C "$source"
export PATH=/home/ubuntu/.cargo/bin:$PATH CARGO_TARGET_DIR=/home/ubuntu/chroma-benchmark/target
export HF_HOME=/mnt/data/huggingface HF_HUB_OFFLINE=1
cd "$source"
cargo clean -p chroma-index --release > "$r/final-clean.log" 2>&1
cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run > "$r/final-build.log" 2>&1
binary=$(find "$CARGO_TARGET_DIR/release/deps" -maxdepth 1 -name 'hierarchical_spann_profile_quantized-*' -type f -executable | head -1)
cp "$binary" "$r/final-binary"
sha256sum "$r/final-binary" > "$r/final-binary.sha256"
find rust/index/benches/hierarchical_index -type f -print0 | sort -z | xargs -0 sha256sum > "$r/final-source.sha256"
settings=(--dataset wikipedia-en --metric l2 --checkpoint 10 --checkpoint-size 1000000 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0 --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false)
for name in final-1 final-2; do
 destination=/mnt/data/continuous-scheduler-$name
 test ! -e "$destination"
 cp -a --reflink=auto "$fixture" "$destination"
 python3 - "$fixture" "$destination" <<'PY'
import os,sys
from pathlib import Path
source,dest=map(Path,sys.argv[1:])
for p in dest.rglob('*'):
 if p.is_file(): assert os.stat(p).st_ino != os.stat(source/p.relative_to(dest)).st_ino
PY
 command=("$r/final-binary" "${settings[@]}" --save --resume --save-dir "$destination")
 printf '%q ' "${command[@]}" > "$r/$name.command"
 /usr/bin/time -v -o "$r/$name.time" "${command[@]}" > "$r/$name.log" 2>&1
 cp "$destination/checkpoint.json" "$r/$name.checkpoint.json"
 echo "$name complete $(date -u +%FT%TZ)"
done
printf '0\n' > "$r/final-exit-status"
