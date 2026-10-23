#!/usr/bin/env bash
set -euo pipefail
r=/home/ubuntu/benchmark-data/wikipedia-40m-current
source_dir=/home/ubuntu/chroma-wikipedia-40m-current
index_dir=/mnt/data/wikipedia-40m-current
exec 9>/mnt/data/chroma-benchmark.lock
flock 9
trap 'code=$?; echo "$code" > "$r/exit-status"; date -u +%FT%TZ > "$r/finished-at"' EXIT
date -u +%FT%TZ > "$r/started-at"
export PATH=/home/ubuntu/.cargo/bin:$PATH
export HF_HOME=/mnt/data/huggingface
export OPENBLAS_NUM_THREADS=32
export OMP_NUM_THREADS=32
echo 'Downloading exact ordered 40M prefix'
/home/ubuntu/chroma/venv/bin/python "$r/download.py" wikipedia --manifest-dir "$r" > "$r/download.log" 2>&1
echo 'Computing exact ground truth at all 1M boundaries'
/usr/bin/time -v -o "$r/ground-truth.time" /home/ubuntu/chroma/venv/bin/python "$r/exact-gt.py" wikipedia --source-manifest "$r/wikipedia_40m_sources.json" --outdir "$r" > "$r/ground-truth.log" 2>&1
cp /home/ubuntu/.cache/wikipedia_en/ground_truth.parquet "$r/previous-ground-truth.parquet"
cp "$r/wikipedia_1m_40m_ground_truth.parquet" /home/ubuntu/.cache/wikipedia_en/ground_truth.parquet
test ! -e "$source_dir"
test ! -e "$index_dir"
mkdir -p "$source_dir"
tar -xf /home/ubuntu/benchmark-data/wikipedia-40m-22eb704.tar -C "$source_dir"
export CARGO_TARGET_DIR=/home/ubuntu/chroma-benchmark/target
cd "$source_dir"
printf '%s\n' 22eb704e71490d95cf715bdb8901f7d6c8a3cf34 > "$r/source-revision.txt"
{ uname -a; lscpu; free -h; df -h /mnt/data; rustc --version; cargo --version; } > "$r/environment.txt"
find rust/index -name '*.rs' -type f -print0 | sort -z | xargs -0 sha256sum > "$r/source.sha256"
echo 'Building clean pinned feature branch'
cargo clean -p chroma-index --release > "$r/clean.log" 2>&1
cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run > "$r/build.log" 2>&1
binary=$(find "$CARGO_TARGET_DIR/release/deps" -maxdepth 1 -name 'hierarchical_spann_profile_quantized-*' -type f -executable | head -1)
cp "$binary" "$r/binary"
sha256sum "$r/binary" > "$r/binary.sha256"
export HF_HUB_OFFLINE=1
command=("$r/binary" --dataset wikipedia-en --metric l2 --checkpoint 40 --checkpoint-size 1000000 --branching-factor 100 --split-threshold 2048 --merge-threshold 512 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-beam-min 10 --write-beam-max 16 --write-level-taus _,_,0.30 --max-replicas 1 --write-rng-epsilon 0 --write-rng-factor 0 --read-navigation fp,1bit --read-beam-min 10 --read-beam-max 256 --read-level-taus _,_,_ --read-level-min-pcts 50,5,0 --recall-rerank-centroids 1,4 --recall-rerank-vectors 1,8 --recall-tau-values 0.7,1,2 --num-queries 100 --max-cache-bytes 1073741824 --compute-gt-clusters false --save --save-dir "$index_dir")
printf '%q ' "${command[@]}" > "$r/command.txt"
printf '\n' >> "$r/command.txt"
date -u +%FT%TZ > "$r/benchmark-started-at"
echo 'Starting 40M benchmark'
/usr/bin/time -v -o "$r/benchmark.time" "${command[@]}" > "$r/full-cli-output.txt" 2>&1
date -u +%FT%TZ > "$r/benchmark-finished-at"
cp "$index_dir/checkpoint.json" "$r/final-checkpoint.json"
python3 - "$r/final-checkpoint.json" <<'PY'
import json, sys
m=json.load(open(sys.argv[1]))
assert m['total_vectors']==40000000 and m['checkpoint_idx']==39, m
print('Verified all 40M vectors committed at checkpoint 40.')
PY
cp "$r/previous-ground-truth.parquet" /home/ubuntu/.cache/wikipedia_en/ground_truth.parquet
echo '40M benchmark complete'
