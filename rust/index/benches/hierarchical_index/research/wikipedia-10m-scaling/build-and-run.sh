#!/usr/bin/env bash
set -euo pipefail
results=/home/ubuntu/benchmark-data/wikipedia-10m-scaling-20261008
source_dir=/home/ubuntu/chroma-wikipedia-10m
cd "$source_dir"
export PATH=/home/ubuntu/.cargo/bin:$PATH
export CARGO_TARGET_DIR=/home/ubuntu/chroma-benchmark/target
printf '%s\n' af6366cce6badf39cb5fb10f739a35c45f7e0a64 > "$results/revision"
printf '%s\n' 'git archive of clean merged hierarchical-spann; no source modifications' > "$results/source-status"
{ rustc --version; cargo --version; uname -a; lscpu; free -h; df -h /mnt/data; } > "$results/environment.txt"
printf '%s\n' 'cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run' > "$results/build.command"
echo "$(date -u +%FT%TZ) Build started"
cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run > "$results/build.log" 2>&1
binary=$(find "$CARGO_TARGET_DIR/release/deps" -maxdepth 1 -name 'hierarchical_spann_profile_quantized-*' -type f -executable | head -1)
test -n "$binary"
cp "$binary" "$results/binary"
sha256sum "$results/binary" > "$results/binary.sha256"
echo "$(date -u +%FT%TZ) Build finished"
export HOME="$results/home"
export HF_HOME=/mnt/data/huggingface
export HF_HUB_OFFLINE=1
save_dir=/mnt/data/wikipedia-10m-scaling-20261008
command=("$results/binary" --dataset wikipedia-en --metric l2 --checkpoint 10 --checkpoint-size 1000000 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0 --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false --save --save-dir "$save_dir")
printf '%q ' "${command[@]}" > "$results/run.command"
printf '\n' >> "$results/run.command"
test ! -e "$save_dir"
echo "$(date -u +%FT%TZ) 10M Wikipedia run started"
set +e
/usr/bin/time -v -o "$results/run.time" "${command[@]}" > "$results/run.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$results/exit-status"
echo "$(date -u +%FT%TZ) Run finished status=$status"
exit "$status"
