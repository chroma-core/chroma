#!/usr/bin/env bash
set -euo pipefail
r=/home/ubuntu/benchmark-data/read-scoring-10m-merged-22eb704
source=/home/ubuntu/chroma-read-scoring-10m-merged-22eb704
old=/home/ubuntu/benchmark-data/wikipedia-10m-scaling-baseline-20261008
nine=/mnt/data/wikipedia-10m-scaling-snapshots-20261008/9000000
fixture=/mnt/data/wikipedia-10m-scaling-snapshots-20261008/10000000
new=/mnt/data/read-scoring-10m-merged-22eb704
mkdir -p "$r/home/.cache/wikipedia_en" "$source"
tar -xf /home/ubuntu/benchmark-data/read-scoring-10m-merged.tar -C "$source"
cp "$old/home/.cache/wikipedia_en/ground_truth.parquet" "$r/home/.cache/wikipedia_en/ground_truth.parquet"
ln -s /mnt/data/huggingface "$r/home/.cache/huggingface"
cp "$old/binary" "$r/baseline-binary"
printf '%s\n' af6366cce6badf39cb5fb10f739a35c45f7e0a64 > "$r/baseline-revision"
printf '%s\n' 22eb704e71490d95cf715bdb8901f7d6c8a3cf34 > "$r/candidate-revision"
sha256sum "$nine/checkpoint.json" "$fixture/checkpoint.json" > "$r/original-metadata-before.sha256"
test ! -e "$new"
echo "$(date -u +%FT%TZ) Independent9M fixture copy started"
cp -a --reflink=auto "$nine" "$new"
cp "$new/checkpoint.json" "$r/resume-input-checkpoint.json"
python3 - "$nine" "$new" <<'PY'
import json,sys,os
from pathlib import Path
old,new=map(Path,sys.argv[1:])
m=json.loads((new/'checkpoint.json').read_text())
assert m['checkpoint_idx']==8 and m['total_vectors']==9000000
for p in new.rglob('*'):
 if p.is_file(): assert os.stat(p).st_ino!=os.stat(old/p.relative_to(new)).st_ino
print('Verified independent clone starts at checkpoint9 with9000000vectors. No file shares an inode with original.',flush=True)
PY
export PATH=/home/ubuntu/.cargo/bin:$PATH
export CARGO_TARGET_DIR=/home/ubuntu/chroma-benchmark/target
cd "$source"
{ rustc --version; cargo --version; uname -a; lscpu; free -h; df -h /mnt/data; } > "$r/environment.txt"
find rust/index -type f -name '*.rs' -print0 | sort -z | xargs -0 sha256sum > "$r/source.sha256"
echo "$(date -u +%FT%TZ) Clean merged build started"
cargo clean -p chroma-index --release > "$r/clean.log" 2>&1
cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run > "$r/build.log" 2>&1
binary=$(find "$CARGO_TARGET_DIR/release/deps" -maxdepth 1 -name 'hierarchical_spann_profile_quantized-*' -type f -executable | head -1)
if rg -a -q READ_PERCENTILES "$binary"; then echo 'Unexpected comparison instrumentation'; exit 1; fi
cp "$binary" "$r/candidate-binary"
sha256sum "$r/"*-binary > "$r/binary.sha256"
"$r/candidate-binary" --help > "$r/candidate-help.txt"
export HOME="$r/home"
export HF_HOME=/mnt/data/huggingface
export HF_HUB_OFFLINE=1
settings=(--dataset wikipedia-en --metric l2 --checkpoint 10 --checkpoint-size 1000000 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0 --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false)
command=("$r/candidate-binary" "${settings[@]}" --save --resume --save-dir "$new")
printf '%q ' "${command[@]}" > "$r/resume.command"; printf '\n' >> "$r/resume.command"
echo "$(date -u +%FT%TZ) Resume9M to10M started"
/usr/bin/time -v -o "$r/resume.time" "${command[@]}" > "$r/resume.log" 2>&1
cp "$new/checkpoint.json" "$r/resume-output-checkpoint.json"
python3 - "$new" <<'PY'
import json,sys
from pathlib import Path
m=json.loads((Path(sys.argv[1])/'checkpoint.json').read_text())
assert m['checkpoint_idx']==9 and m['total_vectors']==10000000
print('Verified resumed build commits exactly10000000vectors at checkpoint10.',flush=True)
PY
echo "$(date -u +%FT%TZ) Resume complete; same-index read pairs started"
for repeat in 1 2 3; do
 if [[ $repeat == 2 ]]; then variants=(candidate baseline); else variants=(baseline candidate); fi
 for variant in "${variants[@]}"; do
  name="$variant-$repeat"
  command=("$r/$variant-binary" "${settings[@]}" --resume --no-commit --save-dir "$fixture")
  printf '%q ' "${command[@]}" > "$r/read-$name.command"; printf '\n' >> "$r/read-$name.command"
  /usr/bin/time -v -o "$r/read-$name.time" "${command[@]}" > "$r/read-$name.log" 2>&1
  echo "$(date -u +%FT%TZ) Read$name complete"
 done
done
sha256sum "$nine/checkpoint.json" "$fixture/checkpoint.json" > "$r/original-metadata-after.sha256"
cmp "$r/original-metadata-before.sha256" "$r/original-metadata-after.sha256"
printf '%s\n' 0 > "$r/exit-status"
echo "$(date -u +%FT%TZ) Timed work complete"
