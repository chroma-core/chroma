#!/usr/bin/env bash
set -euo pipefail
r=/home/ubuntu/benchmark-data/wikipedia-10m-scaling-baseline-20261008
snapshots=/mnt/data/wikipedia-10m-scaling-baseline-snapshots-20261008
export HOME="$r/home"
export HF_HOME=/mnt/data/huggingface
export HF_HUB_OFFLINE=1
export OPENBLAS_NUM_THREADS=32
export OMP_NUM_THREADS=32
mkdir -p "$r/prefix-gt"
/home/ubuntu/chroma/venv/bin/python - "$r" <<'PY'
import sys
from pathlib import Path
r=Path(sys.argv[1])
source=Path('/home/ubuntu/benchmark-data/exact_ground_truth.py').read_text()
assert 'BOUNDARIES = (1_000_000, 10_000_000)' in source
(r/'exact-prefix-ground-truth.py').write_text(source.replace('BOUNDARIES = (1_000_000, 10_000_000)','BOUNDARIES = tuple(range(1_000_000, 10_000_001, 1_000_000))'))
PY
echo "$(date -u +%FT%TZ) Intermediate exact ground truth started"
/usr/bin/time -v -o "$r/prefix-gt.time" /home/ubuntu/chroma/venv/bin/python "$r/exact-prefix-ground-truth.py" wikipedia --source-manifest /home/ubuntu/benchmark-data/wikipedia_10m_sources.json --outdir "$r/prefix-gt" > "$r/prefix-gt.log" 2>&1
cp "$r/prefix-gt/wikipedia_1m_10m_ground_truth.parquet" "$HOME/.cache/wikipedia_en/ground_truth.parquet"
echo "$(date -u +%FT%TZ) Intermediate ground truth completed; read measurements started"
for checkpoint in {1..10}; do
 dir="$snapshots/$((checkpoint*1000000))"
 test -f "$dir/checkpoint.json"
 command=("$r/binary" --dataset wikipedia-en --metric l2 --checkpoint "$checkpoint" --checkpoint-size 1000000 --resume --no-commit --save-dir "$dir" --write-navigation fp --fp-npa --threads 32 --balance-threads 32 --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0 --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false)
 printf '%q ' "${command[@]}" > "$r/read-$checkpoint.command"
 printf '\n' >> "$r/read-$checkpoint.command"
 /usr/bin/time -v -o "$r/read-$checkpoint.time" "${command[@]}" > "$r/read-$checkpoint.log" 2>&1
 echo "$(date -u +%FT%TZ) Read checkpoint $checkpoint completed"
done
printf '%s\n' complete > "$r/read-status"
