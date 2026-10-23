#!/usr/bin/env bash
set -euo pipefail
r=/home/ubuntu/benchmark-data/wikipedia-40m-current
index_dir=/mnt/data/wikipedia-40m-current
exec 9>/mnt/data/chroma-benchmark.lock
flock 9
trap 'code=$?; echo "$code" > "$r/exit-status"; date -u +%FT%TZ > "$r/finished-at"; cp "$r/previous-ground-truth.parquet" /home/ubuntu/.cache/wikipedia_en/ground_truth.parquet' EXIT
rm -f "$r/exit-status" "$r/finished-at"
export HF_HOME=/mnt/data/huggingface
export HF_HUB_OFFLINE=1
cd /home/ubuntu/chroma-wikipedia-40m-current
cp "$r/wikipedia_1m_40m_ground_truth.parquet" /home/ubuntu/.cache/wikipedia_en/ground_truth.parquet
test ! -e "$index_dir"
command=("$r/binary" --dataset wikipedia-en --metric l2 --checkpoint 40 --checkpoint-size 1000000 --branching-factor 100 --split-threshold 2048 --merge-threshold 512 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-beam-min 10 --write-beam-max 16 --write-level-taus _,_,0.30 --max-replicas 1 --write-rng-epsilon 0 --write-rng-factor 0 --read-beam-min 10 --read-beam-max 256 --read-level-taus _,_,_ --read-level-min-pcts 50,5,0 --recall-rerank-vectors 1,8 --recall-tau-values 0.7,1,2 --num-queries 100 --max-cache-bytes 1073741824 --compute-gt-clusters false --save --save-dir "$index_dir")
"$r/binary" --help > "$r/help.txt"
python3 - "$r/help.txt" "${command[@]:1}" <<'PY'
import re, sys
known=set(re.findall(r'--[a-z][a-z0-9-]*',open(sys.argv[1]).read()))
unknown=[a for a in sys.argv[2:] if a.startswith('--') and a not in known]
assert not unknown, unknown
print('Verified every benchmark flag is supported by the current executable.')
PY
printf '%q ' "${command[@]}" > "$r/command.txt"
printf '\n' >> "$r/command.txt"
date -u +%FT%TZ > "$r/benchmark-started-at"
/usr/bin/time -v -o "$r/benchmark.time" "${command[@]}" > "$r/full-cli-output.txt" 2>&1
date -u +%FT%TZ > "$r/benchmark-finished-at"
cp "$index_dir/checkpoint.json" "$r/final-checkpoint.json"
python3 - "$r/final-checkpoint.json" <<'PY'
import json, sys
m=json.load(open(sys.argv[1]))
assert m['total_vectors']==40000000 and m['checkpoint_idx']==39, m
print('Verified all 40M vectors committed at checkpoint 40.')
PY
