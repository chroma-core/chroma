#!/usr/bin/env bash
set -euo pipefail
results=/home/ubuntu/benchmark-data/worker-statistics-performance
nine=/mnt/data/wikipedia-10m-scaling-snapshots-20261008/9000000
fixture=/mnt/data/wikipedia-10m-scaling-snapshots-20261008/10000000
export HF_HOME=/mnt/data/huggingface
export HF_HUB_OFFLINE=1
settings=(--dataset wikipedia-en --metric l2 --checkpoint 10 --checkpoint-size 1000000 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0 --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false)
test -f "$results/build-status"
sha256sum "$nine/checkpoint.json" "$fixture/checkpoint.json" > "$results/fixtures-before.sha256"
for repeat in 1 2 3; do
 if [[ $repeat == 2 ]]; then variants=(candidate baseline); else variants=(baseline candidate); fi
 for variant in "${variants[@]}"; do
  name="$variant-$repeat"
  index_dir="/mnt/data/worker-statistics-$name"
  test ! -e "$index_dir"
  echo "$(date -u +%FT%TZ) Independent copy for $name started"
  cp -a --reflink=auto "$nine" "$index_dir"
  python3 - "$nine" "$index_dir" <<'PY'
import json,sys,os
from pathlib import Path
old,new=map(Path,sys.argv[1:])
metadata=json.loads((new/'checkpoint.json').read_text())
assert metadata['checkpoint_idx']==8 and metadata['total_vectors']==9000000
for p in new.rglob('*'):
 if p.is_file(): assert os.stat(p).st_ino!=os.stat(old/p.relative_to(new)).st_ino
PY
  cp "$index_dir/checkpoint.json" "$results/$name-input.json"
  command=("$results/$variant-binary" "${settings[@]}" --save --resume --save-dir "$index_dir")
  printf '%q ' "${command[@]}" > "$results/$name.command"
  printf '\n' >> "$results/$name.command"
  echo "$(date -u +%FT%TZ) $name started"
  /usr/bin/time -v -o "$results/$name.time" "${command[@]}" > "$results/$name.log" 2>&1
  cp "$index_dir/checkpoint.json" "$results/$name-output.json"
  python3 - "$index_dir" <<'PY'
import json,sys
from pathlib import Path
metadata=json.loads((Path(sys.argv[1])/'checkpoint.json').read_text())
assert metadata['checkpoint_idx']==9 and metadata['total_vectors']==10000000
PY
  echo "$(date -u +%FT%TZ) $name completed"
 done
done
sha256sum "$nine/checkpoint.json" "$fixture/checkpoint.json" > "$results/fixtures-after.sha256"
cmp "$results/fixtures-before.sha256" "$results/fixtures-after.sha256"
printf '%s\n' 0 > "$results/run-status"
