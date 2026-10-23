#!/usr/bin/env bash
set -euo pipefail
results=/home/ubuntu/benchmark-data/dataset-loading-20261008
cd /home/ubuntu/chroma-dataset-loading-probe
for path in /home/ubuntu/chroma-benchmark/target/release/deps/dataset_loading_probe-*; do
 if [[ -x "$path" && ! -d "$path" ]]; then binary="$path"; fi
done
cp "$binary" "$results/probe-binary"
sha256sum "$results/probe-binary" > "$results/binary.sha256"
for dataset in msmarco wikipedia; do
 echo "$(date -u +%FT%TZ) compare $dataset"
 /usr/bin/time -v -o "$results/$dataset-compare.time" "$results/probe-binary" "$dataset" compare 1000000 > "$results/$dataset-compare.log" 2>&1
done
for round in 1 2 3; do
 for dataset in msmarco wikipedia; do
  if [[ "$round" == 2 ]]; then variants=(candidate baseline); else variants=(baseline candidate); fi
  for variant in "${variants[@]}"; do
   name="$dataset-$variant-$round"
   echo "$(date -u +%FT%TZ) started $name"
   /usr/bin/time -v -o "$results/$name.time" "$results/probe-binary" "$dataset" "$variant" 1000000 > "$results/$name.log" 2>&1
   echo "$(date -u +%FT%TZ) completed $name"
  done
 done
done
echo "$(date -u +%FT%TZ) complete"
