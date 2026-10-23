#!/usr/bin/env bash
set -euo pipefail
r=/home/ubuntu/benchmark-data/large-index-balancing-investigation
exec 9>/mnt/data/chroma-benchmark.lock
flock 9
cd /home/ubuntu/chroma-large-index-balancing
cp /home/ubuntu/chroma-benchmark/target/release/deps/hierarchical_spann_profile_quantized-141263a897e45414 "$r/component-io-binary"
sha256sum "$r/component-io-binary" > "$r/component-io-binary.sha256"
git diff > "$r/component-io.diff"
for mode in baseline keep-pins cache8g; do
 export CHROMA_BALANCE_COMPONENT_FIXTURE=/mnt/data/large-index-investigation-components-40m
 export CHROMA_COMPONENT_CACHE_BYTES=1073741824
 export CHROMA_COMPONENT_LEAF_COUNT=200
 unset CHROMA_COMPONENT_KEEP_SCALAR_PINS
 if [[ "$mode" == keep-pins ]]; then export CHROMA_COMPONENT_KEEP_SCALAR_PINS=1; fi
 if [[ "$mode" == cache8g ]]; then export CHROMA_COMPONENT_CACHE_BYTES=8589934592; fi
 /usr/bin/time -v -o "$r/components-io-${mode}.time" timeout 300 "$r/component-io-binary" > "$r/components-io-${mode}.log" 2>&1
done
unset CHROMA_BALANCE_COMPONENT_FIXTURE CHROMA_COMPONENT_KEEP_SCALAR_PINS
export HF_HOME=/mnt/data/huggingface HF_HUB_OFFLINE=1
args=(--dataset wikipedia-en --metric l2 --checkpoint 10 --checkpoint-size 1000000 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0 --num-queries 0 --compute-gt-clusters false --resume --no-commit --save-dir /mnt/data/large-index-investigation-components-9m)
for mode in original-1g original-64m keep-pins-64m; do
 cache=67108864
 binary=/home/ubuntu/benchmark-data/wikipedia-40m-current/binary
 unset CHROMA_COMPONENT_KEEP_SCALAR_PINS
 if [[ "$mode" == original-1g ]]; then cache=1073741824; fi
 if [[ "$mode" == keep-pins-64m ]]; then binary="$r/component-io-binary"; export CHROMA_COMPONENT_KEEP_SCALAR_PINS=1; fi
 printf '%q ' "$binary" "${args[@]}" --max-cache-bytes "$cache" > "$r/balance-${mode}.command.txt"; printf '\n' >> "$r/balance-${mode}.command.txt"
 set +e
 /usr/bin/time -v -o "$r/balance-${mode}.time" timeout 300 "$binary" "${args[@]}" --max-cache-bytes "$cache" > "$r/balance-${mode}.log" 2>&1
 code=$?
 set -e
 echo "$code" > "$r/balance-${mode}.status"
done
date -u +%FT%TZ > "$r/io-balance-finished"
