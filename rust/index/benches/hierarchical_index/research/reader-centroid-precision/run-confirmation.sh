#!/usr/bin/env bash
set -euo pipefail
R=/home/ubuntu/benchmark-data/reader-centroid-precision
exec 9>/mnt/data/chroma-benchmark.lock
flock -n 9 || exit 75
test "$(cat "$R/exit-code")" = 0
mkdir "$R/confirmation"
trap 'printf "%s\n" "$?" > "$R/confirmation/exit-code"' EXIT
COMMON=(--index /mnt/data/wikipedia-40m-baseline-20261009 --ground-truth /mnt/benchmark-source/wikipedia-40m-current/wikipedia_1m_40m_ground_truth.parquet --queries 100 --threads 32 --max-cache-bytes 1073741824 --repeats 3)
"$R/binary" "${COMMON[@]}" --navigation 1bit-4bit --internal-beams 64,256 --leaf-beams 128,256 --rerank 8 --output "$R/confirmation/baseline.jsonl" > "$R/confirmation/baseline-output.txt" 2>&1
"$R/binary" "${COMMON[@]}" --navigation 4bit-fp --internal-beams 64 --leaf-beams 96,112,128 --rerank 4 --output "$R/confirmation/90.jsonl" > "$R/confirmation/90-output.txt" 2>&1
"$R/binary" "${COMMON[@]}" --navigation 4bit-fp --internal-beams 256 --leaf-beams 192,224,256 --rerank 8 --output "$R/confirmation/94.jsonl" > "$R/confirmation/94-output.txt" 2>&1
"$R/binary" "${COMMON[@]}" --navigation 4bit-4bit --internal-beams 256 --leaf-beams 256 --rerank 8 --output "$R/confirmation/4q.jsonl" > "$R/confirmation/4q-output.txt" 2>&1
