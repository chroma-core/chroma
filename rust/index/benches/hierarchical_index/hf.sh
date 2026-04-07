#!/bin/bash
# export PATH="/home/ubuntu/.local/bin:$PATH"
# Download Wikipedia EN parquet shards for up to 7M vectors.
# Each shard has ~100K vectors, so 70 shards covers ~7M.
REPO="Cohere/wikipedia-2023-11-embed-multilingual-v3"
NUM_SHARDS=420

export PATH="$HOME/.local/bin:$PATH"

echo "Downloading $NUM_SHARDS shards from $REPO..."
for i in $(seq 0 $((NUM_SHARDS - 1))); do
  FILE=$(printf "en/%04d.parquet" $i)
  echo "[$((i+1))/$NUM_SHARDS] $FILE"
  hf download --repo-type dataset "$REPO" "$FILE" || {
      echo "  FAILED, retrying in 5s..."
      sleep 5
      hf download --repo-type dataset "$REPO" "$FILE" || {
          echo "  FAILED again on $FILE, skipping"
      }
  }
done
echo "Done."
