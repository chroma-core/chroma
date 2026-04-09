#!/usr/bin/env bash
# Pre-download first 100M MS MARCO v2 vectors (Cohere embeddings).
#
# This script downloads corpus shards in order and stops once cumulative
# parquet row counts reach TARGET_VECTORS (default 100,000,000).
#
# Usage:
#   chmod +x hf_msmarco100m.sh
#   ./hf_msmarco100m.sh
#   HF_HOME=/mnt/data/hf-home ./hf_msmarco100m.sh
#   TARGET_VECTORS=100000000 HF_HOME=/mnt/data/hf-home ./hf_msmarco100m.sh

set -euo pipefail

REPO_ID="${REPO_ID:-Cohere/msmarco-v2-embed-multilingual-v3}"
NUM_SHARDS="${NUM_SHARDS:-139}"
TARGET_VECTORS="${TARGET_VECTORS:-100000000}"
HF_HOME="${HF_HOME:-$HOME/.cache/huggingface}"
MSMARCO_CACHE="${MSMARCO_CACHE:-$HOME/.cache/msmarco_v2}"
RETRIES="${RETRIES:-4}"
export REPO_ID NUM_SHARDS TARGET_VECTORS HF_HOME MSMARCO_CACHE RETRIES

mkdir -p "$HF_HOME" "$MSMARCO_CACHE"

echo "=== MS MARCO v2 first-${TARGET_VECTORS} predownload ==="
echo "Repo:      $REPO_ID"
echo "HF_HOME:   $HF_HOME"
echo "Shards:    $NUM_SHARDS"
echo "Target:    $TARGET_VECTORS vectors"
echo ""

python3 - <<'PY'
import os
import sys
import time
from pathlib import Path

repo_id = os.environ["REPO_ID"]
num_shards = int(os.environ["NUM_SHARDS"])
target_vectors = int(os.environ["TARGET_VECTORS"])
hf_home = os.environ["HF_HOME"]
msmarco_cache = os.environ["MSMARCO_CACHE"]
retries = int(os.environ["RETRIES"])

try:
    from huggingface_hub import hf_hub_download
except Exception:
    import subprocess
    print("Installing huggingface_hub...", flush=True)
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--user", "-U", "huggingface_hub"])
    from huggingface_hub import hf_hub_download

try:
    import pyarrow.parquet as pq
except Exception:
    import subprocess
    print("Installing pyarrow...", flush=True)
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--user", "-U", "pyarrow"])
    import pyarrow.parquet as pq

cumulative = 0
downloaded = []

for i in range(num_shards):
    if cumulative >= target_vectors:
        break

    filename = f"corpus/{i:04d}.parquet"
    last_err = None
    local_path = None

    for attempt in range(1, retries + 1):
        try:
            local_path = hf_hub_download(
                repo_id=repo_id,
                repo_type="dataset",
                filename=filename,
                cache_dir=hf_home,
                resume_download=True,
            )
            break
        except Exception as e:
            last_err = e
            wait_s = min(30, 2 * attempt)
            print(
                f"[{i+1}/{num_shards}] {filename} download failed (attempt {attempt}/{retries}): {e}",
                flush=True,
            )
            if attempt < retries:
                print(f"  retrying in {wait_s}s...", flush=True)
                time.sleep(wait_s)

    if local_path is None:
        print(f"ERROR: failed to download {filename}: {last_err}", file=sys.stderr)
        sys.exit(1)

    try:
        rows = pq.ParquetFile(local_path).metadata.num_rows
    except Exception as e:
        print(f"ERROR: failed reading parquet metadata for {filename}: {e}", file=sys.stderr)
        sys.exit(1)

    cumulative += int(rows)
    downloaded.append((filename, int(rows), cumulative, local_path))
    print(
        f"[{i+1}/{num_shards}] {filename} rows={rows:,} cumulative={cumulative:,}",
        flush=True,
    )

summary_path = Path(msmarco_cache) / f"first_{target_vectors}_shards.txt"
with summary_path.open("w", encoding="utf-8") as f:
    f.write(f"repo_id={repo_id}\n")
    f.write(f"target_vectors={target_vectors}\n")
    f.write(f"achieved_vectors={cumulative}\n")
    f.write(f"num_files={len(downloaded)}\n")
    f.write("files:\n")
    for filename, rows, cum, local_path in downloaded:
        f.write(f"{filename}\trows={rows}\tcumulative={cum}\tpath={local_path}\n")

print("")
print("=== Predownload complete ===")
print(f"Downloaded files: {len(downloaded)}")
print(f"Cumulative rows:  {cumulative:,}")
print(f"Summary file:     {summary_path}")
PY

echo ""
echo "Run benchmark (100M via checkpoints):"
echo "  HF_HOME=\"$HF_HOME\" TMPDIR=/mnt/data2/chroma-tmp CARGO_TARGET_DIR=/mnt/data2/chroma-target \\"
echo "  cargo bench --bench hierarchical_spann_profile_quantized -p chroma-index -- \\"
echo "    --dataset ms-marco --metric cosine --checkpoint 10 --checkpoint-size 10000000 \\"
echo "    --threads \"\$(nproc)\" --max-replicas 1"
