#!/usr/bin/env bash
# Pre-download MSMARCO v2.1 English (Cohere Embed v3) passages + queries.
#
# Dataset: CohereLabs/msmarco-v2.1-embed-english-v3
#   60 passage shards (~4GB each, ~240GB total for all 113.5M passages)
#   1 query file (60MB, 1677 queries with brute-force top-1k GT)
#
# By default downloads enough shards to cover TARGET_VECTORS (100M).
# Set TARGET_VECTORS=0 to download all 113.5M.
#
# Usage:
#   chmod +x hf_msmarco_en.sh
#   ./hf_msmarco_en.sh
#   TARGET_VECTORS=113520750 ./hf_msmarco_en.sh   # all passages

set -euo pipefail

REPO_ID="${REPO_ID:-CohereLabs/msmarco-v2.1-embed-english-v3}"
NUM_SHARDS="${NUM_SHARDS:-60}"
TARGET_VECTORS="${TARGET_VECTORS:-100000000}"
RETRIES="${RETRIES:-4}"
export REPO_ID NUM_SHARDS TARGET_VECTORS RETRIES

echo "=== MSMARCO v2.1 English predownload ==="
echo "Repo:      $REPO_ID"
echo "Shards:    $NUM_SHARDS"
echo "Target:    $TARGET_VECTORS vectors (0 = all)"
echo ""

python3 - <<'PY'
import os
import sys
import time

repo_id = os.environ["REPO_ID"]
num_shards = int(os.environ["NUM_SHARDS"])
target_vectors = int(os.environ["TARGET_VECTORS"])
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

def download_file(filename):
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return hf_hub_download(
                repo_id=repo_id,
                repo_type="dataset",
                filename=filename
            )
        except Exception as e:
            last_err = e
            wait_s = min(30, 2 * attempt)
            print(f"  download failed (attempt {attempt}/{retries}): {e}", flush=True)
            if attempt < retries:
                print(f"  retrying in {wait_s}s...", flush=True)
                time.sleep(wait_s)
    print(f"ERROR: failed to download {filename}: {last_err}", file=sys.stderr)
    sys.exit(1)

# --- Download queries first (small, always needed) ---
print("Downloading queries...", flush=True)
q_path = download_file("queries_parquet/queries.parquet")
q_rows = pq.ParquetFile(q_path).metadata.num_rows
print(f"  queries: {q_rows} rows at {q_path}", flush=True)
print(flush=True)

# --- Download passage shards ---
cumulative = 0
downloaded = []

for i in range(num_shards):
    if target_vectors > 0 and cumulative >= target_vectors:
        break

    filename = f"passages_parquet/msmarco_v2.1_doc_segmented_{i:02d}.parquet"
    print(f"[{i+1}/{num_shards}] {filename}...", end=" ", flush=True)

    local_path = download_file(filename)

    try:
        rows = pq.ParquetFile(local_path).metadata.num_rows
    except Exception as e:
        print(f"ERROR reading parquet metadata: {e}", file=sys.stderr)
        sys.exit(1)

    cumulative += int(rows)
    downloaded.append((filename, int(rows), cumulative, local_path))
    print(f"rows={rows:,} cumulative={cumulative:,}", flush=True)

print(flush=True)
print("=== Predownload complete ===")
print(f"Downloaded shards: {len(downloaded)}")
print(f"Cumulative rows:   {cumulative:,}")
print(f"Queries:           {q_rows}")
PY

echo ""
echo "Run benchmark (100M via checkpoints):"
echo ""
echo "  cd ~/chroma"
echo "  unset HF_HOME"
echo "  export HF_HUB_OFFLINE=1"
echo ""
echo "  nohup cargo bench --bench hierarchical_spann_profile_quantized -p chroma-index -- \\"
echo "    --dataset ms-marco-en --metric cosine \\"
echo "    --checkpoint 100 --checkpoint-size 1000000 \\"
echo "    --branching-factor 100 --split-threshold 4096 --merge-threshold 1024 \\"
echo "    --threads 64 \\"
echo "    --write-beam-tau 1.5 --write-beam-min 10 --write-beam-max 16 \\"
echo "    --write-level-min-pcts 50,5,0 --write-level-taus _,1.4,1.3 \\"
echo "    --max-replicas 1 --write-rng-epsilon 0 --write-rng-factor 0 \\"
echo "    --read-beam-min 10 --read-beam-max 512 \\"
echo "    --read-level-taus _,_,_ --read-level-min-pcts 50,5,0 \\"
echo "    --recall-rerank-vectors 1,16 --recall-tau-values 1.2,1.5,2,3 \\"
echo "    --brute-force-gt \\"
echo "    --save --save-dir /mnt/data2/msmarco_en_100m_save \\"
echo "    > msmarco_en_100m.txt 2>&1 &"
