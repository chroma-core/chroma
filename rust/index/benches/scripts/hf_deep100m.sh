#!/bin/bash
# Download Deep1B first-100M subset for benchmarking.
#
# Dataset: Yandex Deep1B (Babenko & Lempitsky, CVPR 2016)
#   - 100M vectors, 96 dimensions, Euclidean distance
#   - 10K queries with 100-NN ground truth
#
# Resource requirements:
#   Disk:  ~40GB (data) + ~20GB (build artifacts) = ~60GB
#   RAM:   ~45GB (embeddings) + ~15GB (tree/codes/overhead) = ~60GB
#   Recommended: 128GB+ RAM instance, 100GB+ free disk
#
# The base vectors are range-downloaded from the 1B file (~36GB instead
# of the full 358GB), then the header is patched to reflect 100M vectors.
#
# Ground truth was computed against all 1B vectors. The benchmark filters
# GT neighbor IDs to only those present in the 100M subset.
#
# Usage:
#   chmod +x hf_deep100m.sh
#   ./hf_deep100m.sh
#   # Or override cache location:
#   DEEP100M_CACHE=/mnt/data/deep100m ./hf_deep100m.sh

set -euo pipefail

CACHE_DIR="${DEEP100M_CACHE:-$HOME/.cache/deep100m}"
mkdir -p "$CACHE_DIR"

BASE_FILE="$CACHE_DIR/base.100M.fbin"
QUERY_FILE="$CACHE_DIR/query.public.10K.fbin"
GT_FILE="$CACHE_DIR/groundtruth.public.10K.ibin"

NUM_VECTORS=100000000
DIM=96
BYTES_PER_VEC=$((DIM * 4))
HEADER_SIZE=8
DATA_SIZE=$((NUM_VECTORS * BYTES_PER_VEC))
TOTAL_BYTES=$((HEADER_SIZE + DATA_SIZE))
DOWNLOAD_GB=$((TOTAL_BYTES / 1073741824))

YANDEX_BASE="https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP"
HF_BASE="https://huggingface.co/datasets/2026peng/deep1b/resolve/main"

echo "=== Deep1B 100M Subset Download ==="
echo "  100M vectors, 96 dimensions, Euclidean"
echo "  Download size: ~${DOWNLOAD_GB}GB"
echo "  Cache: $CACHE_DIR"
echo ""

# ---------- base vectors (100M, range download from 1B file) ----------

if [ -f "$BASE_FILE" ]; then
    ACTUAL_SIZE=$(stat --printf="%s" "$BASE_FILE" 2>/dev/null || stat -f "%z" "$BASE_FILE" 2>/dev/null)
    if [ "$ACTUAL_SIZE" -ge "$TOTAL_BYTES" ]; then
        echo "[base] Already exists: $BASE_FILE ($(du -h "$BASE_FILE" | cut -f1))"
    else
        echo "[base] File exists but is truncated (${ACTUAL_SIZE} < ${TOTAL_BYTES}), re-downloading..."
        rm -f "$BASE_FILE"
    fi
fi

if [ ! -f "$BASE_FILE" ]; then
    echo "[base] Downloading first 100M vectors from Deep1B..."
    echo "       Range: 0-$((TOTAL_BYTES - 1)) bytes"

    SRC_URL="${YANDEX_BASE}/base.1B.fbin"
    TMP_FILE="${BASE_FILE}.tmp"

    echo "       Trying Yandex: $SRC_URL"
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -L -r "0-1023" "$SRC_URL" 2>/dev/null || echo "000")

    if [ "$HTTP_CODE" = "206" ]; then
        echo "       Range requests supported, downloading ~${DOWNLOAD_GB}GB..."
        curl -L --retry 3 --progress-bar \
            -r "0-$((TOTAL_BYTES - 1))" \
            -o "$TMP_FILE" \
            "$SRC_URL"
    else
        echo "       Yandex range request returned $HTTP_CODE, trying HuggingFace..."
        SRC_URL="${HF_BASE}/base.1B.fbin"
        HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -L -r "0-1023" "$SRC_URL" 2>/dev/null || echo "000")

        if [ "$HTTP_CODE" = "206" ]; then
            echo "       HuggingFace range requests supported, downloading ~${DOWNLOAD_GB}GB..."
            curl -L --retry 3 --progress-bar \
                -r "0-$((TOTAL_BYTES - 1))" \
                -o "$TMP_FILE" \
                "$SRC_URL"
        else
            echo "ERROR: Neither source supports HTTP Range requests."
            echo "       You'll need to download the full base.1B.fbin (~358GB) and truncate manually:"
            echo "         hf download --repo-type dataset 2026peng/deep1b base.1B.fbin"
            echo "         python3 -c \"import struct; f=open('base.1B.fbin','rb'); d=f.read($TOTAL_BYTES); f.close(); f=open('$BASE_FILE','wb'); f.write(struct.pack('<II',$NUM_VECTORS,$DIM)); f.write(d[8:]); f.close()\""
            exit 1
        fi
    fi

    # Patch header: overwrite num_vectors from 1B to 100M (little-endian u32).
    python3 -c "
import struct, sys
with open('$TMP_FILE', 'r+b') as f:
    f.write(struct.pack('<I', $NUM_VECTORS))
    f.seek(4)
    dim = struct.unpack('<I', f.read(4))[0]
    if dim != $DIM:
        print(f'ERROR: expected dim=$DIM, got {dim}', file=sys.stderr)
        sys.exit(1)
print('Header patched: num_vectors=$NUM_VECTORS, dim=$DIM')
"

    mv "$TMP_FILE" "$BASE_FILE"
    echo "  Saved: $BASE_FILE ($(du -h "$BASE_FILE" | cut -f1))"
fi

# ---------- query vectors ----------

if [ -f "$QUERY_FILE" ]; then
    echo "[query] Already exists: $QUERY_FILE"
else
    echo "[query] Downloading query vectors (10K, 96D)..."
    curl -L --retry 3 --progress-bar -o "$QUERY_FILE" "${YANDEX_BASE}/query.public.10K.fbin"
fi

# ---------- ground truth ----------

if [ -f "$GT_FILE" ]; then
    echo "[gt] Already exists: $GT_FILE"
else
    echo "[gt] Downloading ground truth (10K queries, 100-NN)..."
    curl -L --retry 3 --progress-bar -o "$GT_FILE" "${YANDEX_BASE}/groundtruth.public.10K.ibin"
fi

echo ""
echo "=== Download Complete ==="
ls -lh "$CACHE_DIR/"
echo ""
echo "Resource requirements for 100M benchmark:"
echo "  RAM:   ~60GB (embeddings ~45GB + tree/codes ~15GB)"
echo "  Disk:  ~60GB (data ~36GB + build artifacts ~20GB)"
echo "  Recommended: 128GB+ RAM, 100GB+ free disk"
echo ""
echo "Note: GT was computed against all 1B vectors."
echo "Neighbors with ID >= 100M will be filtered at query time."
echo ""
echo "To run (after adding Deep100M dataset support):"
echo "  cargo bench --bench hierarchical_spann_profile_quantized -p chroma-index -- \\"
echo "    --dataset deep-100m --checkpoint 10 --checkpoint-size 10000000 \\"
echo "    --branching-factor 100 --split-threshold 2048 --merge-threshold 512 \\"
echo "    --threads 32 --max-replicas 1 ..."
