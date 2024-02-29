#!/bin/bash

echo "Rebuilding hnsw to ensure architecture compatibility"
pip install --force-reinstall --no-cache-dir chroma-hnswlib
export IS_PERSISTENT=1
export CHROMA_SERVER_NOFILE=65535
uvicorn chromadb.app:app --workers 1 --host 0.0.0.0 --port 8000 --proxy-headers --log-config chromadb/log_config.yml --timeout-keep-alive 30
