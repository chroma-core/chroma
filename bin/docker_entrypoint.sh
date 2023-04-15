#!/bin/bash

echo "Rebuilding hnsw to ensure architecture compatibility"
echo -e "⚠️ This basic stack doesn't support any kind of authentication;
 anyone who knows your server IP will be able to add and query for embeddings.
 More information: https://docs.trychroma.com/deployment"
pip install --force-reinstall --no-cache-dir hnswlib
uvicorn chromadb.app:app --workers 1 --host 0.0.0.0 --port 8000 --proxy-headers --log-config log_config.yml
