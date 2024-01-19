#!/bin/bash
set -e

export IS_PERSISTENT=1
export CHROMA_SERVER_NOFILE=65535
echo Starting server with args: $(eval echo "$@")
exec uvicorn chromadb.app:app $(eval echo "$@")
