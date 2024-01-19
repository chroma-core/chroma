#!/bin/bash
set -e

export IS_PERSISTENT=1
export CHROMA_SERVER_NOFILE=65535
echo "Starting server with args: ${@}"
exec uvicorn chromadb.app:app ${@}
