#!/bin/bash
set -e

export IS_PERSISTENT=1
export CHROMA_SERVER_NOFILE=65535
args="$@"

if [[ $args =~ ^uvicorn.* ]]; then
    echo "Starting server with args: $(eval echo "$args")"
    echo -e "\033[31mWARNING: Please remove 'uvicorn chromadb.app:app' from your command line arguments. This is now handled by the entrypoint script."
    exec $(eval echo "$args")
else
    echo "Starting 'uvicorn chromadb.app:app' with args: $(eval echo "$args")"
    exec uvicorn chromadb.app:app $(eval echo "$args")
fi
