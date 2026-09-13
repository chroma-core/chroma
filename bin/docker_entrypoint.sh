#!/bin/bash
set -e

export IS_PERSISTENT=1
export CHROMA_SERVER_NOFILE=${CHROMA_SERVER_NOFILE:-65536}

# Warn if PERSIST_DIRECTORY is not set (data will be stored in ./chroma inside the container)
if [[ -z "${PERSIST_DIRECTORY}" ]]; then
    echo -e "\033[33mWARNING: PERSIST_DIRECTORY is not set. Data will be stored in /chroma/chroma inside the container."
    echo -e "If you are using a volume mount, set PERSIST_DIRECTORY to your mount path (e.g., /data).\033[0m"
fi

args="$@"

if [[ $args =~ ^uvicorn.* ]]; then
    echo "Starting server with args: $(eval echo "$args")"
    echo -e "\033[31mWARNING: Please remove 'uvicorn chromadb.app:app' from your command line arguments. This is now handled by the entrypoint script."
    exec $(eval echo "$args")
else
    echo "Starting 'uvicorn chromadb.app:app' with args: $(eval echo "$args")"
    exec uvicorn chromadb.app:app $(eval echo "$args")
fi
