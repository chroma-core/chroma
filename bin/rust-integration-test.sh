#!/usr/bin/env bash

set -e

cleanup() {
    echo "Stopping Cargo process..."
    rm -f chroma_integration_test_tmp_dir
    pkill -P $$
}

trap cleanup EXIT

cargo run --bin chroma -- run bin/rust_single_node_integration_test_config.yaml &
sleep 5

export CHROMA_INTEGRATION_TEST_ONLY=1
export CHROMA_API_IMPL=chromadb.api.fastapi.FastAPI
export CHROMA_SERVER_HOST=localhost
export CHROMA_SERVER_HTTP_PORT=3000

echo testing: python -m pytest "$@"
python -m pytest "$@"
