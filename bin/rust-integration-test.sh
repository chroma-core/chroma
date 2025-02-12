#!/usr/bin/env bash

set -e

cleanup() {
    echo "Stopping Cargo process..."
    rm -f chroma_integration_test_tmp_dir
    pkill -P $$
}

trap cleanup EXIT

cargo build --bin chroma
cargo run --bin chroma -- run bin/rust_single_node_integration_test_config.yaml &

echo "Waiting for Chroma server to be available..."
for i in {1..30}; do
    if curl -s http://localhost:3000/api/v2/heartbeat > /dev/null; then
        echo "Chroma server is up!"
        break
    fi
    echo "Retrying in 1 second..."
    sleep 1
done

if ! curl -s http://localhost:3000/api/v2/heartbeat > /dev/null; then
    echo "Chroma server failed to start within 60 seconds."
    exit 1
fi

export CHROMA_INTEGRATION_TEST_ONLY=1
export CHROMA_API_IMPL=chromadb.api.fastapi.FastAPI
export CHROMA_SERVER_HOST=localhost
export CHROMA_SERVER_HTTP_PORT=3000

echo testing: python -m pytest "$@"
python -m pytest "$@"
