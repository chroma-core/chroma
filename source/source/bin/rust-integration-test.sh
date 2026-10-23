#!/usr/bin/env bash

set -e

if [[ $CHROMA_THIN_CLIENT -eq 1 ]]; then
    echo "Using thin client"
    is_thin_client_py="clients/python/is_thin_client.py"
    is_thin_client_target="chromadb/is_thin_client.py"
    cp "$is_thin_client_py" "$is_thin_client_target"
else
    echo "Using normal client"
fi

cargo build --bin chroma
cargo run --bin chroma -- run bin/rust_single_node_integration_test_config.yaml &

echo "Waiting for Chroma server to be available..."
for i in {1..30}; do
    if curl -s http://localhost:8000/api/v2/heartbeat > /dev/null; then
        echo "Chroma server is up!"
        break
    fi
    echo "Retrying in 1 second..."
    sleep 1
done

if ! curl -s http://localhost:8000/api/v2/heartbeat > /dev/null; then
    echo "Chroma server failed to start within 60 seconds."
    exit 1
fi

export CHROMA_INTEGRATION_TEST_ONLY=1
export CHROMA_SERVER_HOST=localhost
export CHROMA_SERVER_HTTP_PORT=8000

echo testing: python -m pytest "$@"
python -m pytest "$@"
