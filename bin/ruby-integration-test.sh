#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cargo build --bin chroma
cargo run --bin chroma -- run "$ROOT_DIR/bin/rust_single_node_integration_test_config.yaml" &
CHROMA_SERVER_PID=$!

cleanup() {
  kill "$CHROMA_SERVER_PID" || true
}
trap cleanup EXIT

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

cd "$ROOT_DIR/clients/ruby"

bundle install
bundle exec rspec
