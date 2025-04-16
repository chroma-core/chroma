#!/usr/bin/env bash

set -e

# Function to check if server is ready
check_server() {
    curl -s http://localhost:8000/openapi.json > /dev/null
    return $?
}

# Function to wait for server with exponential backoff
wait_for_server() {
    local max_attempts=10
    local attempt=1
    local base_delay=1
    local max_delay=32

    echo "Waiting for server to start..."
    while [ $attempt -le $max_attempts ]; do
        if check_server; then
            echo "Server is ready!"
            return 0
        fi

        delay=$((base_delay * (2 ** (attempt - 1))))  # Exponential backoff
        delay=$((delay < max_delay ? delay : max_delay))  # Cap at max_delay

        echo "Attempt $attempt/$max_attempts: Server not ready, waiting ${delay}s..."
        sleep $delay
        attempt=$((attempt + 1))
    done

    echo "Error: Server failed to start after $max_attempts attempts"
    return 1
}

# Start the Chroma server in the background
echo "Building and starting Chroma server..."
cargo build --bin chroma
cargo run --bin chroma run &
SERVER_PID=$!

# Wait for the server to be ready
wait_for_server

# Install dependencies
cd clients/js

# Generate the JS client
echo "Generating JS client..."
pnpm genapi

# Cleanup: kill the server process
kill $SERVER_PID

pnpm prettier

# run git diff and check if packages/chromadb-core/src/generated/ has changed
echo "Checking for changes in generated client..."
if ! git diff --quiet --exit-code packages/chromadb-core/src/generated/; then
    echo "Error: Generated JS client has changed. Please commit the changes."
    git diff packages/chromadb-core/src/generated/ | cat
    exit 1
fi
echo "No changes detected in generated client."

# Install dependencies and run tests
echo "Running tests..."
pnpm -r test --verbose
