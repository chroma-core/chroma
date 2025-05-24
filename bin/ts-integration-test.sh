#!/usr/bin/env bash

set -e

# Install dependencies
cd clients/js

# Generate the JS client
echo "Generating JS client..."
pnpm genapi:chromadb

pnpm prettier

# run git diff and check if packages/chromadb-core/src/generated/ has changed
echo "Checking for changes in generated client..."
if ! git diff --quiet --exit-code packages/chromadb/src/api/; then
    echo "Error: Generated JS client has changed. Please commit the changes."
    git diff packages/chromadb/src/api/ | cat
    exit 1
fi
echo "No changes detected in generated client."

# Install dependencies and run tests
echo "Running tests..."
pnpm -r --filter='!./packages/ai-embeddings/**' test --verbose
