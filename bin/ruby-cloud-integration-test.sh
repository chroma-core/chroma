#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -z "${CHROMA_API_KEY:-}" && -n "${RUBY_INTEGRATION_TEST_CHROMA_API_KEY:-}" ]]; then
  export CHROMA_API_KEY="${RUBY_INTEGRATION_TEST_CHROMA_API_KEY}"
fi

if [[ -z "${CHROMA_API_KEY:-}" ]]; then
  echo "CHROMA_API_KEY or RUBY_INTEGRATION_TEST_CHROMA_API_KEY must be set for cloud integration tests."
  exit 1
fi

export CHROMA_CLOUD_INTEGRATION_TESTS=1

cd "$ROOT_DIR/clients/ruby"

bundle install
bundle exec rspec --tag cloud
