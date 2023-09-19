#!/usr/bin/env bash

set -e

function cleanup {
  docker compose -f docker-compose.cluster.yml down --rmi local --volumes
}

trap cleanup EXIT

docker compose -f docker-compose.cluster.yml up -d --wait pulsar

export CHROMA_CLUSTER_TEST_ONLY=1

echo testing: python -m pytest "$@"
python -m pytest "$@"
