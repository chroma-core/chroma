#!/usr/bin/env bash

set -e

function cleanup {
  docker compose -f docker-compose.cluster.test.yml down --rmi local --volumes
}

trap cleanup EXIT

docker compose -f docker-compose.cluster.test.yml up -d --wait

export CHROMA_CLUSTER_TEST_ONLY=1

echo testing: python -m pytest "$@"
python -m pytest "$@"
