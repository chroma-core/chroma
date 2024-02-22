#!/usr/bin/env bash
set -e

export CHROMA_CLUSTER_TEST_ONLY=1
export CHROMA_SERVER_HOST=localhost:8000
export PULSAR_BROKER_URL=localhost
export CHROMA_COORDINATOR_HOST=localhost
export CHROMA_SERVER_GRPC_PORT="50051"

echo "Chroma Server is running at port $CHROMA_SERVER_HOST"
echo "Pulsar Broker is running at port $PULSAR_BROKER_URL"
echo "Chroma Coordinator is running at port $CHROMA_COORDINATOR_HOST"

echo testing: python -m pytest "$@"
python -m pytest "$@"

export CHROMA_KUBERNETES_INTEGRATION=1
cd go/coordinator
go test -timeout 30s -run ^TestNodeWatcher$ github.com/chroma/chroma-coordinator/internal/memberlist_manager
