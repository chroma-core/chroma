#!/usr/bin/env bash
set -e

# TODO make url configuration consistent.
export CHROMA_CLUSTER_TEST_ONLY=1
export CHROMA_SERVER_HOST=localhost:8000
export PULSAR_BROKER_URL=localhost
export CHROMA_COORDINATOR_HOST=localhost

echo "Chroma Server is running at port $CHROMA_SERVER_HOST"
echo "Pulsar Broker is running at port $PULSAR_BROKER_URL"
echo "Chroma Coordinator is running at port $CHROMA_COORDINATOR_HOST"

kubectl -n chroma port-forward svc/sysdb-lb 50051:50051 &
kubectl -n chroma port-forward svc/logservice-lb 50052:50051 &
kubectl -n chroma port-forward svc/pulsar-lb 6650:6650 &
kubectl -n chroma port-forward svc/pulsar-lb 8080:8080 &
kubectl -n chroma port-forward svc/frontend-service 8000:8000 &

"$@"
