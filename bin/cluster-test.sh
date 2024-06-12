#!/usr/bin/env bash
set -e

# TODO make url configuration consistent.
export CHROMA_CLUSTER_TEST_ONLY=1
export CHROMA_SERVER_HOST=localhost:8000
export CHROMA_COORDINATOR_HOST=localhost

echo "Chroma Server is running at port $CHROMA_SERVER_HOST"
echo "Chroma Coordinator is running at port $CHROMA_COORDINATOR_HOST"

# Despite the fact that tilt forwards the ports
# We need the ports forwarded since tilt ci will
# not keep them forwarded
# https://github.com/tilt-dev/tilt/issues/5964
# kubectl -n chroma port-forward svc/sysdb 50051:50051 &
# kubectl -n chroma port-forward svc/logservice 50052:50051 &
# kubectl -n chroma port-forward svc/query-service 50053:50051 &
# kubectl -n chroma port-forward svc/frontend-service 8000:8000 &

"$@"
