#!/usr/bin/env bash
set -euo pipefail

# Ensure cluster tests target the externalized frontend
export CHROMA_CLUSTER_TEST_ONLY=1
export CHROMA_SERVER_HOST="${CHROMA_SERVER_HOST:-localhost:8000}"
export CHROMA_COORDINATOR_HOST="${CHROMA_COORDINATOR_HOST:-localhost}"

echo "Chroma Server is running at $CHROMA_SERVER_HOST"
echo "Chroma Coordinator is running at $CHROMA_COORDINATOR_HOST"

# Start port-forwards within the same process as the tests so they persist
NAMESPACE=${KUBERNETES_NAMESPACE:-chroma}

pf_pids=()
cleanup() {
  if [[ ${#pf_pids[@]} -gt 0 ]]; then
    echo "Stopping port-forwards..."
    kill ${pf_pids[@]} 2>/dev/null || true
    wait ${pf_pids[@]} 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

start_port_forward() {
  local svc="$1"; local local_port="$2"; local remote_port="$3"
  echo "Port-forwarding $svc $local_port:$remote_port in namespace $NAMESPACE"
  kubectl -n "$NAMESPACE" port-forward "svc/$svc" "$local_port:$remote_port" >/dev/null 2>&1 &
  pf_pids+=($!)
}

# Only forward the frontend service; other services are internal to the cluster
start_port_forward rust-frontend-service 8000 8000

# Wait for the frontend to be ready via healthcheck
HEALTH_URL="http://localhost:8000/api/v2/healthcheck"
echo "Waiting for frontend health at $HEALTH_URL ..."
for i in {1..60}; do
  if curl -fsS "$HEALTH_URL" >/dev/null; then
    echo "Frontend is healthy."
    break
  fi
  sleep 1
  if [[ "$i" -eq 60 ]]; then
    echo "ERROR: Frontend did not become healthy in time" >&2
    exit 1
  fi
done

# Run the provided command (pytest)
"$@"
