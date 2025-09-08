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

# Where to write port-forward logs (CI runner has /tmp)
PF_LOG_DIR=${PF_LOG_DIR:-/tmp}
FRONTEND_PF_LOG="$PF_LOG_DIR/pf-rust-frontend-service.log"
FRONTEND_PF_PID=""
FRONTEND_TAIL_PID=""

# Ensure log dir exists and start streaming logs to stdout
mkdir -p "$PF_LOG_DIR"
: >"$FRONTEND_PF_LOG"
echo "Streaming port-forward logs to stdout from $FRONTEND_PF_LOG"
tail -n +1 -F "$FRONTEND_PF_LOG" &
FRONTEND_TAIL_PID=$!
pf_pids+=($FRONTEND_TAIL_PID)

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
  # Write logs to help debug failures, keep process in background
  kubectl -n "$NAMESPACE" port-forward "svc/$svc" "$local_port:$remote_port" >"$FRONTEND_PF_LOG" 2>&1 &
  local pid=$!
  pf_pids+=($pid)
  FRONTEND_PF_PID=$pid
}

ensure_port_forward() {
  # Restart the port-forward if it died for any reason
  if [[ -z "${FRONTEND_PF_PID:-}" ]] || ! kill -0 "$FRONTEND_PF_PID" 2>/dev/null; then
    echo "Port-forward is not running; restarting..."
    start_port_forward rust-frontend-service 8000 8000
  fi
}

# Wait for frontend pods to be ready to avoid racing port-forward before endpoints exist
echo "Waiting for rust-frontend-service deployment to be ready..."
if ! kubectl -n "$NAMESPACE" rollout status deploy/rust-frontend-service --timeout=180s; then
  echo "Deployment rollout wait failed; waiting for any ready pod with label app=rust-frontend-service..." >&2
  kubectl -n "$NAMESPACE" wait --for=condition=ready pod -l app=rust-frontend-service --timeout=180s
fi

# Only forward the frontend service; other services are internal to the cluster
start_port_forward rust-frontend-service 8000 8000

# Wait for the frontend to be ready via healthcheck
HEALTH_URL="http://localhost:8000/api/v2/healthcheck"
echo "Waiting for frontend health at $HEALTH_URL ..."
for i in {1..60}; do
  ensure_port_forward
  if curl -fsS "$HEALTH_URL" >/dev/null; then
    echo "Frontend is healthy."
    break
  fi
  sleep 1
  if [[ "$i" -eq 60 ]]; then
    echo "ERROR: Frontend did not become healthy in time" >&2
    echo "Last port-forward logs (if any):" >&2
    tail -n 100 "$FRONTEND_PF_LOG" 2>/dev/null || true
    exit 1
  fi
done

# Run the provided command (pytest)
"$@"
