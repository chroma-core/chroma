#!/usr/bin/env bash
set -e

# Check if the test-name and version-number are provided as arguments
if [ -z "$1" ]; then
  echo "Usage: $0 <output-file-path>"
  exit 1  # Exit with code 1 indicating an error
fi

OUTPUT_FILE_PATH=$(readlink -m "$1")
TEMP_DIR=$(mktemp -d)

mkdir "$TEMP_DIR/logs"
mkdir "$TEMP_DIR/traces"

fetch_namespace() {
  local namespace="$1"
  local pods

  echo "Namespace: $namespace"

  # Get the list of all pods in the namespace
  pods=$(kubectl get pods -n "$namespace" -o jsonpath='{.items[*].metadata.name}' || true)
  echo "Got all the pods in $namespace: $pods"

  # Iterate over each pod and get the logs
  for pod in $pods; do
    local output_name

    echo "Getting logs for pod: $pod"

    if [ "$namespace" = "chroma" ]; then
      output_name="${pod}.txt"
    else
      output_name="${namespace}_${pod}.txt"
    fi

    kubectl logs "$pod" -n "$namespace" --since=0s > "${TEMP_DIR}/logs/${output_name}" || true
  done
}

# Create a description of the k8s cluster
kubectl describe -A all > "${TEMP_DIR}/logs/describe-all.txt" || true

fetch_namespace chroma
fetch_namespace chroma2

# Get traces from Jaeger for all services
curl "http://localhost:16686/api/services" | jq -r '.data[]' | while read -r service; do curl "http://localhost:16686/api/traces?limit=100&lookback=1h&maxDuration&minDuration&service=$service" > "$TEMP_DIR/traces/$service.json" || true; done

# Zip all log files
cd "$TEMP_DIR" && zip -r "$OUTPUT_FILE_PATH" . && cd -

# Print confirmation message
echo "Logs have been zipped to $OUTPUT_FILE_PATH"
