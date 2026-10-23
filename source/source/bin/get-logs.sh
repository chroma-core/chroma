#!/usr/bin/env bash
set -e

# Hardcoded namespace
NAMESPACE=chroma
echo "Namespace: $NAMESPACE"

# Check if the test-name and version-number are provided as arguments
if [ -z "$1" ]; then
  echo "Usage: $0 <output-file-path>"
  exit 1  # Exit with code 1 indicating an error
fi

OUTPUT_FILE_PATH=$(readlink -m $1)
TEMP_DIR=$(mktemp -d)

mkdir "$TEMP_DIR/logs"
mkdir "$TEMP_DIR/traces"

# Create a description of the k8s cluster
kubectl describe -A all > "${TEMP_DIR}/logs/describe-all.txt" || true

# Get the list of all pods in the namespace
PODS=$(kubectl get pods -n $NAMESPACE -o jsonpath='{.items[*].metadata.name}')
echo "Got all the pods: $PODS"

# Iterate over each pod and get the logs
for POD in $PODS; do
  echo "Getting logs for pod: $POD"
  # Save the logs to a file named after the pod and test name
  kubectl logs $POD -n $NAMESPACE --since=0s > "${TEMP_DIR}/logs/${POD}.txt" || true
done

# Get traces from Jaeger for all services
curl "http://localhost:16686/api/services" | jq -r '.data[]' | while read -r service; do curl "http://localhost:16686/api/traces?limit=100&lookback=1h&maxDuration&minDuration&service=$service" > "$TEMP_DIR/traces/$service.json" || true; done

# Zip all log files
cd $TEMP_DIR &&zip -r "$OUTPUT_FILE_PATH" . && cd -

# Print confirmation message
echo "Logs have been zipped to $OUTPUT_FILE_PATH"
