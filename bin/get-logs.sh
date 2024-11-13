#!/usr/bin/env bash
set -e

NAMESPACE="chroma"

# Create a directory with the test name to store logs
LOGS_DIR="./logs"
mkdir -p "$LOGS_DIR"

# Get the list of all pods in the namespace
PODS=$(kubectl get pods -n $NAMESPACE -o jsonpath='{.items[*].metadata.name}')
echo "Got all the pods: $PODS"

# Iterate over each pod and get the logs
for POD in $PODS; do
  echo "Getting logs for pod: $POD"
  # Save the logs to a file named after the pod and test name
  kubectl logs $POD -n $NAMESPACE --since=0s > "${LOGS_DIR}/${POD}.log"
done
