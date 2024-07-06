#!/usr/bin/env bash

# Hardcoded namespace
NAMESPACE=chroma

# Check if the test-name is provided as an argument
if [ -z "$1" ]; then
  echo "Usage: $0 <test-file-path>"
  exit 1
fi

TEST_FILE_PATH=$1

# Extract the test file name from the path and remove the ".py" extension
TEST_NAME=$(basename "$TEST_FILE_PATH" .py)

# Create a directory with the test name to store logs
LOGS_DIR="./logs/${TEST_NAME}"
mkdir -p "$LOGS_DIR"

# Get the list of all pods in the namespace
PODS=$(kubectl get pods -n $NAMESPACE -o jsonpath='{.items[*].metadata.name}')

# Iterate over each pod and get the logs
for POD in $PODS; do
  echo "Getting logs for pod: $POD"
  # Save the logs to a file named after the pod and test name
  kubectl logs $POD -n $NAMESPACE --since=0s > "${LOGS_DIR}/${POD}_logs.txt"
done

# Zip all log files
zip -r "${TEST_NAME}_logs.zip" "$LOGS_DIR"

# Print confirmation message
echo "Logs have been zipped to ${TEST_NAME}_logs.zip"

# Output the path to the zip file
echo "$(pwd)/${TEST_NAME}_logs.zip"
