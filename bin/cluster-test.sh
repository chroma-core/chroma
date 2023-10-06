#!/usr/bin/env bash

set -e

function cleanup {
  # Restore the previous kube context
  kubectl config use-context $PREV_CHROMA_KUBE_CONTEXT
  # Kill the tunnel process
  kill $TUNNEL_PID
  minikube delete -p chroma-test
}

trap cleanup EXIT

# Save the current kube context into a variable
export PREV_CHROMA_KUBE_CONTEXT=$(kubectl config current-context)

# Create a new minikube cluster for the test
minikube start -p chroma-test

# Add the ingress addon to the cluster
minikube addons enable ingress -p chroma-test
minikube addons enable ingress-dns -p chroma-test

# Setup docker to build inside the minikube cluster and build the image
eval $(minikube -p chroma-test docker-env)
docker build -t server:latest -f Dockerfile .

# Apply the kubernetes manifests
kubectl apply -f k8s/deployment
kubectl apply -f k8s/crd
kubectl apply -f k8s/cr

# Wait for the pods in the chroma namespace to be ready
kubectl wait --namespace chroma --for=condition=Ready pods --all --timeout=300s

# Run mini kube tunnel in the background to expose the service
minikube tunnel -p chroma-test &
TUNNEL_PID=$!

export CHROMA_CLUSTER_TEST_ONLY=1

echo testing: python -m pytest "$@"
python -m pytest "$@"
