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
docker build -t chroma-coordinator:latest -f go/coordinator/Dockerfile .

# Apply the kubernetes manifests
kubectl apply -f k8s/deployment
kubectl apply -f k8s/crd
kubectl apply -f k8s/cr
kubectl apply -f k8s/test

# Wait for the pods in the chroma namespace to be ready
kubectl wait --namespace chroma --for=condition=Ready pods --all --timeout=300s

# Run mini kube tunnel in the background to expose the service
minikube tunnel -c true -p chroma-test &
TUNNEL_PID=$!

# Wait for the tunnel to be ready. There isn't an easy way to check if the tunnel is ready. So we just wait for 10 seconds
sleep 10

export CHROMA_CLUSTER_TEST_ONLY=1
export CHROMA_SERVER_HOST=$(kubectl get svc server -n chroma -o=jsonpath='{.status.loadBalancer.ingress[0].ip}')
export PULSAR_BROKER_URL=$(kubectl get svc pulsar -n chroma -o=jsonpath='{.status.loadBalancer.ingress[0].ip}')
export CHROMA_COORDINATOR_HOST=$(kubectl get svc coordinator -n chroma -o=jsonpath='{.status.loadBalancer.ingress[0].ip}')
export CHROMA_SERVER_GRPC_PORT="50051"

echo "Chroma Server is running at port $CHROMA_SERVER_HOST"
echo "Pulsar Broker is running at port $PULSAR_BROKER_URL"
echo "Chroma Coordinator is running at port $CHROMA_COORDINATOR_HOST"

echo testing: python -m pytest "$@"
python -m pytest "$@"

export CHROMA_KUBERNETES_INTEGRATION=1
cd go/coordinator
go test -timeout 30s -run ^TestNodeWatcher$ github.com/chroma/chroma-coordinator/internal/memberlist_manager
