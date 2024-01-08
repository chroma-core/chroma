 #!/usr/bin/env bash

eval $(minikube -p chroma-test docker-env)

docker build -t chroma-coordinator:latest -f go/coordinator/Dockerfile .

kubectl delete deployment coordinator -n chroma

# Apply the kubernetes manifests
kubectl apply -f k8s/deployment
kubectl apply -f k8s/crd
kubectl apply -f k8s/cr
kubectl apply -f k8s/test
