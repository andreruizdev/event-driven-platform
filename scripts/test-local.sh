#!/usr/bin/env bash
set -e

echo "========================================="
echo " Event-Driven Microservices Lab"
echo " Local End-to-End Test Harness"
echo "========================================="

# 1. Provision Infrastructure via Terraform (Local Environment)
export KIND_EXPERIMENTAL_PROVIDER=podman
echo -e "\n[1/5] Provisioning Kubernetes cluster via Terraform..."
if ! kind get clusters | grep -q "eventdrivenmicroservices-cluster"; then
    cd terraform/environments/local
    terraform init
    terraform apply -auto-approve
    cd ../../../
else
    echo "Kind cluster 'eventdrivenmicroservices-cluster' is already running."
fi

# 2. Build images locally using Podman
echo -e "\n[2/5] Building platform-engine image via Podman..."
podman build -t docker.io/event-driven-lab/platform-engine:latest apps/platform-engine

# 3. Load images into the Kind cluster
echo -e "\n[3/5] Loading images into Kind cluster using archives..."
podman save --format docker-archive -o platform-engine.tar docker.io/event-driven-lab/platform-engine:latest
kind load image-archive platform-engine.tar

# Clean up tarballs
rm -f *.tar

# 4. Setup Secrets
echo -e "\n[4/5] Setting up local secrets..."
if [ ! -f .env ]; then
    echo "Creating .env from .env.example..."
    cp .env.example .env
fi
kubectl delete secret eventdrivenmicroservices-secrets --ignore-not-found
kubectl create secret generic eventdrivenmicroservices-secrets --from-env-file=.env

# 5. Deploy via Helm
echo -e "\n[5/5] Deploying Event-Driven Lab via Helm..."
HELM_CHART_PATH="./deploy/helm/event-driven-lab"
helm upgrade --install event-driven-lab "$HELM_CHART_PATH" \
  --set platformEngine.image.pullPolicy=Never

# Done
echo -e "\nDeployment triggered! Waiting for pods to stabilize..."
echo "Run the following command to watch your pods spin up:"
echo "    kubectl get pods -w"
echo -e "\nOnce the Grafana pod is ready, access the dashboard at http://localhost:3000 by running:"
echo "    kubectl port-forward svc/eventdrivenmicroservices-grafana 3000:3000"

