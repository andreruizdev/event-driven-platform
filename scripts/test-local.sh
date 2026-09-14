#!/usr/bin/env bash
set -e

echo "========================================="
echo " Event-Driven Microservices Lab"
echo " Local End-to-End Test Harness"
echo "========================================="

# 1. Provision Infrastructure via Terraform (Local Environment)
echo -e "\n[1/5] Provisioning Kubernetes cluster via Terraform..."
if ! docker info >/dev/null 2>&1; then
    echo "WARNING: Docker daemon is not running or accessible in the current context."
    echo "Kind cluster provisioning and container builds require a running Docker daemon."
    echo "To run unit & contract tests locally without Docker, use: cd apps/platform-engine && ./gradlew test"
    exit 0
fi

if ! kind get clusters 2>/dev/null | grep -q "eventdrivenmicroservices-cluster"; then
    cd terraform/environments/local
    terraform init
    terraform apply -auto-approve
    cd ../../../
else
    echo "Kind cluster 'eventdrivenmicroservices-cluster' is already running."
fi

# 2. Build images locally using Docker
echo -e "\n[2/5] Building platform-engine image via Docker..."
docker build -t docker.io/event-driven-lab/platform-engine:latest apps/platform-engine

# 3. Load images into the Kind cluster
echo -e "\n[3/5] Loading images into Kind cluster..."
kind load docker-image docker.io/event-driven-lab/platform-engine:latest --name eventdrivenmicroservices-cluster

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

