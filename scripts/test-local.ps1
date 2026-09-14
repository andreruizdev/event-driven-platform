#!/usr/bin/env pwsh
$ErrorActionPreference = "Stop"

Write-Host "========================================="
Write-Host " Event-Driven Microservices Lab"
Write-Host " Local End-to-End Test Harness"
Write-Host "========================================="

# 1. Provision Infrastructure via Terraform (Local Environment)
$env:KIND_EXPERIMENTAL_PROVIDER = "podman"
Write-Host "`n[1/5] Provisioning Kubernetes cluster via Terraform..."
if (!(kind get clusters | Select-String "eventdrivenmicroservices-cluster" -Quiet)) {
    Push-Location terraform/environments/local
    terraform init
    terraform apply -auto-approve
    Pop-Location
}
else {
    Write-Host "Kind cluster 'eventdrivenmicroservices-cluster' is already running."
}

# 2. Build images locally using Podman
Write-Host "`n[2/5] Building platform-engine image via Podman..."
podman build -t docker.io/event-driven-lab/platform-engine:latest apps/platform-engine

# 3. Load images into the Kind cluster
Write-Host "`n[3/5] Loading images into Kind cluster using archives..."
podman save --format docker-archive -o platform-engine.tar docker.io/event-driven-lab/platform-engine:latest
kind load image-archive platform-engine.tar

# Clean up tarballs
Remove-Item *.tar

# 4. Setup Secrets
Write-Host "`n[4/5] Setting up local secrets..."
if (-not (Test-Path ".env")) {
    Write-Host "Creating .env from .env.example..."
    Copy-Item ".env.example" ".env"
}
kubectl delete secret eventdrivenmicroservices-secrets --ignore-not-found
kubectl create secret generic eventdrivenmicroservices-secrets --from-env-file=.env

# 5. Deploy via Helm
Write-Host "`n[5/5] Deploying Event-Driven Lab via Helm..."
$helmChartPath = "./deploy/helm/event-driven-lab"
helm upgrade --install event-driven-lab $helmChartPath `
    --set platformEngine.image.pullPolicy=Never

# Done
Write-Host "`nDeployment triggered! Waiting for pods to stabilize..."
Write-Host "Run the following command to watch your pods spin up:"
Write-Host "    kubectl get pods -w"
Write-Host "`nOnce the Grafana pod is ready, access the dashboard at http://localhost:3000 by running:"
Write-Host "    kubectl port-forward svc/eventdrivenmicroservices-grafana 3000:3000"

