# Developer & Codespaces Quickstart Guide

**Status:** Official Developer Guide  
**Runtime:** Java 21 LTS, Gradle 8.4, Docker-in-Docker, Kubernetes (Kind), Terraform, Helm  

---

## 1. Environment Architecture

The GitHub Codespace is configured via `.devcontainer/devcontainer.json` using a lean `mcr.microsoft.com/devcontainers/base:ubuntu-24.04` image equipped with:
* **Java 21 LTS** (Temurin distribution)
* **Docker-in-Docker** (privileged daemon for Testcontainers and Kind)
* **Kubernetes Client (`kubectl`) & Helm 3**
* **HashiCorp Terraform**
* **Kind (Kubernetes in Docker)**

---

## 2. Fast Verification (Java & Helm)

To run compilation, contract tests, and ArchUnit architecture checks without spinning up the cluster:

```bash
cd apps/platform-engine
./gradlew test --no-daemon
```

To validate and lint the canonical Helm chart:

```bash
helm lint deploy/helm/event-driven-lab
helm template event-driven-lab deploy/helm/event-driven-lab > /dev/null
```

---

## 3. Full Stack Orchestration (Kind + Helm)

To build the platform container image, provision the local Kind Kubernetes cluster via Terraform, inject secrets, and deploy all 12 services via Helm in a single command:

```bash
./scripts/start-all.sh
```

*(On Windows PowerShell, use `./scripts/start-all.ps1`)*

### Accessing the Platform

1. **Platform Engine Control Room UI**:
   ```bash
   kubectl port-forward svc/platform-engine 8080:8080
   ```
   Open `http://localhost:8080/` in your browser. Log in with `admin` / `local-dev-password` to submit loans, trigger transaction bursts, inject telemetry outliers, and view the real-time SHA-256 ledger.

2. **Grafana Observability Stack**:
   ```bash
   kubectl port-forward svc/eventdrivenmicroservices-grafana 3000:3000
   ```
   Open `http://localhost:3000/` in your browser to inspect pre-configured dashboards for Anomaly Detection Rate, Telemetry Z-Scores, and HTTP Ingestion Throughput.

3. **Automated End-to-End Verification**:
   ```bash
   ./scripts/demo.sh
   ```
   Executes health checks, loan submissions, transaction velocity bursts, outbox queue validation, and cryptographic hash chain verification against the live cluster.

---

## 4. Next Implementation Sequence

1. **Stage 2 (Outbox Leased Locking):** Add database-level row locking (`SELECT ... FOR UPDATE SKIP LOCKED`) to outbox event batch polling for horizontal multi-instance scaling.
2. **Stage 3 (Trace Visualization Validation):** Verify live distributed trace waterfall visualization in Grafana Tempo via the OpenTelemetry Collector.
3. **Stage 5 (Mock Financial Provider):** Implement the isolated mock financial provider for payment approval, decline, and settlement delay simulation.
4. **Stage 7 (Service Extraction):** Decompose the modular monolith into independently deployable microservice containers once external contracts are locked.
