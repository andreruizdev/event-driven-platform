# Event-Driven Microservices Lab

## Project Documentation

- **[Architecture](docs/architecture.md)**: Implemented system boundaries, event flow, and current limitations.
- **[Engineering Guide](docs/engineering-guide.md)**: Setup, verification evidence, limitations, and the canonical next steps.

A hands-on engineering lab demonstrating event-driven distributed systems architecture, reactive streaming anomaly detection, PostgreSQL Transactional Outbox, Apache Kafka event streaming, and Kubernetes deployment.


## 🏛️ System Architecture

```mermaid
flowchart TD
    subgraph Ingestion & Anomaly Detection Engine
        Client[REST Client] -->|HTTP POST| Controller[Loan / Telemetry / Tx Controllers]
        Controller -->|Reactive Stream| Engine[Streaming Anomaly Detector<br/>Z-Score Outliers & Velocity Windows]
        Engine -->|Normal vs Flagged| OutboxSave
    end

    subgraph Atomic R2DBC Persistence & Ledger
        OutboxSave -->|Single Tx| DB[(PostgreSQL)]
      DB -->|outbox_events| Poller[Outbox Publisher<br/>poller default, Debezium CDC opt-in]
        DB -->|ledger_events| Ledger[(SHA-256 Ledger<br/>Type: ANOMALY_FLAGGED)]
    end

    subgraph Messaging & Observability
        Poller -->|Kafka Producer| Kafka[Apache Kafka]
        Kafka -->|Kafka Listener| CEP[Anomaly Event Listener<br/>routes by eventType]
        Kafka -.->|malformed| DLT[outbox-events.DLT]
        CEP -->|insert if absent| AlertOutbox[(anomaly_alert_outbox)]
        AlertOutbox -->|Alert Relay| AlertTopic[eventdrivenmicroservices-anomaly-alerts]

        Engine -.->|OTLP Metrics & Traces| OTel[OTel Collector]
        OTel --> Prometheus[Prometheus Metrics]
        OTel --> Tempo[Grafana Tempo Traces]
    end
```

### Core Components

1. **`apps/platform-engine` (Java 21 / Spring Boot 3 / WebFlux / R2DBC)**:
   - Non-blocking reactive HTTP ingestion API.
   - **Streaming Anomaly Detection Engine**: In-memory sliding window calculating transaction velocity bursts and telemetry Z-Score metric outliers.
   - **Transactional Outbox Pattern**: Persists domain entities (`loan_applications`, `telemetry_events`, `financial_transactions`) alongside `outbox_events` and append-only cryptographic `ledger_events` using PostgreSQL R2DBC.
   - **Outbox Publisher**: `OUTBOX_PUBLISHER_MODE=poller` (default) runs a leased, retrying scheduled relay; `cdc` runs the Debezium bridge instead. The two are mutually exclusive.
   - **Kafka Listener**: Validates the version-1 envelope, routes anomaly event types, durably records an alert intent in `anomaly_alert_outbox` (idempotent on redelivery), and sends malformed records to the `.DLT` topic. A relay publishes intents to `eventdrivenmicroservices-anomaly-alerts` with backoff.

2. **`deploy/helm`**:
   - Helm chart packaging the platform-engine alongside infrastructure services (Kafka, Zookeeper, PostgreSQL, Redis, OpenTelemetry Collector, Prometheus, Grafana, Tempo, Loki).
   - Configured with security contexts, init containers, and HPA for production-like deployment on local `kind` clusters.

---

## 🛠️ Key Architectural Patterns Demonstrated

- **Streaming Anomaly Detection**:
  - **Financial Velocity Anomaly**: Detects rapid high-value transactions from the same account within a sliding temporal window ($> \$10,000$ in 5s or $>5$ tx/s).
  - **Telemetry Z-Score Outlier**: Computes rolling mean ($\mu$) and standard deviation ($\sigma$) to flag statistical outliers ($Z = \frac{|x-\mu|}{\sigma} > 3.0$).
- **Transactional Outbox Pattern**: Enforces dual-write consistency by persisting business state and event records in a single database transaction prior to asynchronous Kafka publishing.
- **Reactive Non-Blocking Core**: Built on Spring WebFlux, Project Reactor, and R2DBC to maximize vertical I/O throughput without thread-per-request bottlenecks.
- **Cryptographic Ledger**: SHA-256 hash-chained append-only `ledger_events` table for tamper-evident transaction history.
- **Debezium (opt-in)**: Helm deploys Connect and a registration Job. CDC-to-envelope delivery and a connector restart were verified in Kind (CI `cdc-drill`); the default publisher remains the scheduled relay until Connect pod loss and bridge-crash replay are also proven (see the engineering guide).
- **Distributed Tracing & Monitoring**: OpenTelemetry (OTLP) metrics and trace exemplars routed to Prometheus and Grafana Tempo via the OTel Collector.
- **Kubernetes Deployment Hardening**: Security contexts (non-root, read-only filesystem), init containers for dependency readiness, and HPA autoscaling.

---

## 🚀 Quick Start & Centralized Harness

### Prerequisites
- Java 21 & Gradle 8.4
- Helm 3+ & kubectl
- Kind & OpenTofu/Terraform
- Docker or Podman (for local cluster execution)

### Centralized Build & Boot Script

Run the single entry-point script to validate the toolchain, run Gradle tests, provision the local Kind cluster, and deploy the Helm chart:

```bash
./scripts/start-all.sh
```

*(On Windows PowerShell, use `./scripts/start-all.ps1`)*

### Accessing & Testing the Platform

1. **Port-Forward the Platform Control Room UI & REST API**:
   ```bash
   kubectl port-forward svc/platform-engine 8080:8080
   ```
   Navigate to `http://localhost:8080/` in your browser. Log in with `admin` / `local-dev-password` to use the interactive loan, transaction, telemetry, outbox, and ledger control room.

2. **Port-Forward the Grafana Observability Dashboard**:
   ```bash
   kubectl port-forward svc/eventdrivenmicroservices-grafana 3000:3000
   ```
   Navigate to `http://localhost:3000` to inspect live application metrics, anomaly rate histograms, and trace exemplars.

3. **Execute the Automated Five-Minute Demo**:
   ```bash
   ./scripts/demo.sh
   ```

---

## 🧪 Testing

- **Java tests**:
  ```bash
  cd apps/platform-engine
  ./gradlew test
  ```
   The latest recorded result and Docker/Testcontainers caveats are in the [engineering guide](docs/engineering-guide.md).

## Current Limitations

- The project is a modular monolith. The scheduled outbox relay is the default publisher; Debezium CDC is opt-in; connector restart is CI-verified, while Connect pod loss and bridge-crash replay are not yet proven.
- Delivery is at-least-once with idempotent consumer effects, not exactly-once.
- The bearer-token check validates only the HMAC signature (no expiry/claims yet).
- Duplicate-delivery E2E tests need a Docker daemon compatible with the pinned Testcontainers; they are skipped locally when it is not.
- Trace headers are handled in some paths, but a complete live HTTP-to-Kafka-to-ledger trace has not been verified.
- The ledger is hash-chained, but database-level append-only enforcement and a full integrity verification operation are not implemented.
- The Helm PostgreSQL workload has no persistent volume. The chart is a local lab deployment, not a production-ready data platform.
- `local-dev-password` is a local fallback credential only. Provide runtime secrets explicitly and do not reuse these credentials outside a disposable development environment.

## Five-Minute Demo

Start the application locally or port-forward the deployed service to `localhost:8080`, then run:

```powershell
./scripts/demo.ps1
```

On Bash-compatible systems:

```bash
./scripts/demo.sh
```

The demo submits a loan, sends a normal transaction, sends a six-transaction velocity burst, and reads the outbox backlog plus ledger hash evidence. It does not provision infrastructure; use `scripts/test-local.ps1` or `scripts/test-local.sh` first when running against Kind.

The browser control room is available at `http://localhost:8080/` when the application is running. Enter the operator credentials, submit a loan, send a transaction burst, establish a telemetry baseline, inject a CPU outlier, and refresh the evidence panel to inspect the current ledger and outbox state. Each API response exposes `X-Correlation-Id` and, when an active span exists, `X-Trace-Id`.

---

## 📦 Tech Stack

| Technology | Role |
|---|---|
| Java 21 | Application runtime |
| Spring Boot 3 (WebFlux) | Reactive REST framework |
| PostgreSQL 15 (R2DBC) | Primary data store + Outbox + Ledger |
| Apache Kafka | Event streaming broker & CEP listener |
| Debezium | CDC connector for WAL-based outbox |
| Redis | Distributed caching and idempotency |
| Kubernetes (kind) | Container orchestration |
| Helm 3 | Kubernetes package management |
| Terraform | Infrastructure provisioning |
| Prometheus | Metrics collection & anomaly alerting |
| Grafana + Tempo + Loki | Dashboards, tracing, and log aggregation |
| OpenTelemetry | Distributed observability pipeline |
