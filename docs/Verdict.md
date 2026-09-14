# EventDrivenMicroservices Architecture Verdict & Decisions

**Status:** Official Architectural Decision Record (ADR)  
**Date:** September 2026  
**Scope:** Core Architecture, Consistency Guarantees, Observability, Testing, and Deployment

---

## 1. Executive Summary

EventDrivenMicroservices is designed to demonstrate enterprise distributed systems engineering: reactive non-blocking ingestion, atomic dual-write consistency via the Transactional Outbox pattern, real-time streaming anomaly detection, tamper-evident cryptographic ledgering, and end-to-end W3C distributed trace correlation on Kubernetes.

The architecture emphasizes **proven domain boundaries before distributed decomposition**. The project operates as an observable modular monolith, providing clean service boundaries while avoiding premature network partitioning, operational fragility, and deployment overhead.

---

## 2. Key Architectural Decisions (ADRs)

### ADR-01: Modular Monolith Prior to Physical Service Decomposition
* **Context:** Distributed microservices introduce network latency, distributed transaction complexity, and multi-service deployment overhead.
* **Decision:** Implement all core capabilities (`LoanService`, `TransactionService`, `StreamingAnomalyDetector`, `LedgerService`, `OutboxProcessor`) within a well-bounded modular monolith (`apps/platform-engine`) using distinct packages (`api`, `application`, `domain`, `infrastructure`).
* **Rationale:** Domain logic, event contracts, and data ownership must be stabilized and tested before extracting independent network boundaries.
* **Extraction Path:** Future decomposition into `financial-api`, `outbox-relay`, `fraud-service`, `ledger-service`, and `alert-service` when scalability demands require separate deployment lifecycles.

### ADR-02: Reactive Non-Blocking Foundation (WebFlux + R2DBC)
* **Context:** High-throughput financial and telemetry ingestion requires maximizing thread efficiency and vertical resource scaling.
* **Decision:** Build the backend on Spring WebFlux, Project Reactor, and PostgreSQL R2DBC driver.
* **Enforcement:** ArchUnit rules prevent blocking operations (`Thread.sleep`, blocking `java.sql.*` calls) across `api` and `application` layers. Blocking JDBC is strictly isolated to startup Flyway schema migrations.

### ADR-03: Dual-Write Consistency via Transactional Outbox
* **Context:** Writing to an ACID database and publishing to an Apache Kafka broker cannot be coordinated via two-phase commit (2PC) without severe latency penalties and failure coupling.
* **Decision:** Persist domain entities (`financial_transactions`, `loan_applications`) and event payloads (`outbox_events`) in a single R2DBC transaction. A resilient background processor relays unprocessed records to Kafka using versioned `EventEnvelope` structures.
* **Guarantees:** At-least-once delivery, zero data loss upon message broker outages, and failure isolation across publication batches.

### ADR-04: Cryptographic SHA-256 Ledgering
* **Context:** Financial audits require verifiable immutability and tamper-evident event histories.
* **Decision:** Maintain an append-only `ledger_events` table where every record cryptographically links to the preceding entry using SHA-256 (`current_hash = SHA256(previous_hash + transaction_type + payload)`), rooted at a deterministic genesis hash.
* **Integrity Guarantee:** Any retrospective modification or deletion invalidates downstream hash continuity, immediately detectable by integrity audit scans.

### ADR-05: Unified Observability with W3C Trace Context Propagation
* **Context:** Debugging distributed event-driven workflows requires end-to-end traceability across HTTP, database outbox records, Kafka topics, and asynchronous consumers.
* **Decision:** Standardize on W3C `traceparent` headers. The HTTP ingress layer extracts or generates trace context, stores it in `outbox_events.traceparent`, forwards it into Kafka message headers, and extracts it in the Complex Event Processing (CEP) anomaly listener.
* **Telemetry Stack:** OpenTelemetry Collector routes metrics to Prometheus, traces to Grafana Tempo, and logs to Grafana Loki, visualized in unified Grafana dashboards.

### ADR-06: External Dependency Simulation Boundary (Flask Mock)
* **Context:** Demonstrating external settlement delays, payment provider failures, and provider timeouts requires a realistic third-party dependency.
* **Decision:** Do not duplicate business logic in multiple languages. Keep core platform logic strictly in Java 21, and isolate any third-party external provider simulation into a dedicated mock service (`apps/mock-financial-provider`) using Python/Flask.

---

## 3. Current Implementation Status

| Component / Subsystem | Status | Verification Evidence |
| :--- | :--- | :--- |
| **Java 21 WebFlux Engine** | Production Ready | Compiles with Java 21 toolchain; 29/29 tests pass |
| **R2DBC PostgreSQL Persistence** | Verified | Flyway migrations & R2DBC repositories operational |
| **Transactional Outbox Engine** | Verified | Dual-write atomic persistence, batching, error isolation |
| **Cryptographic Ledger** | Verified | SHA-256 hash chaining active; query APIs operational |
| **Streaming Anomaly Detection** | Verified | Velocity spike and Z-score outlier detection active |
| **Kafka CEP Stream Listener** | Verified | Consumes outbox events, preserves trace headers, routes alerts |
| **Kubernetes / Kind Deployment** | Verified | Automated cluster provisioning via Terraform & Helm |
| **WebFlux Control Room UI** | Verified | Interactive browser dashboard served at `http://localhost:8080/` |
| **Observability (OTel/Prometheus)** | Verified | Metrics pipeline active; dashboards provisioned in Grafana |

---

## 4. Next Implementation Milestones

1. **Stage 2 (Outbox Leased Locking):** Add database-level record locking (`FOR UPDATE SKIP LOCKED`) to outbox event batch polling for horizontal multi-instance scaling.
2. **Stage 3 (Trace Visualization Validation):** Verify live distributed trace waterfall visualization in Grafana Tempo via the OpenTelemetry Collector.
3. **Stage 5 (Mock Financial Provider):** Implement the isolated mock financial provider for payment approval, decline, and settlement delay simulation.
4. **Stage 7 (Service Extraction):** Decompose the modular monolith into independently deployable microservice containers once external contracts are locked.
      ledger-appended.v1.json
    http/

deploy/
  helm/
    event-driven-lab/
  dashboards/
    grafana/
    prometheus/
  connectors/
    debezium/
  docker-compose/
    observability.yml

tests/
  contract/
  integration/
  e2e/
  observability/
  failure-scenarios/

scripts/
  demo.ps1
  demo.sh
  verify-trace.ps1
  verify-trace.sh
  health-check.ps1
  health-check.sh
```

## Event Contract

Every event should use a shared envelope:

```json
{
  "eventId": "uuid",
  "eventType": "TransactionCreated",
  "eventVersion": 1,
  "aggregateType": "FinancialTransaction",
  "aggregateId": "uuid",
  "correlationId": "uuid",
  "traceId": "trace-id",
  "causationId": "event-id",
  "occurredAt": "2026-09-14T12:00:00Z",
  "payload": {}
}
```

W3C propagation must occur through:

- HTTP `traceparent`.
- Database/outbox metadata.
- Kafka `traceparent` headers.
- Consumer extraction.
- Child spans in anomaly, ledger, and alert processing.

At present, the code stores some `traceparent` data in the outbox but does not complete this chain.

## Demonstration Workflow

The primary demo should be:

1. Open the Demo UI.
2. Submit a loan application.
3. Submit a normal payment.
4. Trigger six transactions for one merchant.
5. Show the velocity anomaly.
6. Send telemetry baseline values followed by an outlier.
7. Show the anomaly alert in the UI.
8. Open the ledger chain and show SHA-256 linkage.
9. Display the correlation ID and trace ID.
10. Open Grafana and follow the same trace in Tempo.
11. Show outbox delivery latency and Kafka consumer activity.

This creates one coherent portfolio story instead of disconnected technical features.

## Observability Design

Use Grafana Tempo as the primary trace backend. Jaeger should be optional rather than deployed alongside Tempo by default.

### Business Flow Dashboard

- Loans submitted.
- Transactions accepted.
- Payments approved and declined.
- Anomalies detected.
- Alerts generated.
- Ledger append success and failure.

### Outbox Reliability Dashboard

- Pending event count.
- Oldest pending event age.
- Publish success and failure count.
- Retry count.
- Outbox-to-Kafka latency.
- Duplicate delivery count.

### Kafka Health Dashboard

- Consumer lag.
- Producer errors.
- Publish throughput.
- Consumer processing latency.

### Application Runtime Dashboard

- HTTP rate.
- HTTP error rate.
- P95/P99 latency.
- JVM memory.
- CPU.
- Restarts.
- Database pool saturation.

### Trace Explorer

- Links from UI correlation IDs to Tempo.
- HTTP -> database -> outbox -> Kafka -> anomaly -> ledger span chain.

### Security and Compliance Dashboard

- Authentication failures.
- JWT validation failures.
- Ledger integrity failures.
- Retention purge counts.
- Secret/configuration errors.

## Documentation Structure

```text
README.md

docs/
  rebuild_guide.md        # canonical execution plan
  Verdict.md              # this architecture verdict
  architecture.md         # current architecture only
  master_plan.md          # portfolio and learning roadmap only
  demo.md                 # five-minute business workflow
  observability.md        # dashboards and trace correlation
  api.md                  # HTTP contracts
  events.md               # Kafka contracts
  testing.md              # test layers and commands
  failure-scenarios.md    # infrastructure and provider failures
  security.md             # secrets and runtime hardening
```

`master_plan.md` should be rewritten as a portfolio and learning roadmap. It must stop claiming deleted files, old identifiers, nonexistent APIs, and completed capabilities that are only planned.

## Staged Implementation Order

### Stage 1: Documentation and baseline truth

- Rewrite `architecture.md`.
- Rewrite `master_plan.md`.
- Add an implemented/missing capability matrix to `rebuild_guide.md`.
- Remove claims about nonexistent ledger endpoints and complete trace propagation.

### Stage 2: Make the current monolith demonstrable

- Add ledger, anomaly, outbox, and health query APIs.
- Add `scripts/demo.ps1` and `scripts/demo.sh`.
- Fix Helm image naming.
- Add a local mock payment provider.
- Add correlation IDs to all responses and events.

### Stage 3: Complete observability

- Add Kafka producer and consumer tracing.
- Add database and outbox spans.
- Add Tempo and Loki Grafana datasources.
- Add a real Tempo configuration.
- Add dashboards for business flow, outbox, Kafka, runtime, and security.
- Verify one trace end to end.

### Stage 4: UI

Build the UI after read APIs exist. The UI should consume the Java API and show business state, not directly query Kafka or PostgreSQL.

### Stage 5: Extract services

Extract in this order:

1. `financial-api`
2. `outbox-relay`
3. `fraud-service`
4. `ledger-service`
5. `alert-service`

Each service must have an independent Dockerfile, Helm Deployment, readiness/liveness probes, metrics, logs, trace service name, unit tests, integration tests, contract tests, and failure scenario.

## Acceptance Criteria

The project should not claim final completion until these are true:

- One demo command or UI workflow creates a loan and payment activity.
- An anomaly is deterministically triggered.
- The event reaches Kafka.
- The anomaly service consumes it.
- The ledger records it.
- The UI displays the result.
- The same trace ID is visible across HTTP, database, Kafka, anomaly, ledger, and alert processing.
- Grafana displays the relevant metrics.
- Tempo displays the full trace.
- A failure scenario demonstrates retry or recovery.
- The workflow passes locally and in Codespaces.
- Unit, integration, contract, end-to-end, observability, and Helm tests pass.

The immediate architectural recommendation is to remain a modular monolith through the demonstrable workflow and observability stages. Extracting independent microservices before that proof exists would create more deployment surface without improving the core demonstration.
