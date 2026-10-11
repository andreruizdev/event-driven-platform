# EventDrivenMicroservices System Architecture

**Status:** Official System Architecture Specification  
**Version:** 2.0  
**Related Documents:** [engineering-guide.md](engineering-guide.md), [README.md](../README.md)

This document describes the implementation, not a target production architecture. The project is a single Spring application deployed with local supporting infrastructure. Current build/test evidence and remaining work are tracked in [engineering-guide.md](engineering-guide.md).

---

## 1. System Topology

The platform operates as an observable modular monolith deployed to Kubernetes via Helm. All core financial ingestion, outbox dispatch, stream processing, anomaly detection, and cryptographic ledgering run within `platform-engine`, backed by PostgreSQL, Redis, and Apache Kafka.

```mermaid
flowchart TD
    Client[HTTP Client / Control Room UI] -->|HTTP / REST| API[platform-engine WebFlux]
    
    subgraph Data Consistency & Ledger
        API -->|Atomic Reactive Tx| PG[(PostgreSQL 15 R2DBC)]
        PG -->|outbox_events| RELAY[Outbox Processor Relay - default mode]
        PG -->|logical WAL - CDC mode| DEBEZIUM[Debezium PostgreSQL Connector]
        DEBEZIUM -->|raw row-change event| CDC[CDC shadow topic]
        CDC --> BRIDGE[CDC Envelope Bridge]
        BRIDGE -->|versioned EventEnvelope| KAFKA
        PG -->|ledger_events| LEDGER[SHA-256 Chained Ledger]
    end

    subgraph Streaming & Analytics
        RELAY -->|Versioned EventEnvelope| KAFKA[Apache Kafka]
        KAFKA -->|Kafka Consumer Group| LISTENER[Anomaly Event Listener]
        LISTENER -->|insertIfAbsent by eventId| ALERTBOX[(anomaly_alert_outbox)]
        ALERTBOX --> ALERTRELAY[Alert Outbox Relay]
        ALERTRELAY -->|publish, then mark published| ALERT[eventdrivenmicroservices-anomaly-alerts]
        API <-->|Sliding Window State| REDIS[(Redis 7, single node)]
    end

    subgraph Unified Observability
        API -. W3C Spans / Metrics .-> OTEL[OpenTelemetry Collector]
        OTEL --> PROM[Prometheus]
        OTEL --> TEMPO[Grafana Tempo]
        OTEL --> LOKI[Grafana Loki]
        PROM --> GRAFANA[Grafana Dashboards]
        TEMPO --> GRAFANA
        LOKI --> GRAFANA
    end
```

---

## 2. Ingestion & API Layer

The HTTP layer uses Spring WebFlux and is protected by HTTP Basic and a custom HMAC Bearer-token check. The token check verifies the signature but does not currently validate standard JWT claims, algorithm, or expiry, and grants no authorities (only the fixed `eventdrivenmicroservices-edge-node` principal).

* `POST /api/v1/loans`: Ingests loan origination applications, validates applicant UUIDs and dynamic loan tier properties, persists the loan, creates an outbox event, and appends an `ApplicationSubmitted` record to the SHA-256 ledger.
* `POST /api/v1/transactions`: Ingests financial transactions, evaluates real-time sliding-window velocity thresholds, and dual-writes transaction state with outbox events.
* `POST /api/v1/telemetry`: High-throughput telemetry ingestion evaluating rolling Z-score outliers ($Z > 3.0$).
* `GET /api/v1/ledger`: Retrieves chronological ledger event history with hash proofs.
* `GET /api/v1/ledger/latest-hash`: Exposes the latest cryptographic head of the SHA-256 chain.
* `GET /api/v1/ledger/verify`: Recomputes the full chain in append order and returns `valid`, `eventsChecked`, and the first invalid event ID and reason, if any.
* `GET /api/v1/outbox/status`: Returns current outbox backlog depth, oldest unprocessed record timestamp, and queue age.
* `GET /`: Interactive web control room serving static WebFlux resources for operators and live demos.

### Context Propagation
Every HTTP response includes an `X-Correlation-Id` header (generated or preserved from client requests) and an `X-Trace-Id` header when the filter observes an active OpenTelemetry span. End-to-end trace continuity is not currently verified.

---

## 3. Transactional Outbox Pattern

To provide at-least-once delivery without distributed two-phase commit:
1. **Atomic Dual-Write:** The business entity and the corresponding outbox record are committed together in a single PostgreSQL R2DBC transaction.
2. **Selectable Publisher:** `OUTBOX_PUBLISHER_MODE` is `poller` by default. In poller mode, a background publisher queries pending events in batches of up to 50, claims leases, publishes sequentially, and isolates per-event failures with capped exponential retry delay. In opt-in `cdc` mode, PostgreSQL logical replication feeds Debezium's insert-only shadow topic; the CDC bridge maps the row to the same envelope and canonical topic, waits for Kafka acknowledgement, then sets `processed` to mean published (not consumer-processed). The two publishers are mutually exclusive.
3. **Kafka Header Injection:** When present, the stored W3C `traceparent` is injected into the Kafka record header. This is partial propagation, not a verified end-to-end trace.
4. **Contract Envelope:** Payloads are serialized using the current `EventEnvelope` schema (`eventVersion`):
   ```json
   {
    "eventId": "uuid",
    "eventType": "ApplicationSubmitted",
    "eventVersion": 1,
    "aggregateType": "LoanApplication",
    "aggregateId": "11111111-1111-4111-8111-111111111111",
    "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    "occurredAt": "2026-09-14T19:11:00.437Z",
    "payload": "{...}"
   }
   ```

  CDC's canonical publish and the subsequent `processed` update are not a distributed transaction. If the bridge publishes and then fails before updating PostgreSQL, replay can publish the same event again; consumers must deduplicate by stable `eventId`. Connector restart/replay and consumer idempotency are not yet verified.

---

## 4. Cryptographic Ledgering

The ledger stores hash-linked records for loan submissions and selected anomaly events:
* **Genesis Hash:** Rooted at deterministic `64-zero` SHA-256 string (`0000000000000000000000000000000000000000000000000000000000000000`).
* **Hash Chaining:** Each record calculates:
  $$\text{current\_hash} = \text{SHA-256}(\text{previous\_hash} + \text{transaction\_type} + \text{payload})$$
* **Current limitation:** A unique index on `previous_hash` prevents forks, but no database trigger prevents update/delete and no integrity-verification API exists. Hash linkage is implemented; enforced immutability and automated tamper detection are not.

---

## 5. Streaming Anomaly Detection Engine

The anomaly detection engine runs in-process with dual-backend state management:
* **Financial Velocity Anomaly:** Flags a five-second window when transaction count reaches five or the summed amount exceeds $10,000. State uses Redis when available, with a per-process in-memory fallback that is not shared across replicas.
* **Telemetry Z-Score Outlier:** Calculates rolling mean ($\mu$) and standard deviation ($\sigma$) from prior readings (up to 20), flagging an observation where:
  $$Z = \frac{|x - \mu|}{\sigma} \ge 3.0$$
* **Kafka anomaly listener:** Parses `EventEnvelope` and routes only recognized anomaly `eventType` values. It persists a PostgreSQL alert intent keyed by `eventId` before returning; a leased relay publishes the intent to the alert topic and marks it delivered only after Kafka acknowledgement. Repeated input events create one intent. Malformed or unsupported envelopes go to a seven-day dead-letter topic with original Kafka coordinates; transient listener failures retry with a one-second interval. Delivery remains at-least-once: a relay crash after Kafka acknowledgement but before the database update can duplicate an alert, so downstream alert consumers must deduplicate by `eventId`. This is not a rules-based CEP engine.

---

## 6. Observability & Infrastructure

The local lab stack is packaged in the canonical Helm chart `deploy/helm/event-driven-lab`:
* **OpenTelemetry Collector:** Central gateway receiving OTLP gRPC/HTTP signals on ports 4317 and 4318.
* **Grafana Tempo:** Distributed trace storage supporting traceparent lookups.
* **Prometheus:** Metrics scraping endpoint monitoring throughput, JVM metrics, and custom anomaly counters.
* **Grafana Loki:** Centralized log aggregation across all Kubernetes pods.
* **Grafana:** Dashboards pre-provisioned with Prometheus, Tempo, and Loki datasources.
* **Local deployment status:** The platform container runs non-root with a read-only root filesystem and external database/JWT secret references. PostgreSQL is a single-replica Deployment without a persistent volume; several infrastructure images use `latest`, and not all pods have probes or equivalent hardening. Treat this chart as a local lab, not production-ready deployment configuration.
Current verification evidence and implementation priorities live in [engineering-guide.md](engineering-guide.md).

