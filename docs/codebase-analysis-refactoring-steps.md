# Codebase Analysis and Refactoring Steps

**Project:** EventDrivenMicroservices
**Date:** September 14, 2026
**Related documents:** [rebuild_guide.md](rebuild_guide.md), [Verdict.md](Verdict.md), [README.md](../README.md)

## 1. Purpose

This document translates the current codebase analysis into an executable refactoring path. The goal is to produce a visible, truthful demo quickly while preserving the phased rebuild model and leaving a clean path toward independently deployable microservices.

The guiding rule is:

> Build one complete, observable business workflow before extracting independent services.

The project should become demonstrable first, modular second, and distributed only where the boundaries are proven by behavior and operational evidence.

## 2. Current Baseline

### 2.1 What exists

- Java 21 / Gradle 8.4 Spring Boot application in `apps/platform-engine`.
- Spring WebFlux and Reactor request paths.
- R2DBC repositories and PostgreSQL Flyway migrations.
- Loan, transaction, and telemetry ingestion APIs.
- Transactional outbox persistence.
- Scheduled Kafka publishing.
- Redis-backed and in-memory anomaly detection.
- SHA-256 ledger append logic.
- JWT and Basic authentication boundaries.
- Helm deployment for application, PostgreSQL, Redis, Kafka, Debezium, Prometheus, Grafana, Tempo, Loki, and OpenTelemetry Collector.
- Terraform Kind environment.
- Java tests, ArchUnit tests, Testcontainers scaffolding, CI, and local deployment scripts.

### 2.2 What is not yet proven

- Full HTTP -> PostgreSQL -> outbox -> Kafka -> anomaly -> ledger trace correlation.
- Kafka trace-header propagation and consumer context extraction.
- Debezium connector registration and verified CDC delivery.
- Idempotent outbox claiming and retry behavior.
- Ledger query and integrity APIs.
- Application UI.
- Mock external financial provider.
- Complete Grafana Tempo and Loki datasource configuration.
- Complete runtime test suite with Docker/Testcontainers.
- Clean Kind deployment from a fresh checkout.

### 2.3 Documentation status

- `docs/rebuild_guide.md` is the canonical phased implementation plan.
- `docs/Verdict.md` is the architecture and coherence decision record.
- `README.md` should become the public project and demo entry point.
- `docs/architecture.md` must describe implemented architecture only.
- `docs/master_plan.md` should describe the long-term learning and portfolio roadmap, not current runtime behavior.

## 3. Target Refactoring Principles

1. Preserve the Phase 0 build and deployment baseline.
2. Prefer a modular monolith before service extraction.
3. Separate business workflows from infrastructure adapters.
4. Define event contracts before creating more consumers.
5. Make every claim executable through a test, command, dashboard, or trace.
6. Keep the business UI separate from Grafana infrastructure dashboards.
7. Use Tempo as the default trace backend; keep Jaeger optional.
8. Use Flask only for an external mock provider, not for duplicating the Java core.
9. Keep secrets outside source-controlled Helm templates and application code.
10. Extract a service only after its data, API, event, and operational ownership are clear.

## 4. Refactoring Destination

### 4.1 Initial modular monolith

Refactor the current Java package layout toward feature ownership:

```text
com.eventdrivenmicroservices.platform
├── api
│   ├── loan
│   ├── transaction
│   ├── telemetry
│   ├── anomaly
│   ├── ledger
│   └── outbox
├── application
│   ├── loan
│   ├── transaction
│   ├── telemetry
│   └── payment
├── domain
│   ├── loan
│   ├── transaction
│   ├── anomaly
│   └── ledger
├── infrastructure
│   ├── postgres
│   ├── redis
│   ├── kafka
│   ├── outbox
│   └── observability
├── security
└── config
```

The first refactor should move classes behind these ownership boundaries without changing deployment topology.

### 4.2 Future service topology

```text
apps/
├── platform-engine/          # initial modular monolith / financial API
├── web-ui/                   # business workflow UI
├── mock-financial-provider/  # optional Flask external dependency
└── contracts/                # HTTP and event schemas

services later extracted:
├── financial-api
├── outbox-relay
├── fraud-service
├── ledger-service
└── alert-service
```

## 5. Staged Refactoring Plan

## Refactoring Progress

- Documentation hierarchy reconciled: `README.md`, `architecture.md`, `master_plan.md`, `Verdict.md`, and this playbook now have distinct responsibilities.
- HTTP controller boundary completed: production controllers and controller tests moved from `controller` to `api` with routes unchanged.
- Application boundary completed: orchestration services and their tests moved from `service` to `application` with behavior unchanged.
- `gradlew.bat clean compileJava --no-daemon` passed after the application move.
- `gradlew.bat compileTestJava --no-daemon` passed after the application move.
- Infrastructure persistence boundary completed: PostgreSQL repositories moved under `infrastructure.postgres`; outbox entity, repository, and processor moved under `infrastructure.outbox`.
- `gradlew.bat clean compileJava compileTestJava --no-daemon` passed after the infrastructure move.
- Adapter boundary completed: database, Redis, Kafka listener, outbox, and observability adapters now have explicit infrastructure ownership; security is under `platform.security`.
- Deprecated empty `AsyncExecutorConfig` was removed.
- `gradlew.bat clean compileJava compileTestJava --no-daemon` passed after the complete adapter move.
- Read API slice completed: added authenticated ledger history/latest-hash and outbox backlog-status endpoints under `api`.
- Focused `LedgerControllerTest` and `OutboxControllerTest` pass with the real Basic-auth security chain.
- Deterministic demo workflow added as `scripts/demo.ps1` and `scripts/demo.sh`; it exercises loan submission, normal transaction, velocity burst, outbox status, and ledger evidence without provisioning infrastructure.
- PowerShell demo syntax passed; Bash syntax was not executable in the current Windows session because Bash is unavailable.
- Lightweight business UI added under `src/main/resources` with authenticated loan, transaction-burst, telemetry outlier, ledger, and outbox workflows.
- `gradlew.bat clean compileJava compileTestJava --no-daemon` passed after the UI integration.
- Telemetry baseline/outlier controls added; the UI now demonstrates both transaction velocity and telemetry Z-score paths.
- `gradlew.bat clean compileJava compileTestJava --no-daemon` passed after telemetry UI integration.
- Correlation layer added through `CorrelationIdWebFilter`; API responses expose `X-Correlation-Id` and active-span `X-Trace-Id`, and the UI displays both.
- `gradlew.bat clean compileJava compileTestJava --no-daemon` passed after correlation integration.
- W3C `traceparent` now propagates from outbox records into Kafka producer headers, is extracted by the anomaly listener, and is forwarded to alert messages.
- `gradlew.bat clean compileJava compileTestJava --no-daemon` passed after Kafka propagation integration.
- Added `AnomalyEventListenerTest` asserting traceparent preservation from the outbox topic to the anomaly-alert topic.
- Focused Kafka header test passes.
- Added versioned `EventEnvelope` serialization at the Kafka publication boundary and `EventEnvelopeTest` for metadata/payload contract stability.
- Focused envelope and Kafka trace tests pass.
- Added `docs/testing.md` with layered unit, contract, PostgreSQL, Kafka, E2E, observability, and failure-scenario strategy.
- Next implementation slice: run Docker-gated Testcontainers checks and add a trace-flow integration test across persistence, Kafka, anomaly processing, and ledger evidence.

## Stage 0: Freeze and Verify Phase 0

### Goal

Create a stable baseline before changing behavior.

### Steps

1. Run Java 21 production compilation.
2. Run Helm lint and template rendering.
3. Confirm `JAVA_HOME` and VS Code Gradle tooling use Java 21.
4. Confirm all secrets are externalized.
5. Confirm no legacy project identifiers remain.
6. Record known full-test and runtime blockers.

### Exit gate

```text
./gradlew clean compileJava --no-daemon        PASS
helm lint deploy/helm/event-driven-lab         PASS
helm template event-driven-lab ...             PASS
```

Do not claim full Phase 0 runtime completion until Docker/Testcontainers and Kind validation are also passing.

## Stage 1: Create the Showcase Slice

### Goal

Ship something visible and understandable from the current codebase.

### Steps

1. Add read APIs for:
   - Recent anomalies.
   - Ledger chain.
   - Ledger integrity status.
   - Outbox pending status.
   - Application health and dependency status.
2. Add a demo script in both PowerShell and Bash.
3. Create one deterministic workflow:
   - Submit a loan.
   - Submit a normal transaction.
   - Submit a transaction burst.
   - Trigger a velocity anomaly.
   - Query the resulting anomaly and ledger record.
4. Add a lightweight UI served by Spring WebFlux static resources.
5. Add correlation ID to responses and displayed results.
6. Update `README.md` with a five-minute demo path.

### Exit gate

A new user can run one documented command, submit a request, trigger an anomaly, and see the resulting ledger state without reading the implementation first.

## Stage 2: Stabilize the Outbox

### Goal

Make the core event-driven claim reliable and measurable.

### Steps

1. Introduce an event envelope with event ID, type, version, aggregate ID, correlation ID, trace ID, causation ID, and timestamp.
2. Add claiming or leasing for pending outbox records.
3. Prevent concurrent duplicate processing.
4. Add retry count and last-error fields.
5. Add publish success, failure, retry, backlog, age, and latency metrics.
6. Test Kafka unavailable, publish failure, retry, and duplicate delivery.
7. Decide whether the initial demo uses the scheduled relay or Debezium as the primary path.
8. Do not claim both delivery mechanisms are active until both are verified.

### Exit gate

An outbox integration test proves that domain persistence and event persistence are atomic, failed publication is retried, and duplicate delivery does not duplicate ledger effects.

## Stage 3: Complete Trace Correlation

### Goal

Make one business request followable across every important hop.

### Steps

1. Extract W3C trace context from HTTP headers.
2. Create application spans for validation and business decisions.
3. Add database spans for domain and outbox writes.
4. Inject W3C trace context into Kafka headers.
5. Extract context in Kafka consumers.
6. Add anomaly, ledger, and alert child spans.
7. Store correlation ID and trace ID in event metadata.
8. Return correlation ID and trace ID from demo-facing APIs.

### Exit gate

A single request produces one trace visible in Tempo containing HTTP, PostgreSQL, outbox, Kafka, anomaly, ledger, and alert spans.

## Stage 4: Finish the Observability Surface

### Goal

Turn infrastructure into evidence for the business workflow.

### Dashboards

1. Business flow.
2. Outbox reliability.
3. Kafka health.
4. Application runtime.
5. Security and compliance.
6. Trace exploration.

### Steps

1. Add Tempo and Loki Grafana datasources.
2. Add and validate Tempo configuration.
3. Align dashboard queries with emitted metric names.
4. Add outbox backlog and latency metrics.
5. Add anomaly counters by type.
6. Add ledger integrity and append failure metrics.
7. Add alert rules for failed publication, growing backlog, and dependency failure.

### Exit gate

The demo UI and Grafana dashboards show the same request, event, anomaly, and ledger outcome from different operational perspectives.

## Stage 5: Add the Mock Financial Provider

### Goal

Demonstrate a realistic external dependency and failure handling.

### Structure

```text
apps/mock-financial-provider/
├── app.py
├── routes/
│   ├── payments.py
│   └── settlements.py
├── services/
├── tracing/
└── tests/
```

### Scenarios

- Payment approved.
- Payment declined.
- Provider timeout.
- Delayed settlement.
- Duplicate response.
- Provider unavailable.

### Exit gate

The Java application handles provider success and failure while preserving correlation IDs and trace continuity.

## Stage 6: Harden the Database and Security Boundaries

### Database

1. Add ledger query and verification support.
2. Add database-level append-only enforcement where required.
3. Add forward-only telemetry partition lifecycle migrations.
4. Verify retention boundaries.
5. Add repository tests against PostgreSQL.

### Security

1. Add valid, invalid, expired, malformed, and missing JWT tests.
2. Use constant-time HMAC comparison.
3. Make missing production secrets fail startup.
4. Verify actuator role protection.
5. Verify no secrets exist in source, examples, Helm templates, or demo output.

### Exit gate

Security and data lifecycle behavior is tested, documented, and observable.

## Stage 7: Extract Services Carefully

Extract only after Stages 1 through 6 are stable.

Recommended order:

1. `financial-api`
2. `outbox-relay`
3. `fraud-service`
4. `ledger-service`
5. `alert-service`

Each extracted service requires:

- Independent build and container image.
- Helm deployment.
- Readiness and liveness probes.
- Service-specific metrics and logs.
- OpenTelemetry service name.
- Unit and integration tests.
- Contract tests.
- Failure scenario.
- Clearly owned persistence and event contracts.

## 6. Test Strategy

### Unit tests

- Validation rules.
- Velocity thresholds.
- Z-score warmup and outliers.
- Redis fallback.
- Ledger hash calculation.
- Event envelope creation.

### Integration tests

- PostgreSQL repositories.
- Flyway startup.
- Transactional outbox.
- Kafka producer and consumer.
- Redis state.
- Mock provider adapter.

### Contract tests

- HTTP request and response schemas.
- Kafka event versions.
- Provider failure contracts.

### End-to-end tests

```text
UI or HTTP
  -> financial API
  -> PostgreSQL
  -> outbox
  -> Kafka
  -> fraud/anomaly processing
  -> ledger
  -> alert
  -> UI query
```

### Observability tests

- Correlation ID is preserved.
- Trace ID appears in response, logs, event metadata, and Kafka headers.
- Expected spans exist in the trace backend.
- Metrics are emitted with stable names.

### Deployment tests

- Helm lint.
- Helm render.
- Container startup.
- Kind readiness.
- External secret creation.
- Probe behavior.
- Failure and recovery scenarios.

## 7. Public Demo and Portfolio Packaging

### README structure

1. One-paragraph project story.
2. Screenshot or short demo recording.
3. Five-minute quick start.
4. Business workflow diagram.
5. Observability screenshots.
6. Current capabilities.
7. Known limitations.
8. Rebuild roadmap.
9. Testing evidence.
10. Resume-ready technical summary.

### Demonstration script

The public demo should show:

1. Cluster and service readiness.
2. Loan submission.
3. Normal payment.
4. Transaction burst.
5. Anomaly detection.
6. Ledger append and verification.
7. Outbox delivery.
8. Grafana metrics.
9. Tempo trace correlation.
10. One controlled failure and recovery.

### Claims policy

Only claim behavior that is verified by an executable test, deployment check, dashboard, trace, or reproducible demo command.

## 8. Definition of Done

The first public release is ready when:

- The application starts locally and in Codespaces.
- One command or UI workflow demonstrates the complete business path.
- The workflow creates a deterministic anomaly.
- Kafka delivery is observable.
- The ledger result is queryable and verifiable.
- The same trace is visible across the workflow.
- Grafana dashboards show business and infrastructure signals.
- A failure scenario demonstrates recovery behavior.
- README instructions work from a clean checkout.
- Tests and deployment checks are reported honestly.

The long-term microservice architecture is an extension of this proven vertical slice, not a replacement for it.
