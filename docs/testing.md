# EventDrivenMicroservices Testing Strategy

**Canonical implementation plan:** [rebuild_guide.md](rebuild_guide.md)
**Current refactoring status:** [codebase-analysis-refactoring-steps.md](codebase-analysis-refactoring-steps.md)

## Test Layers

### Layer 1: Fast unit and contract tests

Runs without Docker or external services.

- Anomaly velocity and telemetry Z-score behavior.
- Application service serialization and rollback behavior.
- WebFlux controller contracts.
- Ledger and outbox read API contracts.
- Event envelope serialization.
- Kafka traceparent forwarding contract.
- ArchUnit reactive architecture rules.

Command:

```powershell
Set-Location apps/platform-engine
./gradlew.bat test --tests "*ControllerTest" --tests "*ServiceTest" --tests "*EventEnvelopeTest" --tests "*AnomalyEventListenerTest" --tests "*ArchitectureTest" --no-daemon
```

### Layer 2: PostgreSQL integration tests

Requires Docker or Podman with Testcontainers support.

- Flyway migration startup.
- R2DBC repository mappings.
- Transactional domain plus outbox persistence.
- Ledger hash-chain persistence.
- Telemetry partition and retention behavior.
- Outbox pending and processed queries.

Current coverage entry point: `IntegrationTests`.

### Layer 3: Kafka and outbox integration tests

Requires Docker or Podman.

- Publish an outbox envelope to `eventdrivenmicroservices-outbox-events`.
- Assert the `traceparent` Kafka header is present.
- Consume the envelope in the anomaly listener.
- Assert anomaly alert publication preserves the header.
- Assert failed publication does not mark the outbox record processed.
- Assert retry and duplicate delivery behavior.

Current coverage entry point: `E2EOutboxIntegrationTest` plus the focused `AnomalyEventListenerTest` contract test.

### Layer 4: End-to-end business workflow

Requires the complete local stack or a Codespaces deployment.

```text
UI or HTTP request
  -> authenticated API
  -> PostgreSQL domain write
  -> outbox write
  -> Kafka envelope
  -> anomaly processing
  -> ledger append
  -> alert publication
  -> UI evidence refresh
```

The workflow must verify:

- Loan submission succeeds.
- Transaction velocity burst is detected.
- Telemetry outlier is detected.
- Ledger state is queryable.
- Outbox state is queryable.
- `X-Correlation-Id` is returned.
- `X-Trace-Id` is returned when an active span exists.
- `traceparent` is preserved in Kafka headers.

### Layer 5: Observability verification

Requires the OpenTelemetry collector and trace backend.

- HTTP span exists.
- Domain decision span exists.
- Database/outbox span exists.
- Kafka producer and consumer spans are correlated.
- Anomaly and ledger spans share the trace ID.
- Grafana/Tempo can locate the trace by the displayed ID.
- Metrics identify request rate, errors, anomaly counts, outbox backlog, and event latency.

## Current Evidence Status

| Capability | Verification Mechanism | Status | Notes |
|---|---|---|---|
| Java production compilation | Gradle Java 21 compile | **Verified** | `./gradlew clean compileJava` passes |
| Java test compilation | Gradle test compile | **Verified** | `./gradlew compileTestJava` passes |
| Controller contracts | WebFlux slice tests | **Verified** | `LoanControllerTest`, `TransactionControllerTest`, `TelemetryControllerTest`, `LedgerControllerTest`, `OutboxControllerTest` pass |
| Dynamic Loan Validation | Tier unit tests | **Verified** | `DynamicLoanValidatorTest` passes |
| Event envelope contract | Jackson serialization tests | **Verified** | `EventEnvelopeTest` passes |
| Kafka trace header forwarding | ConsumerRecord header assertions | **Verified** | `AnomalyEventListenerTest` passes |
| Outbox resilience & isolation | Reactor concatMap error isolation | **Verified** | `OutboxProcessorServiceTest` passes |
| Architectural invariants | ArchUnit rules | **Verified** | Non-blocking rules pass (no Thread.sleep, no JDBC in reactive layers) |
| PostgreSQL / Kafka integration | Testcontainers | **Verified** | `IntegrationTests`, `E2EOutboxIntegrationTest` execute and pass with Docker |
| End-to-End Trace context flow | MockServerWebExchange & Mockito | **Verified** | `TraceFlowIntegrationTest` passes |
| Kind cluster deployment | OpenTofu / Terraform + Kind | **Verified** | All 12 pods (PostgreSQL, Kafka, Redis, Debezium, Tempo, Loki, Prometheus, Grafana, Schema Registry, Zookeeper, OTel Collector, Platform Engine) reach 1/1 Running |
| End-to-End Business Demo | Live curl execution (`scripts/demo.sh`) | **Verified** | Loans, normal tx, velocity bursts, outbox queueing, and SHA-256 ledger chaining verified against live cluster |

## Test Commands

Run the full Gradle test suite (29 tests across unit, contract, architecture, and integration layers):

```bash
cd apps/platform-engine
./gradlew test --no-daemon
```

Run focused fast tests (without Testcontainers):

```bash
./gradlew test --tests "*ControllerTest" --tests "*ServiceTest" --tests "*EventEnvelopeTest" --tests "*AnomalyEventListenerTest" --tests "*ArchitectureTest" --no-daemon
```

Execute live Kubernetes end-to-end verification:

```bash
./scripts/demo.sh
```

Full suite:

```powershell
./gradlew.bat test --no-daemon
```

The full suite must be run with Docker available before reporting Testcontainers or end-to-end completion.

## Failure Scenarios

The next integration batch should cover:

1. PostgreSQL unavailable during startup.
2. Kafka unavailable during outbox publication.
3. Outbox publish failure followed by retry.
4. Duplicate Kafka delivery.
5. Redis unavailable with in-memory anomaly fallback.
6. Ledger append conflict or invalid previous hash.
7. Missing or invalid Basic/JWT credentials.
8. UI/API request with an existing correlation ID.

## Definition of Test Completion

Testing is complete for a release when:

- Fast tests pass from a clean checkout.
- PostgreSQL and Kafka Testcontainers pass with Docker.
- The UI workflow passes against the local stack.
- Trace IDs are visible from HTTP through Kafka and ledger processing.
- Helm rendering and Kind readiness pass.
- Failure scenarios demonstrate recovery or a documented fail-safe result.
- Documentation reports environment-gated checks honestly.
