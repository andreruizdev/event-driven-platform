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

| Capability | Evidence | Status |
|---|---|---|
| Java production compilation | Gradle Java 21 compile | Passing |
| Java test compilation | Gradle test compile | Passing |
| Controller contracts | WebFlux slice tests | Passing for focused read APIs |
| Event envelope contract | `EventEnvelopeTest` | Passing |
| Kafka trace header forwarding | `AnomalyEventListenerTest` | Passing |
| PostgreSQL integration | Testcontainers | Docker-gated |
| Kafka integration | Testcontainers | Docker-gated |
| Full trace in Tempo | Runtime integration | Not yet proven |
| Kind deployment | Helm/Kind runtime | Not yet proven |
| UI browser workflow | Runtime browser check | Not yet proven |

## Test Commands

Fast compile:

```powershell
Set-Location apps/platform-engine
./gradlew.bat clean compileJava compileTestJava --no-daemon
```

Focused contract tests:

```powershell
./gradlew.bat test --tests "*EventEnvelopeTest" --tests "*AnomalyEventListenerTest" --no-daemon
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
