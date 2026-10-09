# Engineering Guide

This is the single source for developer setup, verification evidence, current gaps, and next implementation steps. The [README](../README.md) is the entry point; [architecture.md](architecture.md) describes the implemented system.

## Current Status

Last checked: 2026-10-09.

| Area | Current evidence |
|---|---|
| Java build and tests | `./gradlew test --no-daemon` succeeds: 37 tests, 34 passed, 3 skipped, 0 failures. |
| Fast behavior tests | Controller, service, anomaly, outbox, JWT signature/configuration, and architecture tests pass. |
| Outbox observability | Pending count/age gauges, publish/failure/retry counters, latency histogram, and payload-free recent-event summaries are implemented and covered by unit/controller tests. |
| Control room | Updated HTML, CSS, and JavaScript were served from the current local Spring process; `/api/v1/outbox/recent` returned HTTP 200 with Basic authentication. No browser automation or JavaScript runtime is installed in this container. |
| PostgreSQL/Kafka integration | Not established by the latest local run. `IntegrationTests` and both `E2EOutboxIntegrationTest` cases skipped because Docker-backed tests were unavailable. CI now provides health-checked PostgreSQL and Redis services, prioritizes their connection settings, and fails if either integration report is missing or skipped; a GitHub Actions run is still required to verify the path. |
| Helm and Grafana | `helm lint` and `helm template` pass for `deploy/helm/event-driven-lab`, including the new outbox panels. Live dashboard queries have not been verified against a deployed current checkout. |
| Kind | Twelve application-stack pods were Ready in the existing cluster, but they predated the current checkout; no fresh deployment was verified. |
| Full business workflow | Not verified. `scripts/demo.sh` submits requests and prints responses but does not assert Kafka delivery, anomaly results, ledger integrity, or trace visibility. |

Do not promote skipped, mocked, or stale-cluster checks to “verified” status. Update this table only from a dated executable result.

## Developer Setup

Use Java 21 with the checked-in Gradle wrapper. Docker or a compatible runtime is required for Testcontainers and local Kind provisioning. Helm, kubectl, Kind, and Terraform are required for the local cluster workflow.

Run fast tests and chart checks from the repository root:

```bash
cd apps/platform-engine
./gradlew test --no-daemon
cd ../..
helm lint deploy/helm/event-driven-lab
helm template event-driven-lab deploy/helm/event-driven-lab >/dev/null
```

The Windows wrapper is `apps/platform-engine/gradlew.bat`. The full local orchestration entry point is `scripts/start-all.sh` or `scripts/start-all.ps1`; it builds an image, applies local Terraform, manages a Kubernetes secret, and installs the chart. Use it only against the intended local Kind context.

The local Terraform cluster and CI Kind cluster are both named `eventdrivenmicroservices-cluster`. CI builds and loads `platform-engine:local`, then installs chart `event-driven-lab` as release `event-driven-lab` in namespace `event-driven-lab`; the Helm image repository/tag overrides must remain aligned with that built image.

For the UI and API, port-forward `svc/platform-engine` to local port 8080. The default `admin` / `local-dev-password` credentials are development fallbacks only; never use them in a shared or production environment. `scripts/demo.sh` and `scripts/demo.ps1` are request generators, not end-to-end assertion suites.

## Test Scope

### Always-on tests

These run without Docker:

- Controller and service contracts.
- Loan validation and anomaly scoring, including the control-room six-reading baseline followed by an outlier.
- Outbox envelope, failure isolation, retry delay, and event-listener mocked contract.
- JWT HMAC signature comparison and required-secret profile guard.
- ArchUnit rules for `Thread.sleep` and JDBC dependencies in API/application packages.

### Docker-backed tests

Run with Docker available and inspect reports for skips, not only the Gradle exit code:

```bash
cd apps/platform-engine
./gradlew test --no-daemon
```

`IntegrationTests` performs `SELECT 1` through the configured R2DBC `DatabaseClient`. The E2E suite asserts loan persistence, its outbox event, Kafka envelope/key delivery, processed state, and a ledger entry; a second case asserts Redis-backed high-value anomaly detection, anomaly outbox publication, alert-topic delivery, and `traceparent` preservation. Both tests prefer explicit `SPRING_R2DBC_*` and `SPRING_FLYWAY_*` endpoints when provided, with runtime-mapped Testcontainers URLs as the local fallback; Redis uses the CI service or a local Testcontainers container. CI fails if either integration report is missing or skipped. These E2E assertions compile but remain unverified at runtime until a Docker-backed GitHub Actions run succeeds.

### Deployment and workflow evidence

Helm lint/render checks establish template validity, not pod health. A fresh deployment should verify readiness, persistent data across a PostgreSQL pod restart, secret wiring, and the current application image. A workflow test should assert request result, anomaly, outbox processed state, Kafka alert, ledger hash verification, and trace evidence.

## Prioritized Work

1. **Prove anomaly behavior on both state backends.** The in-memory detector now scores against prior samples, and the control-room regression passes. Add Redis-backed tests for warmup, eviction, concurrent readings, and fallback behavior; assert that the documented UI sequence flags its outlier.
2. **Prove the event path under failure.** Outbox metrics, a payload-free recent-event endpoint, and Grafana panels are now implemented. Keep the scheduled application relay as the current primary path unless the project explicitly switches to Debezium. The Helm chart contains a Debezium Connect deployment/configuration, but the connector is not registered by the current scripts. Add PostgreSQL/Kafka integration coverage for atomic writes, retry/backoff, duplicate delivery, alert-send failure, and processed-state transitions. Parse the event envelope rather than routing by arbitrary serialized-text matches.
3. **Close trust and data-lifecycle gaps.** JWT HMAC comparison is constant-time and `k8s`/`prod` now fail startup without a configured key; implement and test JWT algorithm/claim/expiry validation. Add PostgreSQL persistence to the local chart, ledger integrity verification and append-only enforcement, and integration tests for concurrent appends and telemetry/outbox retention boundaries.
4. **Verify observability end to end.** Prove one trace from HTTP through outbox, Kafka, anomaly, and ledger in Tempo. Validate metric names and dashboard queries against emitted metrics; current instrumentation/configuration is not live evidence.
5. **Refresh the local deployment and CI evidence.** Deploy the current checkout to Kind, assert workload readiness and health, and make CI fail when Docker-backed tests are skipped unexpectedly or when the demo assertions fail.

External payment-provider simulation and service extraction are optional later work. Do not extract services until the modular monolith has a repeatable, tested business workflow and stable contracts.

## Completion Standard

Call a behavior verified only when its focused automated test or repeatable operational check exercises the real owning boundary. Record the command, date, result, environment requirements, and skips. A unit test can verify local logic; it cannot establish PostgreSQL, Kafka, Kubernetes, or Tempo behavior.