# Engineering Guide

This is the single source for developer setup, verification evidence, current gaps, and next implementation steps. The [README](../README.md) is the entry point; [architecture.md](architecture.md) describes the implemented system.

## Current Status

Last checked: 2026-10-11.

| Area | Current evidence |
|---|---|
| Java build and tests | Latest recorded CI run `195c0fe` passed all 37 tests; later commits have no recorded CI result. Local full suite on 2026-10-10 passed 43 of 46 tests; 3 Docker-backed integration tests were skipped (Testcontainers 1.19.7 negotiates Docker API 1.32, below the local daemon minimum of 1.40), with 0 failures/errors. The anomaly-alert replay E2E has therefore not run locally. |
| Recovery fault tests | On 2026-10-11, focused CDC bridge, anomaly listener, and alert relay tests passed with injected Kafka-send and database-update failures. These are mocked component tests, not proof of Debezium restart or live broker outage recovery. |
| Debezium CDC | Optional CDC mode is live-verified in isolated Kind namespace `cdc-validation`: connector/task reached `RUNNING`; a committed outbox row reached the shadow topic and the bridge emitted the canonical `EventEnvelope`, after which the row was marked processed. Poller remains the default mode. Connector restart/replay and CDC consumer crash recovery remain unverified. |
| Fast behavior tests | Controller, service, anomaly, outbox, JWT signature/configuration, and architecture tests pass. |
| Outbox observability | Pending count/age gauges, publish/failure/retry counters, latency histogram, and payload-free recent-event summaries are implemented and covered by unit/controller tests. |
| Control room | Updated HTML, CSS, and JavaScript were served from the current local Spring process; `/api/v1/outbox/recent` returned HTTP 200 with Basic authentication. No browser automation or JavaScript runtime is installed in this container. |
| PostgreSQL/Kafka/Redis integration | Latest CI run for `195c0fe` passed the health-checked service-backed tests and no-skip report gate. A local rerun on 2026-10-11 reported both `E2EOutboxIntegrationTest` cases skipped despite the Docker CLI being reachable; duplicate-consumer E2E remains unverified locally. |
| Helm and Grafana | `helm lint` and `helm template` pass for `deploy/helm/event-driven-lab`, including the new outbox panels. Live dashboard queries have not been verified against a deployed current checkout. |
| Kind | Latest CI run for `195c0fe` built/loaded `platform-engine:local`, installed the chart in `event-driven-lab`, and passed the readiness job. PostgreSQL persistence across pod restart remains unverified. |
| E2E business slices | CI verifies loan -> PostgreSQL/outbox -> Kafka -> ledger and Redis-backed anomaly -> alert topic with `traceparent`. Browser-driven UI behavior, full Tempo trace visualization, and the telemetry UI workflow remain unverified. |
| CDC runtime check | On 2026-10-09, an isolated chart release registered the Debezium connector and verified a synthetic PostgreSQL insert in `eventdrivenmicroservices-outbox-events` as version-1 envelope with key/event ID/traceparent; database `processed=true` followed successful publication. This does not verify restart/replay or production cutover. |
| Anomaly consumer durability | On 2026-10-09, isolated Kind verified anomaly envelope -> event-ID-keyed PostgreSQL alert intent -> alert topic; the row was marked published after Kafka acknowledgement. Replaying the same canonical event ID left one published intent. Broker crash-window duplicates and poison-event policy remain open. |
| Poison event handling | On 2026-10-09, isolated Kind verified malformed canonical input reaches the seven-day DLT with source topic/partition/offset metadata. Parser headers use a fixed payload-free message. Transient listener errors use an indefinite one-second retry policy; live outage recovery is not yet verified. |

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
- Alert relay Kafka-send failure followed by retry-after-recovery (`AnomalyAlertOutboxRelayTest`); focused local run on 2026-10-09 passed all 3 tests, using a mocked Kafka broker.
- JWT HMAC signature comparison and required-secret profile guard.
- ArchUnit rules for `Thread.sleep` and JDBC dependencies in API/application packages.

### Docker-backed tests

Run with Docker available and inspect reports for skips, not only the Gradle exit code:

```bash
cd apps/platform-engine
./gradlew test --no-daemon
```

`IntegrationTests` performs `SELECT 1` through the configured R2DBC `DatabaseClient`. The E2E suite asserts loan persistence, its outbox event, Kafka envelope/key delivery, processed state, and a ledger entry; a second case asserts Redis-backed high-value anomaly detection, anomaly outbox publication, alert-topic delivery, and `traceparent` preservation. Both tests prefer explicit `SPRING_R2DBC_*` and `SPRING_FLYWAY_*` endpoints when provided, with runtime-mapped Testcontainers URLs as the local fallback; Redis uses the CI service or a local Testcontainers container. Both backend E2E cases passed in CI on `195c0fe`; they do not establish browser behavior, full trace visualization, or durable storage across pod replacement.

The anomaly E2E case also redelivers an event after its alert intent is durable and checks that only one row exists for that event ID. The focused local run on 2026-10-09 skipped both Docker-backed tests because Testcontainers' Docker client used API 1.32 while the local daemon requires at least API 1.40; this assertion is not verification evidence.

### Deployment and workflow evidence

Helm lint/render checks establish template validity, not pod health. A fresh deployment should verify readiness, persistent data across a PostgreSQL pod restart, secret wiring, and the current application image. A workflow test should assert request result, anomaly, outbox processed state, Kafka alert, ledger hash verification, and trace evidence.

## Stability-to-Microservices Plan

This is the ordered implementation plan. Complete each exit gate before advancing; record the CI run, test report, or operational result in **Current Status**. `195c0fe` is the latest baseline currently recorded as passing CI, including Testcontainers and Kind readiness.

### 0. Preserve the Verified Baseline — Complete

- [x] Reactive loan/transaction/telemetry ingestion and transactional outbox writes.
- [x] Scheduled relay with leases, retry/backoff, outbox metrics, and Kafka alert path.
- [x] CI PostgreSQL/Redis services, no-skip integration report gate, and successful Kind deployment/readiness.
- [x] E2E CI coverage for loan -> PostgreSQL/outbox -> Kafka -> ledger and Redis anomaly -> alert topic with `traceparent`.

**Exit evidence:** CI run `195c0fe` passed security, Java/Testcontainers, and Kind jobs. This verifies the current slices, not production availability or exactly-once delivery.

### 1. Activate Debezium CDC as the Sole Primary Publisher — In Progress, Highest Priority

- [x] Configure the Helm PostgreSQL workload for logical replication (`wal_level=logical`).
- [x] Add an idempotent Helm hook to create the dedicated replication role and insert-only outbox publication from externally managed secrets.
- [x] ~~Add a default-on `OUTBOX_POLLER_ENABLED` switch~~ — superseded by `OUTBOX_PUBLISHER_MODE` below; `OUTBOX_POLLER_ENABLED` no longer exists in code or the chart.
- [x] Configure Connect's environment-backed secret provider and Secret reference, REST service, and health probes; keep the connector version tagged.
- [x] Add a Helm-managed idempotent connector registration Job; it waits for connector and task state to become `RUNNING`. Isolated Kind registration and basic delivery were live-verified on 2026-10-09; restart/replay remains open.
- [x] Map committed outbox inserts from the raw Debezium shadow topic to the established Kafka topic, key, `EventEnvelope`, event ID, and trace metadata. The publication emits inserts only, excluding `processed`, lock, and retry updates.
- [x] Add a single `OUTBOX_PUBLISHER_MODE` (`poller` by default, `cdc` opt-in) so the scheduled relay and CDC bridge are mutually exclusive.
- [x] Define `processed` in CDC mode as successfully published to the canonical Kafka topic, not consumer completion. A bridge crash after Kafka acknowledgement but before the database update can republish on replay; consumer idempotency is still required.

**Exit gate:** Basic CDC-to-envelope delivery is verified in isolated Kind. Remaining gate: prove connector recreation/restart from stored offsets and duplicate/replay recovery with idempotent consumer effects before making CDC the deployment default.

### 2. Make Consumers Replay-Safe and Failure-Tolerant — In Progress

- [x] Parse the versioned envelope and route anomaly handling by exact `eventType`; serialized payload contents no longer control routing.
- [x] Persist alert intents in PostgreSQL with a unique `eventId` before acknowledging the input record; retry alert-topic publication independently with bounded backoff.
- [x] Verify duplicate input replay creates one alert intent in isolated Kind.
- [x] Route malformed/unsupported envelopes to an explicitly provisioned seven-day DLT with original Kafka coordinates; parser diagnostics do not copy rejected payload data into headers.
- [x] Configure indefinite one-second retries for transient listener failures; live outage recovery remains unverified.
- [x] Add focused fault-injection tests for CDC Kafka outage recovery and publish-acknowledged/database-mark failure; replay uses the same event ID and envelope. Mocked Kafka/repository only; passed locally on 2026-10-11.
- [x] Add listener redelivery-after-intent and alert-relay publish-acknowledged/database-mark failure tests. Unit tests passed locally on 2026-10-11; they do not prove actual process restart or downstream alert deduplication.
- [x] Extend the PostgreSQL/Kafka E2E to send a duplicate anomaly event through the real listener topic and wait for the consumer-group offset to advance before asserting one alert intent. A local 2026-10-11 run skipped both E2E cases; CI execution is still required.
- [x] Run the PostgreSQL/Kafka duplicate-delivery E2E without skips and verify the new group-offset assertion. Verified 2026-10-11 by CI run 38181492686 (commit cbcb72b), whose no-skip report gate passed.
- [ ] Run the live Debezium connector restart/replay drill in an ephemeral Kind CI job.
- [ ] Exercise connector restart, consumer crash between intent commit and offset commit, Kafka outage, and recovery. The relay may duplicate the alert if it crashes after Kafka acknowledgement but before marking the intent published; downstream alert consumers must deduplicate by `eventId`.
- [ ] Measure bounded backlog/lag/recovery targets and define DLT inspection/replay operations.
- [ ] Measure high-volume bursts and sustained throughput; specify bounded backlog, consumer lag, and recovery-time targets before claiming surge tolerance.

**Exit gate:** Duplicate receipt intent is deduplicated and alert delivery is retried; complete the crash/outage drills and downstream idempotency before claiming replay-safe business effects.

### 3. Prove Data Integrity and Operational Recovery — Planned

- [ ] Add persistent PostgreSQL storage to the chart and test data survival across pod replacement.
- [ ] Monitor Debezium connector/task state, replication-slot WAL retention, Kafka consumer lag, outbox age/depth, retry rate, and end-to-end event latency; alert on stalled progress and WAL growth.
- [ ] Define WAL, Kafka, outbox, and telemetry retention together. Never purge unconsumed/unconfirmed events; prove expired-data boundaries while preserving recoverability.
- [ ] Add ledger verification, database append-only enforcement, and serialized/concurrent append tests. Audit: `LedgerService` reads the head via `ORDER BY created_at DESC LIMIT 1` without a lock or conflict retry, so concurrent appends can collide on the unique `previous_hash` index and timestamp ties make the head ambiguous; only `GET /api/v1/ledger` and `/latest-hash` exist.
- [ ] Harden `DataComplianceService`: the purge runs fire-and-forget (`.subscribe()`), drops only one monthly partition computed from the cutoff, and partitions exist only through 2027-06 with no creation job. Make it a chained reactive job, add partition pre-creation, and reconcile the 7-day retention default with monthly partitions.
- [ ] Complete JWT algorithm/claims/expiry validation, role-to-authority mapping, and authorization tests. Audit: the filter checks only the HMAC signature and always grants the same principal with no authorities.
- [ ] Test Redis outage and fallback explicitly. Redis-backed anomaly decisions are shared state; the in-memory fallback is per process and can diverge across replicas.

**Exit gate:** Failure drills demonstrate bounded backlog, observable recovery, durable source data, deterministic ledger verification, and no retention of expired data beyond policy.

### 4. Make the Workflow Observable and Usable — Planned

- [ ] Verify a single trace from HTTP through database/outbox, Debezium, Kafka, anomaly, ledger, and alert in Tempo.
- [ ] Validate Grafana queries against live metrics; add connector/replication lag, consumer lag, event latency, failure, and recovery panels only for emitted signals.
- [ ] Make the control room a workflow timeline keyed by event ID/correlation ID: accepted, persisted, published, anomaly/alert, ledger, retries, and trace link. Show pending/failure/loading/empty states without exposing sensitive payloads.
- [ ] Remove the duplicate root/static UI assets or establish one tested canonical resource source; add browser tests for the loan and anomaly workflows.
- [ ] Turn `scripts/demo.sh` into an assertion-based smoke test for the same visible outcomes, not just a request generator.

**Exit gate:** A clean Kind deployment can run the demo and let an operator follow the same event through UI evidence, Kafka/ledger state, metrics, and Tempo.

### 5. Extract Business Microservices — Not Before Gates 1–4

- [ ] Stabilize versioned event contracts and document ownership for loans/transactions, outbox delivery, anomaly/alerts, and ledger.
- [ ] Extract one bounded context at a time, beginning with the outbox relay/CDC runtime or anomaly consumer; give each service an independent build, deployment, data boundary, security policy, and telemetry identity.
- [ ] Add contract tests, migrations, idempotency, health/readiness, resource limits, failure tests, and independent CI for each extracted service.
- [ ] Retain a working end-to-end workflow after every extraction; do not split solely to increase service count.

**Exit gate:** Each service deploys and recovers independently, owns a documented contract/data boundary, and the full business workflow remains green.

External-provider simulation is optional learning work; it does not precede CDC correctness, idempotency, durability, or recovery. Do not claim exactly-once transport. The reliability target is at-least-once delivery with idempotent effects and demonstrable recovery.

## Completion Standard

Call a behavior verified only when its focused automated test or repeatable operational check exercises the real owning boundary. Record the command, date, result, environment requirements, and skips. A unit test can verify local logic; it cannot establish PostgreSQL, Kafka, Kubernetes, or Tempo behavior.