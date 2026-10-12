# Engineering Guide

This is the single source for developer setup, verification evidence, current gaps, and next implementation steps. The [README](../README.md) is the entry point; [architecture.md](architecture.md) describes the implemented system.

## Current Status

Last checked: 2026-10-11.

| Area | Current evidence |
|---|---|
| Java build and tests | CI run 38184674490 (commit `ae293c3`, 2026-10-11) passed the full suite with the no-skip report gate for the Docker-backed integration tests. Testcontainers is now 1.21.4; on a daemon whose minimum API is 1.40+, set `api.version=1.44` in `~/.docker-java.properties` (docker-java still defaults to 1.32). With that, the full local suite (69 tests, 0 skipped) passed on 2026-10-11 (Codespace, uncommitted changes). |
| Recovery fault tests | On 2026-10-11, focused CDC bridge, anomaly listener, and alert relay tests passed with injected Kafka-send and database-update failures. These are mocked component tests, not proof of Debezium restart or live broker outage recovery. |
| Debezium CDC | Optional CDC mode is live-verified in isolated Kind namespace `cdc-validation`: connector/task reached `RUNNING`; a committed outbox row reached the shadow topic and the bridge emitted the canonical `EventEnvelope`, after which the row was marked processed. CDC is the default mode (since 2026-10-12); the poller is an opt-in fallback. On 2026-10-11, the `Debezium CDC Drill (Kind)` CI job (run 38182187800, commit `d61fd1e`, `scripts/cdc-drill.sh`) passed on a clean Kind cluster in `cdc` mode: connector/task `RUNNING`, a row processed, a connector restart with tasks, `RUNNING` again, a second row processed, and exactly one canonical Kafka message per row. Kafka Connect pod loss, bridge crash between publish and `processed`, and a live Kafka outage under CDC remain unverified. | Update 2026-10-12 (CI run 38189122766, commit `08a74e1`): with the default chart (`cdc`), the drill also passed Connect pod deletion and platform-pod deletion, and measured 20 sequential samples at p50 69 ms, p95 105 ms, max 170 ms (`poll.interval.ms=50`; the default 500 ms gave p50 481 ms, p95 771 ms). The drill gates p95 at 500 ms.
| Fast behavior tests | Controller, service, anomaly, outbox, ledger verification, retention planning, JWT signature/claims/configuration, and architecture tests pass locally. |
| Outbox observability | Pending count/age gauges, publish/failure/retry counters, latency histogram, and payload-free recent-event summaries are implemented and covered by unit/controller tests. |
| Control room | Updated HTML, CSS, and JavaScript were served from the current local Spring process; `/api/v1/outbox/recent` returned HTTP 200 with Basic authentication. No browser automation or JavaScript runtime is installed in this container. |
| PostgreSQL/Kafka/Redis integration | CI run 38184674490 passed the health-checked service-backed tests and no-skip gate, including: the duplicate-delivery case that waits for the consumer-group offset to pass the resent record; a 20-way concurrent ledger append with chain verification and mutation rejection; and a retention case that drops an expired partition, deletes expired rows, keeps recent rows, and pre-creates a future partition. Local reruns skip these (see the first row). |
| Helm and Grafana | `helm lint` and `helm template` pass for `deploy/helm/event-driven-lab`, including the new outbox panels. Live dashboard queries have not been verified against a deployed current checkout. |
| Kind | CI run 38184674490 built/loaded `platform-engine:local`, installed the chart in `event-driven-lab`, passed readiness, and passed `scripts/postgres-durability-drill.sh`: a marker row survived deletion and replacement of the PVC-backed PostgreSQL pod. The same run's `Debezium CDC Drill (Kind)` job passed. Node loss, volume expansion, and backup/restore are not tested. |
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

For the UI and API, port-forward `svc/platform-engine` to local port 8080. Outside the `k8s` profile the `admin` / `local-dev-password` credentials are development fallbacks; the Helm deployment requires the secret key `ADMIN_PASSWORD` and `FallbackCredentialGuard` aborts startup if either the admin or database password is blank or the fallback. `scripts/demo.sh` is an assertion-based smoke test (health, loan, transactions, outbox drain, ledger growth and chain validity) run in CI; `scripts/demo.ps1` remains a request generator.

## Test Scope

### Always-on tests

These run without Docker:

- Controller and service contracts.
- Loan validation and anomaly scoring, including the control-room six-reading baseline followed by an outlier.
- Outbox envelope, failure isolation, retry delay, and event-listener mocked contract.
- Alert relay Kafka-send failure followed by retry-after-recovery (`AnomalyAlertOutboxRelayTest`); focused local run on 2026-10-09 passed all 3 tests, using a mocked Kafka broker.
- JWT validation: HMAC-SHA256 signature, HS256-only (including rejecting `none`), mandatory `exp`, `nbf`, 30 s configurable skew, optional `JWT_ISSUER`/`JWT_AUDIENCE`, `sub` as principal, and the required-secret profile guard.
- ArchUnit rules for `Thread.sleep` and JDBC dependencies in API/application packages.

### Docker-backed tests

Run with Docker available and inspect reports for skips, not only the Gradle exit code:

```bash
cd apps/platform-engine
./gradlew test --no-daemon
```

`IntegrationTests` performs `SELECT 1` through the configured R2DBC `DatabaseClient`. The E2E suite asserts loan persistence, its outbox event, Kafka envelope/key delivery, processed state, and a ledger entry; a second case asserts Redis-backed high-value anomaly detection, anomaly outbox publication, alert-topic delivery, and `traceparent` preservation. Both tests prefer explicit `SPRING_R2DBC_*` and `SPRING_FLYWAY_*` endpoints when provided, with runtime-mapped Testcontainers URLs as the local fallback; Redis uses the CI service or a local Testcontainers container. Both backend E2E cases passed in CI on `195c0fe`; they do not establish browser behavior, full trace visualization, or durable storage across pod replacement.

The anomaly E2E case also redelivers an event after its alert intent is durable and checks that only one row exists for that event ID. A local run on 2026-10-09 skipped both tests because docker-java defaulted to API 1.32 against a daemon minimum of 1.40; see the Java build status row for the workaround.

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

### 1. Activate Debezium CDC as the Sole Primary Publisher — Default as of 2026-10-12

- [x] Configure the Helm PostgreSQL workload for logical replication (`wal_level=logical`).
- [x] Add an idempotent Helm hook to create the dedicated replication role and insert-only outbox publication from externally managed secrets.
- [x] ~~Add a default-on `OUTBOX_POLLER_ENABLED` switch~~ — superseded by `OUTBOX_PUBLISHER_MODE` below; `OUTBOX_POLLER_ENABLED` no longer exists in code or the chart.
- [x] Configure Connect's environment-backed secret provider and Secret reference, REST service, and health probes; keep the connector version tagged.
- [x] Add a Helm-managed idempotent connector registration Job; it waits for connector and task state to become `RUNNING`. Isolated Kind registration and basic delivery were live-verified on 2026-10-09; connector restart was live-verified in CI on 2026-10-11. Connect pod loss and bridge-crash replay remain open.
- [x] Map committed outbox inserts from the raw Debezium shadow topic to the established Kafka topic, key, `EventEnvelope`, event ID, and trace metadata. The publication emits inserts only, excluding `processed`, lock, and retry updates.
- [x] Add a single `OUTBOX_PUBLISHER_MODE` (`cdc` by default since 2026-10-12, `poller` opt-in) so the scheduled relay and CDC bridge are mutually exclusive.
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
- [x] Run the live Debezium connector restart drill in an ephemeral Kind CI job (`cdc-drill`, `scripts/cdc-drill.sh`). Passed 2026-10-11, run 38182187800.
- [x] Extend the drill to Kafka Connect pod deletion and a bridge crash between canonical publish and the `processed` update. Verified 2026-10-12 (CI run 38186479608, commit `f2b2f6a`): a row written while the Connect pod is down is delivered exactly once after it returns (offsets live in Kafka topics), and a row followed by deletion of the platform pod ends `processed` with at least one canonical message (a replay may duplicate it, per the at-least-once contract). The bridge kill is not timed to the narrow publish-to-update window, so that window remains covered by the mocked `OutboxCdcBridgeTest` rather than a deterministic live fault.
- [ ] Exercise connector restart, consumer crash between intent commit and offset commit, Kafka outage, and recovery. The relay may duplicate the alert if it crashes after Kafka acknowledgement but before marking the intent published; downstream alert consumers must deduplicate by `eventId`.
- [ ] Measure bounded backlog/lag/recovery targets and define DLT inspection/replay operations.
- [ ] Measure high-volume bursts and sustained throughput; specify bounded backlog, consumer lag, and recovery-time targets before claiming surge tolerance.

**Exit gate:** Duplicate receipt intent is deduplicated and alert delivery is retried; complete the crash/outage drills and downstream idempotency before claiming replay-safe business effects.

### 3. Prove Data Integrity and Operational Recovery — Planned

- [x] Add persistent PostgreSQL storage to the chart and test data survival across pod replacement. Verified 2026-10-11 (CI run 38184674490, commit `ae293c3`): a `postgres.persistence` PVC (default on, 5Gi, `Recreate` strategy, `PGDATA` subdirectory) and `scripts/postgres-durability-drill.sh`, run by the Kind integration job, which writes a marker row, deletes the pod, and asserts the row survives. Still a single-replica local lab database: no backups, HA, or storage-class tuning.
- [x] Monitor replication-slot WAL retention, outbox age/depth, publish failures and Kafka consumer lag with Prometheus alert rules (`outbox-cdc` group). Verified 2026-10-12 (run 38189122766): the drill asserts the rules load without error and the gauges (`outbox_pending`, `outbox_oldest_pending_age_seconds`, `replication_slot_*`, `outbox_publish_success_total`) are scraped. Not yet verified: alerts actually firing (no fault-injection test) and Connect connector/task state, which Prometheus does not scrape (Connect exposes no metrics here without a JMX exporter). Consumer lag alerts rely on Kafka client metrics that are unverified.
- [x] Retention defined together (CI run 38195405082): telemetry, processed outbox and published alert-outbox rows 7 days; Kafka canonical, DLT and CDC shadow topics 7 days; WAL growth guarded by the `ReplicationSlotWalRetentionHigh` alert. Unprocessed or unpublished rows and ledger rows are never purged. Residual: the straddling partition uses a bounded `DELETE`.
- [x] Add ledger verification, database append-only enforcement, and serialized/concurrent append tests. Implemented and CI-verified on 2026-10-11 (run 38183456784, commit `c9a4bfb`, no-skip gate passed): `LedgerService.appendToLedger` takes a PostgreSQL transaction-scoped advisory lock before reading the head, so appends serialize across replicas; migration `V5` adds a unique `seq` identity column for deterministic head ordering and triggers rejecting `UPDATE`/`DELETE`/`TRUNCATE`; `GET /api/v1/ledger/verify` recomputes the whole chain and reports the first invalid event. Covered by `LedgerServiceVerificationTest` (mocked) and a 20-way concurrent append plus mutation-rejection case in `E2EOutboxIntegrationTest`. Open: verification reads the whole table (needs checkpoints or pagination at scale), and a retention policy for ledger rows must account for the append-only trigger.
- [x] Harden `DataComplianceService`. Verified 2026-10-11 (CI run 38184674490, commit `ae293c3`): the daily job is a chained reactive `Mono` returned to the scheduler (stages isolated so one failure does not skip the others); it drops every monthly partition wholly older than the cutoff (strict name validation), row-deletes expired rows left in the straddling and default partitions, and pre-creates the current plus `partitions-ahead` (default 3) monthly partitions, replacing the fixed list that ended at 2027-06. Covered by `DataComplianceServiceTest` and a DB-backed retention case in `E2EOutboxIntegrationTest`. Residual: with the 7-day default, expired rows inside the current monthly partition are removed by a bounded `DELETE`, not a cold drop, which conflicts with the cold-delete rule in `.agents/AGENTS.md`; weekly or daily partitions would remove that trade-off. The job has no multi-replica leader election.
- [x] Complete JWT algorithm/claims/expiry validation and tests. Implemented 2026-10-11 and covered by `SecurityConfigTest` (CI run 38185545443, commit `e7b1ff1`, all 4 jobs green): HS256 only, mandatory numeric `exp`, `nbf`, 30 s skew, optional issuer/audience, `sub` principal. Bearer tokens deliberately grant no authorities, so edge-node tokens cannot reach `ADMIN` endpoints. `BearerAuthenticationHttpTest` covers the HTTP filter (valid, expired, wrong secret, missing, and no ADMIN access). Open: no key rotation or `kid` support, and no token-issuing component (tokens are minted out of band).
- [x] Test Redis outage and fallback explicitly. Covered by `StreamingAnomalyDetectorRedisOutageTest` (unit-level, mocked Redis whose every call fails): velocity spikes, velocity bursts, and telemetry outliers are still detected through the in-memory window. Not covered: a live Redis pod outage and recovery. The fallback window is per replica, so during an outage detection is per replica, not global.

**Exit gate:** Failure drills demonstrate bounded backlog, observable recovery, durable source data, deterministic ledger verification, and no retention of expired data beyond policy.

### 4. Make the Workflow Observable and Usable — Planned

- [x] Single trace verified in Tempo (CI run 38195405082): `http post /api/v1/transactions`, `ledger.append`, `outbox.cdc.publish` and `outbox.consume` share one trace id, asserted by `scripts/cdc-drill.sh`.
- [ ] Validate Grafana queries against live metrics; add connector/replication lag, consumer lag, event latency, failure, and recovery panels only for emitted signals.
- [ ] Make the control room a workflow timeline keyed by event ID/correlation ID: accepted, persisted, published, anomaly/alert, ledger, retries, and trace link. Show pending/failure/loading/empty states without exposing sensitive payloads.
- [ ] Remove the duplicate root/static UI assets or establish one tested canonical resource source; add browser tests for the loan and anomaly workflows.
- [x] Turn `scripts/demo.sh` into an assertion-based smoke test for the same visible outcomes, not just a request generator. Verified by CI run 38199559941; the same run scrapes `kafka_consumer_fetch_manager_records_lag_max` in the drill.

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
## Load baseline

`scripts/load-test.sh` (CI run 38195405082, 400 requests at concurrency 20) reconciles database rows, outbox rows and Kafka messages by `eventId` and fails on lost events, p95 above budget, or errors. Local Kind measurements on a 1-CPU pod: Bearer JWT about 110 req/s with p95 about 530 ms at 2000 requests (concurrency 50), zero loss and zero duplicates; HTTP Basic about 9 req/s because of BCrypt, so Basic is for local/admin use only. With `KILL_POD=1` a single replica refuses requests during restart (444 of 1500 refused) but accepted requests were never lost or duplicated. The R2DBC pool is fixed at 10 connections.
