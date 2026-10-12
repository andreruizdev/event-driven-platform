# Engineering Guide

This is the single source for developer setup, verification evidence, current gaps, and next implementation steps. The [README](../README.md) is the entry point; [architecture.md](architecture.md) describes the implemented system.

## Current Status

Last checked: 2026-10-12.

| Area | Current evidence |
|---|---|
| Java build and tests | CI run 38206550351 (commit `5383987`, 2026-10-12) passed the full suite with the no-skip report gate, including the Playwright browser workflow and PostgreSQL migration/retention tests. Testcontainers is 1.21.4; on a daemon whose minimum API is 1.40+, set `api.version=1.44` in `~/.docker-java.properties` (docker-java still defaults to 1.32). The full local `./gradlew test` suite also passed on 2026-10-12 with Docker-backed tests, 0 skipped. |
| Recovery fault tests | Mocked CDC bridge, anomaly listener, and alert relay failure tests pass. On 2026-10-12, the Docker-backed CDC replay E2E injected a PostgreSQL trigger failure on the `processed` update after a real Kafka acknowledgement, replayed the same CDC row through the bridge, and verified two identical canonical records and one durable alert intent. A second Docker-backed E2E held the real Kafka listener after its PostgreSQL intent commit, verified the source offset had not advanced, injected a listener failure, then verified redelivery, offset advancement, and one durable intent. Both passed with 0 skips. These tests exercise live PostgreSQL/Kafka acknowledgment/retry boundaries, but not abrupt process restart or Debezium source-offset replay. |
| Debezium CDC | CDC has been the Helm default since 2026-10-12; the poller is opt-in. CI runs 38182187800 and 38186479608 exercised connector restart, Connect pod loss with an outbox row written during the outage, and platform-pod loss; the row was processed after recovery. The connector-state alert was separately exercised firing and clearing in CI run 38201832047. Run 38189122766 measured 20 sequential samples at p50 69 ms, p95 105 ms, max 170 ms with `poll.interval.ms=50`; the drill's p95 budget is 500 ms. Deterministic bridge-level replay after the publish/processed-update failure is now covered locally with Docker-backed PostgreSQL and Kafka; production-style Debezium offset replay and live Kafka outage recovery remain unverified. |
| Fast behavior tests | Controller, service, anomaly, outbox, ledger verification, retention planning, JWT signature/claims/configuration, and architecture tests pass locally. |
| Outbox observability | Pending count/age gauges, publish/failure/retry counters, Kafka-ack latency histogram, payload-free recent-event summaries, and sanitized workflow timeline are implemented. Controller tests verify status handling and exclude payload/error text. |
| Control room | The Playwright/Chromium E2E now also opens a loan's event timeline and asserts the applicant ID is absent; this and the V7 migration passed locally and in CI run 38213179485 (commit `f117420`, 2026-10-12, all jobs green). The timeline supports event/correlation lookup, pending/failure/loading/empty states, and a Grafana trace link. The UI renders ledger head/verification only, not ledger payloads. `browserTimelineShowsValidationEmptyLoadingAndErrorStates` covers validation error, empty, loading (aria-busy) and server-error states without leaking the error body; it passed locally (0 skips) and also caught a UI bug where the background evidence refresh overwrote an opened timeline (fixed with `state.timelineActive`). CI confirmation is recorded below when available. |
| PostgreSQL/Kafka/Redis integration | CI run 38206550351 (commit `5383987`, 2026-10-12) passed the full build/test and no-skip gate, including duplicate-delivery and concurrent ledger cases, and runs V5-to-V6 migration/retention checks. The V7 migration adding correlation IDs and ledger-to-outbox links applied successfully in the local Docker-backed E2E and migration tests and in CI run 38213179485. The E2E asserts that the correlation ID reaches the loan, outbox and ledger rows. |
| Helm and Grafana | `helm lint` and `helm template` pass for `deploy/helm/event-driven-lab`; dashboard panels cover emitted connector/task state, replication-slot state/retained WAL, Kafka consumer lag, outbox backlog, publish acknowledgement latency, failures, retries, and successful publishes. `scripts/cdc-drill.sh` queries all 16 dashboard PromQL expressions against live Prometheus and requires `status: success` (CI run 38213179485, all jobs green). Separately, a headless-Chromium test renders all 13 panels with live data and no query errors (CI run 38217831739, 2026-10-12). Firing/clearing of remaining alerts is outstanding. |
| Kind | CI run 38184674490 built/loaded `platform-engine:local`, installed the chart in `event-driven-lab`, passed readiness, and passed `scripts/postgres-durability-drill.sh`: a marker row survived deletion and replacement of the PVC-backed PostgreSQL pod. Run 38204706720 passed `scripts/postgres-backup-verify.sh`: it exported a custom-format archive, restored it into a temporary database, and matched representative table counts. Run 38206550351 also passed the durability and backup/restore drills after V6. The archive is saved to an operator-selected path; this is not scheduled or off-site backup. Node loss, volume expansion, and independent-storage restore remain untested. |
| E2E business slices | CI verifies loan -> PostgreSQL/outbox -> Kafka -> ledger and Redis-backed anomaly -> alert topic with `traceparent`. Playwright verifies authenticated loan submission and an anomaly burst appear in the control-room evidence table. The CDC drill finds HTTP, ledger, CDC publish, and consumer spans in one Tempo trace. The new timeline browser assertion, Grafana visual navigation, and telemetry browser workflow remain unverified. |
| CDC runtime check | Kind drills verify connector restart and recovery after Connect pod loss, including processing an outbox row written while Connect is unavailable. The opt-in broker outage/recovery drill passed in CI run 38210129144 (commit `bce9b1f`) after moving pre-outage topic assertions ahead of the intentionally destructive single-broker restart. The Docker-backed trigger test verifies the bridge's publish/processed failure and replay boundary, but not production cutover or Debezium source-offset replay after process restart. |
| Anomaly consumer durability | CI's PostgreSQL/Kafka E2E redelivers an anomaly event and waits for consumer offset progress before asserting one alert intent for its event ID. Local Docker-backed E2E also holds the real listener after intent commit but before return, confirms the offset is uncommitted, injects a listener failure, and verifies retry creates one intent and advances the offset (0 skips). Alert intent persistence is idempotent; downstream alert consumers must still deduplicate because alert publication is at-least-once. Abrupt consumer process restart between intent commit and input offset commit and live Kafka outage recovery remain open. |
| Poison event handling | On 2026-10-09, isolated Kind verified malformed canonical input reaches the seven-day DLT with source topic/partition/offset metadata. Parser headers use a fixed payload-free message. Transient listener errors use an indefinite one-second retry policy; live outage recovery is not yet verified. |

Do not promote skipped, mocked, or stale-cluster checks to “verified” status. Update this table only from a dated executable result.

## Developer Setup

Use Java 21 with the checked-in Gradle wrapper. Docker or a compatible runtime is required for Testcontainers and local Kind provisioning. Helm, kubectl, Kind, and Terraform are required for the local cluster workflow.

Run tests and chart checks from the repository root:

```bash
cd apps/platform-engine
./gradlew test --no-daemon
cd ../..
helm lint deploy/helm/event-driven-lab
helm template event-driven-lab deploy/helm/event-driven-lab >/dev/null
```

The Windows wrapper is `apps/platform-engine/gradlew.bat`. To run the browser E2E, install its pinned browser first with `cd apps/platform-engine && ./gradlew installPlaywrightChromium`. The full local orchestration entry point is `scripts/start-all.sh` or `scripts/start-all.ps1`; it builds an image, applies local Terraform, manages a Kubernetes secret, and installs the chart. Use it only against the intended local Kind context.

The local Terraform cluster and CI Kind cluster are both named `eventdrivenmicroservices-cluster`. CI builds and loads `platform-engine:local`, then installs chart `event-driven-lab` as release `event-driven-lab` in namespace `event-driven-lab`; the Helm image repository/tag overrides must remain aligned with that built image.

For the UI and API, port-forward `svc/platform-engine` to local port 8080. Outside the `k8s` profile the `admin` / `local-dev-password` credentials are development fallbacks; the Helm deployment requires the secret key `ADMIN_PASSWORD` and `FallbackCredentialGuard` aborts startup if either the admin or database password is blank or the fallback. `scripts/demo.sh` is an assertion-based smoke test (health, loan, transactions, outbox drain, ledger growth and chain validity) run in CI; `scripts/demo.ps1` remains a request generator.

## Test Scope

### Always-on tests

These run without Docker:

- Controller and service contracts.
- Loan validation and anomaly scoring, including the control-room six-reading baseline followed by an outlier.
- Outbox envelope, failure isolation, retry delay, and event-listener mocked contract.
- Alert relay Kafka-send failure followed by retry-after-recovery (`AnomalyAlertOutboxRelayTest`); focused local run on 2026-10-09 passed all 3 tests, using a mocked Kafka broker.
- JWT validation: HMAC-SHA256 signature, HS256-only (including rejecting `none`), mandatory `exp`, `nbf`, 30 s configurable skew, optional `JWT_ISSUER`/`JWT_AUDIENCE`, `sub` as principal, and the required-secret profile guard. Rotation: `JWT_SECRET_KEY_PREVIOUS` secrets are tried after the current one (`SecurityConfigTest`).
- ArchUnit rules for `Thread.sleep` and JDBC dependencies in API/application packages.

### Docker-backed tests

Run with Docker available and inspect reports for skips, not only the Gradle exit code:

```bash
cd apps/platform-engine
./gradlew test --no-daemon
```

`IntegrationTests` performs `SELECT 1` through the configured R2DBC `DatabaseClient`. The E2E suite asserts loan persistence, outbox event, Kafka envelope/key delivery, processed state, ledger entry, Redis-backed anomaly detection, alert-topic delivery, `traceparent` preservation, duplicate-input idempotency, retention, and concurrent ledger integrity. A Playwright/Chromium case drives authenticated loan and transaction-anomaly workflows through the control room and checks the evidence table. These tests prefer explicit `SPRING_R2DBC_*` and `SPRING_FLYWAY_*` endpoints when provided, with runtime-mapped Testcontainers URLs as local fallback. They do not establish full Tempo UI navigation, every UI workflow, production availability, or independent-storage backup recovery.

The anomaly E2E case redelivers an event after its alert intent is durable, waits for consumer offset advancement, and checks that only one intent row exists for the event ID. CI run 38206550351 passed the no-skip integration-test gate. On compatible local Docker daemons whose API minimum is 1.40, use the `api.version=1.44` workaround above.

### Deployment and workflow evidence

Helm lint/render checks establish template validity, not pod health. CI's fresh Kind deployment verifies readiness, persistence across PostgreSQL pod replacement, manual backup/restore, CDC recovery, and the connector alert transition. The CDC-source Kafka outage/recovery drill passed in run 38210129144. Dashboard PromQL validation passed in run 38213179485. Redis outage recovery, DLT operations, Debezium source-offset replay, alert-topic outage, and consumer process restart between intent commit and offset commit remain unverified.

## Stability-to-Microservices Plan

This is the dependency-ordered implementation plan. Gates 1–4 track CDC delivery, consumer recovery, data integrity, and operator workflow; record dated executable evidence in **Current Status** and keep unproven behavior open. Latest full no-skip baseline: CI run `38206550351` (commit `5383987`), with the control-room browser workflow additionally passing CI run `38204706720`.

### 0. Preserve the Verified Baseline — Complete

- [x] Reactive loan/transaction/telemetry ingestion and transactional outbox writes.
- [x] Scheduled relay with leases, retry/backoff, outbox metrics, and Kafka alert path.
- [x] CI PostgreSQL/Redis services, no-skip integration report gate, and successful Kind deployment/readiness.
- [x] E2E CI coverage for loan -> PostgreSQL/outbox -> Kafka -> ledger and Redis anomaly -> alert topic with `traceparent`.

**Exit evidence:** CI run `38206550351` passed security, Java/Testcontainers, and Kind jobs. This verifies the implemented lab slices, not production availability or exactly-once delivery.

### 1. Activate Debezium CDC as the Sole Primary Publisher — Complete for the lab baseline

- [x] Configure the Helm PostgreSQL workload for logical replication (`wal_level=logical`).
- [x] Add an idempotent Helm hook to create the dedicated replication role and insert-only outbox publication from externally managed secrets.
- [x] ~~Add a default-on `OUTBOX_POLLER_ENABLED` switch~~ — superseded by `OUTBOX_PUBLISHER_MODE` below; `OUTBOX_POLLER_ENABLED` no longer exists in code or the chart.
- [x] Configure Connect's environment-backed secret provider and Secret reference, REST service, and health probes; keep the connector version tagged.
- [x] Add a Helm-managed idempotent connector registration Job; it waits for connector and task state to become `RUNNING`. Connector restart and Connect pod loss/recovery with a row written during the outage passed the Kind drills (runs 38182187800 and 38186479608).
- [x] Map committed outbox inserts from the raw Debezium shadow topic to the established Kafka topic, key, `EventEnvelope`, event ID, and trace metadata. The publication emits inserts only, excluding `processed`, lock, and retry updates.
- [x] Add a single `OUTBOX_PUBLISHER_MODE` (`cdc` by default since 2026-10-12, `poller` opt-in) so the scheduled relay and CDC bridge are mutually exclusive.
- [x] Define `processed` in CDC mode as successfully published to the canonical Kafka topic, not consumer completion. A bridge crash after Kafka acknowledgement but before the database update can republish on replay; consumer idempotency is still required.

**Exit evidence:** CI runs 38182187800 and 38186479608 verify CDC envelope delivery, connector restart, and recovery after Connect pod loss. CI run 38181492686 verifies replay of a canonical anomaly event produces one durable alert intent. Gate 1's deployment-default and basic recovery criteria are met for the lab; end-to-end Debezium offset replay and live Kafka outage recovery remain Gate 2 work, not claims of exactly-once delivery.

### 2. Make Consumers Replay-Safe and Failure-Tolerant — In Progress

- [x] Parse the versioned envelope and route anomaly handling by exact `eventType`; serialized payload contents no longer control routing.
- [x] Persist alert intents in PostgreSQL with a unique `eventId` before acknowledging the input record; retry alert-topic publication independently with bounded backoff.
- [x] Verify duplicate input replay creates one alert intent in isolated Kind.
- [x] Route malformed/unsupported envelopes to an explicitly provisioned seven-day DLT with original Kafka coordinates; parser diagnostics do not copy rejected payload data into headers.
- [x] Configure indefinite one-second retries for transient listener failures; live outage recovery remains unverified.
- [x] Add focused fault-injection tests for CDC Kafka outage recovery and publish-acknowledged/database-mark failure; replay uses the same event ID and envelope. Mocked Kafka/repository only; passed locally on 2026-10-11.
- [x] Add listener redelivery-after-intent and alert-relay publish-acknowledged/database-mark failure tests. Unit tests passed locally on 2026-10-11; they do not prove actual process restart or downstream alert deduplication.
- [x] Extend the PostgreSQL/Kafka E2E to send a duplicate anomaly event through the real listener topic, wait for consumer-group offset advancement, and assert one alert intent. Verified without skips in CI run 38181492686; the later full suite and no-skip gate also passed in run 38206550351.
- [x] Run the live Debezium connector restart drill in an ephemeral Kind CI job (`cdc-drill`, `scripts/cdc-drill.sh`). Passed 2026-10-11, run 38182187800.
- [x] Drill Connect pod loss and platform-pod recovery (CI run 38186479608). The platform pod is deleted after an outbox row is inserted; this is a process-recovery drill, not deterministic injection between Kafka acknowledgement and setting `processed`.
- [x] Deterministically fail the PostgreSQL `processed` update after canonical Kafka acknowledgement, then replay the same CDC row through the bridge. The 2026-10-12 Docker-backed E2E uses a PostgreSQL trigger for the targeted failure, verifies two identical records in live Kafka, successful processed-state persistence on replay, consumer offset advancement, and exactly one durable alert intent (0 skips). This validates the bridge replay boundary; it does not restart Debezium or prove connector source-offset recovery.
- [x] Inject a consumer-listener failure after the durable alert-intent commit and before the listener returns. On 2026-10-12, the Docker-backed E2E verified the group offset remained uncommitted while paused, then observed the production retry handler redeliver the record, advance the offset, and preserve exactly one intent (0 skips). This validates the ack boundary but is not an abrupt process restart.
- [x] Exercise live Kafka broker outage/recovery for the CDC source path in disposable Kind only. CI run 38210129144 (commit `bce9b1f`) proves a row written while Kafka is unavailable becomes processed and appears in the canonical topic after recovery. The drill intentionally loses broker-local data and is gated by `ALLOW_KAFKA_DATA_LOSS_DRILL=true`.
- [x] Alert-relay outage/recovery: the Docker-backed E2E pauses the Kafka container, verifies `retry_count`/`last_error` are recorded with no publish, unpauses, and verifies the alert is published and the intent marked delivered (0 skips; CI run 38215581158, commit `0189df2`).
- [ ] Terminate/restart the consumer process between intent commit and offset commit (abrupt kill is still unproven). Alert-relay duplicates after Kafka acknowledgement remain possible; downstream alert consumers must deduplicate by `eventId`.
- [x] DLT inspection/replay: `scripts/dlt-ops.sh inspect` lists partition/offset/timestamp/headers without payloads; `replay <partition> <offset>` needs `CONFIRM_REPLAY=true`, only targets the canonical topic and pipes the record inside the Kafka pod. The CDC drill verifies dead-lettering, payload-free inspection, refusal without confirmation and a confirmed replay (CI run 38215581158). Replay is at-least-once; fix the cause first or the record is dead-lettered again.
- [ ] Measure bounded backlog/lag/recovery targets.
- [ ] Measure high-volume bursts and sustained throughput; specify bounded backlog, consumer lag, and recovery-time targets before claiming surge tolerance.

**Exit gate:** Duplicate receipt intent is deduplicated and alert delivery is retried; complete the crash/outage drills and downstream idempotency before claiming replay-safe business effects.

### 3. Prove Data Integrity and Operational Recovery — In Progress

- [x] Add persistent PostgreSQL storage to the chart and test data survival across pod replacement. Verified 2026-10-11 (CI run 38184674490, commit `ae293c3`): a `postgres.persistence` PVC (default on, 5Gi, `Recreate` strategy, `PGDATA` subdirectory) and `scripts/postgres-durability-drill.sh`, run by the Kind integration job, which writes a marker row, deletes the pod, and asserts the row survives. A portable backup/restore drill passed in CI run 38204706720. Still a single-replica local lab database without scheduled/off-site backups, HA, or storage-class tuning.
- [x] Monitor replication-slot WAL retention, outbox age/depth, publish failures and Kafka consumer lag with Prometheus alert rules (`outbox-cdc` group). Verified 2026-10-12 (run 38189122766): the drill asserts the rules load without error and the gauges (`outbox_pending`, `outbox_oldest_pending_age_seconds`, `replication_slot_*`, `outbox_publish_success_total`) are scraped. Connect exposes no Prometheus metrics, so `ConnectorStateMonitor` polls the Connect REST status every 15 s and exposes `eventdrivenmicroservices.connector.running`, `.tasks.running` and `.tasks.failed`, with alerts `DebeziumConnectorNotRunning` and `DebeziumTaskFailed`. `scripts/cdc-drill.sh` now pauses the connector, waits for `DebeziumConnectorNotRunning` to fire, resumes it and waits for the alert to clear (passed in a live local Kind run on 2026-10-12; CI run 38201832047, commit `5e3d2e7`, all jobs green). Consumer lag (`kafka_consumer_fetch_manager_records_lag_max`) is scraped and asserted (run 38199559941). Other alert rules are only checked to load, not fired.
- [x] Retention defined together (CI run 38195405082): telemetry, processed outbox and published alert-outbox rows 7 days; Kafka canonical, DLT and CDC shadow topics 7 days; WAL growth guarded by the `ReplicationSlotWalRetentionHigh` alert. Unprocessed or unpublished rows and ledger rows are never purged. V6 converts telemetry to UTC-hour partitions; fully expired hours are dropped hourly, for a maximum ~1-hour overrun in the managed range. Expired out-of-range rows in the default partition remain an explicit row-delete fallback, warning-logged and covered by CI run 38206550351.
- [x] Add ledger verification, database append-only enforcement, and serialized/concurrent append tests. Implemented and CI-verified on 2026-10-11 (run 38183456784, commit `c9a4bfb`, no-skip gate passed): `LedgerService.appendToLedger` takes a PostgreSQL transaction-scoped advisory lock before reading the head, so appends serialize across replicas; migration `V5` adds a unique `seq` identity column for deterministic head ordering and triggers rejecting `UPDATE`/`DELETE`/`TRUNCATE`; `GET /api/v1/ledger/verify` recomputes the whole chain (O(1) memory, O(n) time) and reports the first invalid event; `?recent=N` (max 100000) verifies only the latest N events, anchored on the stored previous hash of the first one, so it cannot detect tampering before the window (the response carries `scope`: `FULL` or `RECENT:n`); `GET /api/v1/ledger` is bounded by `?limit=` (default 100, max 1000). Covered by `LedgerServiceVerificationTest` (mocked) and a 20-way concurrent append plus mutation-rejection case in `E2EOutboxIntegrationTest`. Open: the default full verification still reads the whole table (no checkpoints), and a retention policy for ledger rows must account for the append-only trigger.
- [x] Harden `DataComplianceService`. The original monthly lifecycle passed CI run 38184674490 (2026-10-11). Flyway V6, CI-verified in run 38206550351 (commit `5383987`, all jobs green), migrates existing rows into UTC-hour partitions and replaces the daily job with an hourly UTC lifecycle that drops fully expired hour partitions and pre-creates the current hour plus 168 hours ahead. The migration upgrade test verifies recent-row placement and preserves future overflow rows; the DB-backed retention test verifies expired-hour dropping, default-overflow cleanup, current-row preservation, and future partition creation. The seven-day retention therefore has at most about a one-hour overrun for managed timestamps. Expired rows in the out-of-range default partition are still row-deleted and logged; this is an explicit overflow exception, not a cold drop. V6 takes an `ACCESS EXCLUSIVE` lock and rewrites existing telemetry once; larger-volume migration duration has not been measured and requires an appropriate maintenance window. The job has no multi-replica leader election.
- [x] Complete JWT algorithm/claims/expiry validation and tests. Implemented 2026-10-11 and covered by `SecurityConfigTest` (CI run 38185545443, commit `e7b1ff1`, all 4 jobs green): HS256 only, mandatory numeric `exp`, `nbf`, 30 s skew, optional issuer/audience, `sub` principal. Bearer tokens deliberately grant no authorities, so edge-node tokens cannot reach `ADMIN` endpoints. `BearerAuthenticationHttpTest` covers the HTTP filter (valid, expired, wrong secret, missing, and no ADMIN access). Key rotation is supported through `JWT_SECRET_KEY_PREVIOUS` (comma-separated, tried after the current key). Open: no `kid` support, and no token-issuing component (tokens are minted out of band).
- [x] Test Redis outage and fallback explicitly. Covered by `StreamingAnomalyDetectorRedisOutageTest` (unit-level, mocked Redis whose every call fails): velocity spikes, velocity bursts, and telemetry outliers are still detected through the in-memory window. Not covered: a live Redis pod outage and recovery. The fallback window is per replica, so during an outage detection is per replica, not global.

**Exit gate:** Failure drills demonstrate bounded backlog, observable recovery, durable source data, deterministic ledger verification, and retention within the documented seven-day-plus-one-hour partition bound (with explicit overflow cleanup).

### 4. Make the Workflow Observable and Usable — In Progress

- [x] Single trace verified in Tempo (CI run 38195405082): `http post /api/v1/transactions`, `ledger.append`, `outbox.cdc.publish` and `outbox.consume` share one trace id, asserted by `scripts/cdc-drill.sh`.
- [x] Dashboard PromQL validated against live Prometheus (CI run 38213179485).
- [x] Grafana visual rendering verified: `GrafanaDashboardRenderingTest` (headless Chromium, Kind Grafana 13.2.3, after the load test) found all 13 panels, zero datasource query errors, and 0 of 13 panels showing No data; screenshot in the `grafana-dashboard-render` artifact of CI run 38217831739. Recovery panels show emitted signals only. Publish-ack duration is not end-to-end event latency; no whole-workflow latency metric is emitted.
- [x] Migration V7 and the updated Playwright timeline test passed through database-backed CI (run 38213179485). The control room has an event/correlation-keyed sanitized timeline for accepted, persisted, published, anomaly/alert, ledger, retries, and available trace ID; it excludes payloads and raw failure text and presents loading, error, empty, pending, and failed states.
- [x] Remove the duplicate root/static UI assets and add a Playwright/Chromium browser test for authenticated loan and anomaly workflows. Verified locally and in CI run 38204706720 (2026-10-12).
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
