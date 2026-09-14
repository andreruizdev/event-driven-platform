# EventDrivenMicroservices Rebuild Guide

This is the current rebuild plan for the EventDrivenMicroservices Lab. It is grounded in the files that exist in this repository as of September 2026. Historical architecture notes and agent memory were removed when they no longer described the active system.

## Current System

- Java 21 and Gradle 8.4 application in `apps/platform-engine`.
- Spring Boot WebFlux, R2DBC, PostgreSQL, Flyway, Kafka, Redis, and OpenTelemetry.
- Reactive ingestion controllers for loans, transactions, and telemetry.
- Transactional outbox, Debezium connector configuration, SHA-256 ledger, anomaly detection, and compliance purge services.
- Canonical Helm chart: `deploy/helm/event-driven-lab`.
- Local Kind provisioning: `terraform/environments/local/main.tf`.
- Local deployment entry points: `scripts/test-local.ps1` and `scripts/test-local.sh`.

## Phase 0 Status: Foundation

Phase 0 is structurally complete. The acceptance evidence is:

- Java production sources compile with Java 21: `./gradlew clean compileJava --no-daemon` passes.
- The canonical Helm chart passes `helm lint` and `helm template`.
- CI uses `deploy/helm/event-driven-lab` and creates the external `eventdrivenmicroservices-secrets` secret idempotently.
- Application and database credentials are environment-driven; `.env.local` is ignored.
- Active Java packages, database properties, Kafka topics, resource names, and scripts use the EventDrivenMicroservices identity.
- Helm no longer creates a credential-bearing Secret. CI or local orchestration must create it before installation.

Phase 0 is not an all-green runtime test claim. The full Gradle test task still requires Docker for Testcontainers and has separate behavioral test failures to resolve. Those are tracked below and must be closed before declaring the application runtime baseline complete.

### Gradle Runtime Requirement

Gradle 8.4 must run on Java 21. If VS Code reports `Unsupported class file major version 70` for `build.gradle`, its Gradle/Java language tooling is running on Java 26. Set the Gradle JVM and `JAVA_HOME` to a Java 21 installation, then reload the workspace. The project build script also declares a Java 21 toolchain for compilation.

## Phase 0 Operating Checks

```powershell
Set-Location apps/platform-engine
./gradlew clean compileJava --no-daemon
```

```powershell
Set-Location ../..
helm lint deploy/helm/event-driven-lab
helm template event-driven-lab deploy/helm/event-driven-lab
```

For a full local runtime check, provide Docker or Podman, Kind, kubectl, Helm, and Terraform, then run `scripts/test-local.ps1` or `scripts/test-local.sh`.

## Implementation Sequence

### Phase 1: Reactive Backend Baseline

Goal: prove the WebFlux and R2DBC application boundary independently of messaging.

1. Verify every controller request/response contract and validation rule.
2. Verify Flyway migration `V1__Initial_Schema.sql` against a disposable PostgreSQL instance.
3. Add focused repository and controller tests that do not require Kafka.
4. Keep blocking JDBC usage limited to Flyway startup; request handling remains reactive.

Exit evidence: controller tests pass, schema startup passes, and ArchUnit rejects blocking request-path code.

### Phase 2: Transactional Outbox

Goal: prove atomic domain persistence plus outbox persistence and reliable publication.

1. Test loan, transaction, and telemetry dual-writes inside the reactive transaction boundary.
2. Test successful publication, publication failure, retry behavior, and processed-state transitions.
3. Verify the canonical topic `eventdrivenmicroservices-outbox-events` end to end.
4. Verify the Debezium connector configuration matches PostgreSQL and `public.outbox_events`.

Exit evidence: a disposable PostgreSQL/Kafka test proves persistence and publication behavior with no lost event.

### Phase 3: Cryptographic Ledger

Goal: make ledger integrity explicit and testable.

1. Test genesis creation, previous-hash linkage, and SHA-256 recomputation.
2. Test concurrent append behavior and duplicate-sequence handling.
3. Add database-level append-only enforcement if immutability is a hard requirement.
4. Add a focused integrity verification command or test fixture.

Exit evidence: tampering, gaps, and invalid previous hashes are detected deterministically.

### Phase 4: Anomaly Detection

Goal: verify velocity, telemetry Z-score, Redis state, metrics, and alert routing.

1. Stabilize the existing detector unit tests and define deterministic baseline data.
2. Test Redis-backed windows and in-memory fallback behavior separately.
3. Verify metrics use the current `eventdrivenmicroservices.*` names.
4. Verify anomaly events route to `eventdrivenmicroservices-anomaly-alerts` and create the expected ledger event.

Exit evidence: normal and anomalous fixtures produce predictable scores, metrics, Kafka alerts, and ledger records.

### Phase 5: Security

Goal: make authentication behavior fail-safe and verifiable.

1. Add tests for valid, invalid, expired, malformed, and missing JWT credentials.
2. Use constant-time HMAC signature comparison.
3. Make a missing production JWT secret fail startup rather than silently leave validation unusable.
4. Verify Basic authentication and actuator role protection.

Exit evidence: security tests pass and no secret is present in source, Helm templates, or committed examples.

### Phase 6: Database Lifecycle

Goal: make telemetry retention and partition lifecycle operationally complete.

1. Add forward-only migrations for current and future telemetry partitions.
2. Test the scheduled purge and its fallback behavior against PostgreSQL.
3. Decide whether row-delete fallback is acceptable or whether cold partition deletion must be enforced.
4. Verify outbox retention independently from telemetry retention.

Exit evidence: retention tests prove the intended deletion boundary and leave recent data intact.

### Phase 7: Container Image

Goal: produce a reproducible, minimal, non-root image.

1. Build the multi-stage Docker image from the Gradle wrapper.
2. Verify non-root execution, writable `/tmp` behavior, and health endpoint startup.
3. Add JVM container settings only after measuring their need.
4. Run a vulnerability scan and record results outside the source tree unless a reviewed report is needed.

Exit evidence: image build, startup, health check, and security scan pass.

### Phase 8: Helm and Kind

Goal: deploy the complete stack reproducibly.

1. Provision the Kind cluster with Terraform.
2. Create the external secret before Helm installation.
3. Validate every deployment, service, probe, init container, resource limit, and security context.
4. Verify Kafka, PostgreSQL, Redis, and platform-engine readiness.

Exit evidence: all required pods become ready and the platform health endpoint responds through the deployed service.

### Phase 9: Observability

Goal: verify useful, correctly named metrics and traces rather than merely installing tools.

1. Confirm emitted Micrometer names match the Grafana queries.
2. Validate OTLP collector receivers and exporters against the deployed service names.
3. Decide whether Prometheus scraping remains as a deliberate compatibility path or is removed in favor of OTLP.
4. Add dashboard artifacts only after queries are verified against live metrics.

Exit evidence: anomaly, HTTP, trace, and outbox signals are visible in the deployed observability stack.

### Phase 10: Infrastructure and CI/CD

Goal: make the local and CI workflows repeatable and honest.

1. Keep the current local Terraform environment small until a second environment is required.
2. Add Terraform modules only when they remove duplication or support a real environment boundary.
3. Add a demo script only after the runtime flow is stable.
4. Make CI run security scan, Java verification, Helm validation, and Kind integration in that order.
5. Require CI to fail on chart rendering, missing secrets, test failures, or unready pods.

Exit evidence: CI is green from a clean checkout and the documented local workflow reproduces the same result.

## Immediate Work Queue

1. Run the full Gradle suite with Docker available and classify the five current failures.
2. Fix the detector and controller behavioral assertions.
3. Add missing security and outbox tests.
4. Add the required partition lifecycle migration and ledger immutability enforcement decision.
5. Execute the Kind deployment and capture readiness evidence.

## Naming Rules

- Java packages and Gradle group: `com.eventdrivenmicroservices.platform`.
- Database property namespace: `eventdrivenmicroservices.database`.
- Environment variables: `EVENTDRIVENMICROSERVICES_*`.
- Kubernetes resources: `eventdrivenmicroservices-*`.
- Helm chart and release: `event-driven-lab`.
- No legacy project identifiers may be introduced.

## Agent Skill Map

The active skills under `.agents/skills/` are organized around the rebuild rather than historical project branding:

- `event-driven-phase-reviewer`: compare implementation and evidence against every phase in this guide.
- `event-driven-architecture-enforcer`: inspect reactive boundaries, blocking calls, and package ownership.
- `event-driven-compliance-reviewer`: check retention, outbox auditability, and secret-handling requirements.
- `event-driven-database-auditor`: inspect migrations, partitions, outbox, and ledger schema contracts.
- `event-driven-deployment-validator`: validate Docker, Terraform, Helm, Kind, probes, and readiness.
- `event-driven-documentation-reviewer`: detect stale paths, names, claims, and undocumented runtime behavior.
- `event-driven-infra-secops`: review Kubernetes and container hardening.
- `event-driven-outbox-tester`: verify outbox delivery and Kafka latency.
- `ledger-integrity-checker`: verify ledger hash chains and append-only behavior.