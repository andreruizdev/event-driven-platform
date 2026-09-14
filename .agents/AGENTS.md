# EventDrivenMicroservices: Engineering Guidelines

These guidelines apply to code, deployment manifests, documentation, and automation in this repository.

---

## 1. Orchestration & Expertise Utilization
* Prefer current source files and focused tests over historical notes.
* Keep changes small, reversible, and grounded in the current implementation.

---

## 2. Architectural Adherence (Lab Operating Principles)
* **Non-Blocking Java Backend**: Any Spring Boot endpoints MUST follow the reactive non-blocking pattern established with Spring WebFlux, Project Reactor, and R2DBC (`platform-engine`). Do not introduce blocking I/O (e.g. synchronous JDBC/Hibernate, `Thread.sleep()`, or `.block()` calls) within request threads.
* **Transactional Outbox Pattern**: Dual-writes (persisting domain state alongside event outbox records) MUST occur within a single atomic database transaction before asynchronous publishing to Kafka.
* **Observability**: Prefer the configured OpenTelemetry collector for metrics and traces. Keep any direct Prometheus scrape path deliberate and documented.
* **Immutable Ledger Integrity**: Never write `UPDATE` or `DELETE` SQL statements against the `ledger_events` table. All modifications to the ledger must be append-only with SHA-256 hash-chaining.
* **Cold Deletes via Partitioning**: High-volume data streams (like `telemetry_events`) must not use massive transactional `DELETE` queries. Always utilize PostgreSQL Table Partitioning (`PARTITION BY RANGE`) for zero-downtime partition dropping.

---

## 3. Documentation
* **Reasoning Transparency**: Document reasoning, analysis, and proposed strategy before committing to major architectural shifts or executing large multi-file edits.
* Keep `docs/rebuild_guide.md` as the canonical implementation plan.
* Document current behavior only; do not preserve obsolete component names or unsupported claims.
* Record durable project plans in repository memory when they affect future work.

---

## 4. Security & Compliance
* **No Hardcoded Secrets**: Never write code containing hardcoded credentials (API keys, DB passwords, JWT secrets). Always use environment variables or Kubernetes secrets.
* **Principle of Least Privilege**: Default to restrictive settings in Dockerfiles and Kubernetes manifests (e.g. `runAsNonRoot: true`, `readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false`).
* **Financial Data Compliance**: Maintain auditability across all loan and transaction payloads via outbox logging, ledger hash-chaining, and compliance retention purges.
