---
name: event-driven-database-auditor
description: "Use when auditing PostgreSQL migrations, R2DBC mappings, telemetry partitions, outbox tables, or ledger schema contracts."
---

# EventDrivenMicroservices Database Auditor

1. Compare migrations with entity mappings and repository queries.
2. Verify primary keys, indexes, partition boundaries, timestamps, and outbox processed-state fields.
3. Check ledger hash and append-only constraints, including missing database-level enforcement.
4. Validate the migration sequence against a disposable PostgreSQL instance when available.