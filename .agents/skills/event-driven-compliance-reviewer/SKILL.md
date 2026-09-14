---
name: event-driven-compliance-reviewer
description: "Use when reviewing secret handling, retention purges, outbox audit trails, or zero-data-retention behavior."
---

# EventDrivenMicroservices Compliance Reviewer

1. Search source, Helm, CI, and examples for committed credentials or unsafe defaults.
2. Verify telemetry and processed-outbox retention behavior against the migrations and compliance service.
3. Check that purge operations preserve recent data and produce auditable behavior.
4. Report any fallback path that weakens the documented compliance guarantee.