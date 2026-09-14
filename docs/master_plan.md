# EventDrivenMicroservices Master Plan

This document describes the long-term learning, portfolio, and delivery plan. It does not replace the implementation plan in [rebuild_guide.md](rebuild_guide.md) or the current architecture in [architecture.md](architecture.md).

## Project Story

EventDrivenMicroservices is a Java 21 reactive event-driven platform that demonstrates financial ingestion, transactional outbox delivery, anomaly detection, cryptographic ledgering, Kubernetes deployment, and observable distributed workflows.

The public story should be based on one verified workflow rather than a list of planned technologies:

```text
loan or transaction request
  -> PostgreSQL persistence
  -> transactional outbox
  -> Kafka event
  -> anomaly decision
  -> ledger append
  -> alert and trace evidence
```

## Delivery Tracks

### Track A: Working demo

1. Add read APIs for anomalies, outbox status, ledger chain, and ledger integrity.
2. Add deterministic PowerShell and Bash demo scripts.
3. Add a lightweight UI served by the Spring application.
4. Show a normal transaction and a velocity anomaly.
5. Return and display correlation and trace IDs.

### Track B: Event reliability

1. Define a versioned event envelope.
2. Add outbox claiming, retries, and idempotency.
3. Add publish latency and backlog metrics.
4. Test Kafka failure and duplicate delivery.
5. Verify whether the scheduled relay or Debezium is the primary demo path.

### Track C: Observability

1. Propagate W3C context through HTTP and Kafka.
2. Add database, outbox, anomaly, ledger, and alert spans.
3. Configure Tempo and Loki datasources in Grafana.
4. Add business-flow, outbox, Kafka, runtime, and compliance dashboards.
5. Prove one complete trace in Tempo.

### Track D: External dependency simulation

Add an optional Flask mock financial provider for approval, decline, timeout, delayed settlement, duplicate response, and provider outage scenarios. Keep the core platform Java-based.

### Track E: Service extraction

Extract only after the modular monolith has a verified demo and contract tests:

1. Financial API.
2. Outbox relay.
3. Fraud/anomaly service.
4. Ledger service.
5. Alert service.

## Portfolio Milestones

### Milestone 1: Foundation

- Java 21 build passes.
- Helm lint and rendering pass.
- Secrets are externalized.
- Naming and documentation are coherent.

### Milestone 2: Visible workflow

- A clean checkout starts the local environment.
- One script or UI flow creates an event.
- Anomaly and ledger results are visible.

### Milestone 3: Operational proof

- Metrics show business and infrastructure behavior.
- Tempo shows the correlated trace.
- Failure and recovery are demonstrable.
- Tests cover unit, integration, contract, E2E, and observability behavior.

### Milestone 4: Distributed architecture

- Services have independent boundaries and contracts.
- Each service has its own deployment, health checks, telemetry, and tests.

## Documentation Hierarchy

- `README.md`: public project story, quick start, screenshots, and demo.
- `docs/rebuild_guide.md`: canonical phased implementation plan.
- `docs/architecture.md`: current technical architecture.
- `docs/Verdict.md`: analysis and architectural decisions.
- `docs/codebase-analysis-refactoring-steps.md`: executable structural refactoring playbook.
- `docs/master_plan.md`: portfolio and learning roadmap.
- `docs/testing.md`: executable test layers and evidence policy.

## Definition of Success

The project is ready to showcase when a reviewer can:

1. Follow the README from a clean checkout.
2. Run the documented local or Codespaces workflow.
3. Submit a realistic financial request.
4. Observe persistence, event publication, anomaly evaluation, and ledgering.
5. Follow the same correlation ID and trace in the UI and Grafana.
6. Trigger a controlled failure and see recovery behavior.

All later architectural work should strengthen this story rather than obscure it.
