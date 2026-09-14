---
name: event-driven-outbox-tester
description: "Tests the Transactional Outbox pattern latency and Kafka event delivery SLA."
---

# Event-Driven Outbox Tester Skill

When invoked, perform the following actions:

1. **Verify Outbox Persistence**: Check that `LoanService` writes outbox events inside `@Transactional` R2DBC database blocks.
2. **Measure Delivery Latency**: Verify that `OutboxProcessorService` polls and publishes events to Kafka topic `eventdrivenmicroservices-outbox-events`.
3. **Verify Debezium CDC**: Ensure Debezium PostgreSQL connector streams WAL changes for `public.outbox_events`.
4. **Assert SLA**: Flag any delivery latency exceeding 500 ms.
