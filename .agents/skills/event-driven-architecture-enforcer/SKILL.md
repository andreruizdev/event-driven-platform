---
name: event-driven-architecture-enforcer
description: "Use when checking WebFlux, Reactor, R2DBC, transactional outbox boundaries, blocking I/O, or package architecture."
---

# EventDrivenMicroservices Architecture Enforcer

1. Inspect request-path Java code for blocking JDBC, `block`, sleeps, or synchronous filesystem/network calls.
2. Confirm domain persistence and outbox writes share the intended reactive transaction boundary.
3. Confirm package ownership under `com.eventdrivenmicroservices.platform`.
4. Run the narrowest ArchUnit or Gradle test available and distinguish static findings from runtime evidence.