package com.eventdrivenmicroservices.platform.infrastructure.outbox;

import java.time.Instant;
import java.util.UUID;

public record EventEnvelope(
        UUID eventId,
        String eventType,
        int eventVersion,
        String aggregateType,
        String aggregateId,
        String traceparent,
        Instant occurredAt,
        String payload) {
}
