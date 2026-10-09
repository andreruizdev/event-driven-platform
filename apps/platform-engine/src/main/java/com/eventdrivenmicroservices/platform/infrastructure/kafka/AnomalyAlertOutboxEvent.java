package com.eventdrivenmicroservices.platform.infrastructure.kafka;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.UUID;

@Table("anomaly_alert_outbox")
public record AnomalyAlertOutboxEvent(
        @Id @Column("event_id") UUID eventId,
        @Column("aggregate_id") String aggregateId,
        String envelope,
        String traceparent,
        @Column("created_at") Instant createdAt,
        @Column("published_at") Instant publishedAt,
        @Column("retry_count") int retryCount,
        @Column("last_error") String lastError,
        @Column("locked_until") Instant lockedUntil) {
}