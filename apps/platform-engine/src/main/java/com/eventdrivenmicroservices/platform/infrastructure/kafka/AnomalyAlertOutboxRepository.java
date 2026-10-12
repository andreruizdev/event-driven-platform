package com.eventdrivenmicroservices.platform.infrastructure.kafka;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.query.Param;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

public interface AnomalyAlertOutboxRepository
        extends ReactiveCrudRepository<AnomalyAlertOutboxEvent, UUID> {

    Mono<AnomalyAlertOutboxEvent> findByEventId(UUID eventId);

    @Modifying
    @Query("""
            INSERT INTO anomaly_alert_outbox (event_id, aggregate_id, envelope, traceparent, created_at)
            VALUES (:eventId, :aggregateId, :envelope, :traceparent, :createdAt)
            ON CONFLICT (event_id) DO NOTHING
            """)
    Mono<Integer> insertIfAbsent(@Param("eventId") UUID eventId,
                                 @Param("aggregateId") String aggregateId,
                                 @Param("envelope") String envelope,
                                 @Param("traceparent") String traceparent,
                                 @Param("createdAt") Instant createdAt);

    @Query("""
            UPDATE anomaly_alert_outbox
            SET locked_until = :lockedUntil
            WHERE event_id = (
                SELECT event_id
                FROM anomaly_alert_outbox
                WHERE published_at IS NULL
                  AND (locked_until IS NULL OR locked_until < :now)
                ORDER BY created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            AND published_at IS NULL
            RETURNING *
            """)
    Mono<AnomalyAlertOutboxEvent> claimNext(@Param("now") Instant now,
                                             @Param("lockedUntil") Instant lockedUntil);

    @Modifying
    @Query("""
            UPDATE anomaly_alert_outbox
            SET published_at = :publishedAt, locked_until = NULL, last_error = NULL
            WHERE event_id = :eventId AND published_at IS NULL
            """)
    Mono<Integer> markPublished(@Param("eventId") UUID eventId,
                                @Param("publishedAt") Instant publishedAt);

    @Modifying
    @Query("""
            UPDATE anomaly_alert_outbox
            SET retry_count = retry_count + 1,
                last_error = :lastError,
                locked_until = :nextAttemptAt
            WHERE event_id = :eventId AND published_at IS NULL
            """)
    Mono<Integer> recordFailure(@Param("eventId") UUID eventId,
                                @Param("lastError") String lastError,
                                @Param("nextAttemptAt") Instant nextAttemptAt);

    @Modifying
    @Query("DELETE FROM anomaly_alert_outbox WHERE published_at IS NOT NULL AND published_at < :cutoffDate")
    Mono<Integer> deletePublishedBefore(@Param("cutoffDate") Instant cutoffDate);
}
