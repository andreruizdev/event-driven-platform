package com.eventdrivenmicroservices.platform.infrastructure.outbox;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import java.util.UUID;

import reactor.core.publisher.Flux;

import reactor.core.publisher.Mono;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.repository.query.Param;
import java.time.Instant;

public interface OutboxEventRepository extends ReactiveCrudRepository<OutboxEvent, UUID> {
    Flux<OutboxEvent> findByProcessedFalse();

    @Query("SELECT * FROM outbox_events WHERE processed = false AND (locked_until IS NULL OR locked_until < :now) ORDER BY created_at ASC LIMIT :limit")
    Flux<OutboxEvent> findPendingForClaim(@Param("now") Instant now, @Param("limit") int limit);

    @Modifying
    @Query("UPDATE outbox_events SET locked_by = :lockedBy, locked_until = :lockedUntil WHERE id = :id AND (locked_until IS NULL OR locked_until < :now) AND processed = false")
    Mono<Integer> claimLock(@Param("id") UUID id, @Param("lockedBy") String lockedBy, @Param("lockedUntil") Instant lockedUntil, @Param("now") Instant now);

    Mono<Long> countByProcessedFalse();

    Mono<OutboxEvent> findFirstByProcessedFalseOrderByCreatedAtAsc();
    
    @Modifying
    @Query("DELETE FROM outbox_events WHERE processed = true AND created_at < :cutoffDate")
    Mono<Integer> deleteProcessedOldEvents(@Param("cutoffDate") Instant cutoffDate);
}
