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

    Mono<Long> countByProcessedFalse();

    Mono<OutboxEvent> findFirstByProcessedFalseOrderByCreatedAtAsc();
    
    @Modifying
    @Query("DELETE FROM outbox_events WHERE processed = true AND created_at < :cutoffDate")
    Mono<Integer> deleteProcessedOldEvents(@Param("cutoffDate") Instant cutoffDate);
}
