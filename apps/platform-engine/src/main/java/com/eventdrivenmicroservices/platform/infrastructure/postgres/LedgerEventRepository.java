package com.eventdrivenmicroservices.platform.infrastructure.postgres;

import com.eventdrivenmicroservices.platform.domain.LedgerEvent;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;

import java.util.UUID;

@Repository
public interface LedgerEventRepository extends ReactiveCrudRepository<LedgerEvent, UUID> {

    Flux<LedgerEvent> findAllByOrderByCreatedAtDesc();

    Mono<LedgerEvent> findFirstByOutboxEventId(UUID outboxEventId);
    
    @Query("SELECT current_hash FROM ledger_events ORDER BY seq DESC LIMIT 1")
    Mono<String> findLatestHash();

    @Query("SELECT * FROM ledger_events ORDER BY seq ASC")
    Flux<LedgerEvent> findAllInChainOrder();

    @Query("SELECT * FROM (SELECT * FROM ledger_events ORDER BY seq DESC LIMIT :limit) latest ORDER BY seq ASC")
    Flux<LedgerEvent> findRecentInChainOrder(long limit);
}
