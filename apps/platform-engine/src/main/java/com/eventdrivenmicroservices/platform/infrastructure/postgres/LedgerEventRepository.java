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
    
    @Query("SELECT current_hash FROM ledger_events ORDER BY created_at DESC LIMIT 1")
    Mono<String> findLatestHash();
}
