package com.eventdrivenmicroservices.platform.infrastructure.postgres;

import com.eventdrivenmicroservices.platform.model.TelemetryEvent;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import java.util.UUID;

import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.repository.query.Param;
import reactor.core.publisher.Mono;
import java.time.OffsetDateTime;

@Repository
public interface TelemetryEventRepository extends ReactiveCrudRepository<TelemetryEvent, UUID> {
    
    @Modifying
    @Query("DELETE FROM telemetry_events WHERE timestamp < :cutoffDate")
    Mono<Integer> deleteOldTelemetryEvents(@Param("cutoffDate") OffsetDateTime cutoffDate);
}
