package com.eventdrivenmicroservices.platform.infrastructure.postgres;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Exposes WAL retained by logical replication slots. A stalled or dead Debezium connector keeps its slot
 * pinned, which grows WAL until the disk fills; this makes that visible before it becomes an outage.
 */
@Component
@Slf4j
@ConditionalOnProperty(prefix = "app.outbox.publisher", name = "mode", havingValue = "cdc")
public class ReplicationSlotMonitor {

    static final String SLOT_QUERY = """
            SELECT COALESCE(max(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)), 0)::bigint AS retained_bytes,
                   count(*) FILTER (WHERE NOT active)::bigint AS inactive_slots,
                   count(*)::bigint AS slots
            FROM pg_replication_slots
            WHERE slot_type = 'logical'
            """;

    private final DatabaseClient databaseClient;
    private final AtomicLong retainedBytes = new AtomicLong();
    private final AtomicLong inactiveSlots = new AtomicLong();
    private final AtomicLong slots = new AtomicLong();

    public ReplicationSlotMonitor(DatabaseClient databaseClient, MeterRegistry registry) {
        this.databaseClient = databaseClient;
        Gauge.builder("eventdrivenmicroservices.replication.slot.retained.bytes", retainedBytes, AtomicLong::doubleValue)
                .description("Largest WAL volume retained by a logical replication slot")
                .register(registry);
        Gauge.builder("eventdrivenmicroservices.replication.slot.inactive", inactiveSlots, AtomicLong::doubleValue)
                .description("Logical replication slots with no active consumer")
                .register(registry);
        Gauge.builder("eventdrivenmicroservices.replication.slot.count", slots, AtomicLong::doubleValue)
                .description("Logical replication slots present")
                .register(registry);
    }

    @Scheduled(fixedDelayString = "${app.outbox.replication-monitor.interval-ms:15000}")
    public void refresh() {
        poll()
                .doOnError(error -> log.warn("Replication slot refresh failed", error))
                .onErrorResume(error -> Mono.empty())
                .subscribe();
    }

    Mono<Void> poll() {
        return databaseClient.sql(SLOT_QUERY)
                .map((row, meta) -> new long[] {
                        row.get("retained_bytes", Long.class),
                        row.get("inactive_slots", Long.class),
                        row.get("slots", Long.class)})
                .one()
                .doOnNext(values -> {
                    retainedBytes.set(values[0]);
                    inactiveSlots.set(values[1]);
                    slots.set(values[2]);
                })
                .then();
    }
}
