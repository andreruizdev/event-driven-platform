package com.eventdrivenmicroservices.platform.application;

import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEventRepository;
import com.eventdrivenmicroservices.platform.infrastructure.postgres.TelemetryEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

@Service
@Slf4j
public class DataComplianceService {

    private final TelemetryEventRepository telemetryEventRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final DatabaseClient databaseClient;

    @Value("${eventdrivenmicroservices.compliance.retention-days:7}")
    private int retentionDays;

    @Autowired
    public DataComplianceService(TelemetryEventRepository telemetryEventRepository,
                                 OutboxEventRepository outboxEventRepository,
                                 @Autowired(required = false) DatabaseClient databaseClient) {
        this.telemetryEventRepository = telemetryEventRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.databaseClient = databaseClient;
    }

    /**
     * Executes scheduled zero-downtime data purge complying with AGENTS.md Rule #2.
     * High-volume telemetry uses PostgreSQL partition dropping (Cold Deletes) to prevent WAL bloat.
     */
    @Scheduled(cron = "0 0 2 * * ?") // Run at 2 AM every day
    public void executeDataPurge() {
        log.info("Starting Data Compliance Lifecycle Purge. Retention period: {} days", retentionDays);

        OffsetDateTime telemetryCutoff = OffsetDateTime.now().minusDays(retentionDays);
        Instant outboxCutoff = Instant.now().minus(retentionDays, ChronoUnit.DAYS);

        // 1. Zero-Downtime Partition Drop (Cold Deletes)
        executeColdPartitionPurge(telemetryCutoff)
                .doOnSuccess(v -> log.info("[COLD PURGE] Partition lifecycle maintenance completed successfully."))
                .doOnError(e -> log.error("[COLD PURGE] Error during partition lifecycle maintenance", e))
                .subscribe();

        // 2. Processed Outbox Events Purge
        outboxEventRepository.deleteProcessedOldEvents(outboxCutoff)
                .doOnSuccess(count -> log.info("[OUTBOX PURGE] Purged {} processed old outbox events.", count))
                .doOnError(e -> log.error("[OUTBOX PURGE] Error purging processed outbox events", e))
                .subscribe();
    }

    /**
     * Identifies and drops partitions that have aged past the retention period.
     * Table names follow the telemetry_events_yYYYYmMM convention.
     */
    public Mono<Void> executeColdPartitionPurge(OffsetDateTime cutoffDate) {
        if (databaseClient == null) {
            log.warn("[COLD PURGE] DatabaseClient unavailable; falling back to transactional delete.");
            return telemetryEventRepository.deleteOldTelemetryEvents(cutoffDate).then();
        }

        // Determine candidate expired month partition
        OffsetDateTime expiredMonth = cutoffDate.minusMonths(1);
        String partitionName = "telemetry_events_y" + expiredMonth.format(DateTimeFormatter.ofPattern("yyyy'm'MM"));

        log.info("[COLD PURGE] Checking candidate expired partition for zero-WAL drop: {}", partitionName);
        String dropDdl = "DROP TABLE IF EXISTS " + partitionName + " CASCADE";

        return databaseClient.sql(dropDdl)
                .then()
                .doOnSuccess(v -> log.info("[COLD PURGE] Dropped expired partition: {}", partitionName))
                .onErrorResume(e -> {
                    log.warn("[COLD PURGE] Partition drop note: {} - falling back to row cleanup", e.getMessage());
                    return telemetryEventRepository.deleteOldTelemetryEvents(cutoffDate).then();
                });
    }
}
