package com.eventdrivenmicroservices.platform.application;

import com.eventdrivenmicroservices.platform.infrastructure.kafka.AnomalyAlertOutboxRepository;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEventRepository;
import com.eventdrivenmicroservices.platform.infrastructure.postgres.TelemetryEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

@Service
@Slf4j
public class DataComplianceService {

    private static final Pattern PARTITION_NAME = Pattern.compile("^telemetry_events_y(\\d{4})m(\\d{2})$");
    private static final String LIST_PARTITIONS_SQL = """
            SELECT c.relname AS relname
            FROM pg_inherits i
            JOIN pg_class c ON c.oid = i.inhrelid
            JOIN pg_class p ON p.oid = i.inhparent
            WHERE p.relname = 'telemetry_events'
            """;

    private final TelemetryEventRepository telemetryEventRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final AnomalyAlertOutboxRepository alertOutboxRepository;
    private final DatabaseClient databaseClient;

    @Value("${eventdrivenmicroservices.compliance.retention-days:7}")
    private int retentionDays;

    @Value("${eventdrivenmicroservices.compliance.partitions-ahead:3}")
    private int partitionsAhead;

    @Autowired
    public DataComplianceService(TelemetryEventRepository telemetryEventRepository,
                                 OutboxEventRepository outboxEventRepository,
                                 AnomalyAlertOutboxRepository alertOutboxRepository,
                                 @Autowired(required = false) DatabaseClient databaseClient) {
        this.telemetryEventRepository = telemetryEventRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.alertOutboxRepository = alertOutboxRepository;
        this.databaseClient = databaseClient;
    }

    /**
     * Daily lifecycle job. Spring subscribes to the returned Mono, so the whole chain runs in order and
     * stays attached to the scheduler instead of a detached subscription.
     * Each stage is isolated so one failing stage never skips retention of the others.
     */
    @Scheduled(cron = "0 0 2 * * ?")
    public Mono<Void> executeDataPurge() {
        return Mono.defer(() -> {
            log.info("Starting Data Compliance Lifecycle Purge. Retention period: {} days", retentionDays);
            OffsetDateTime telemetryCutoff = OffsetDateTime.now(ZoneOffset.UTC).minusDays(retentionDays);
            Instant outboxCutoff = Instant.now().minus(retentionDays, ChronoUnit.DAYS);

            Mono<Void> partitions = ensureFuturePartitions(YearMonth.now(ZoneOffset.UTC))
                    .doOnError(e -> log.error("[PARTITIONS] Error pre-creating telemetry partitions", e))
                    .onErrorResume(e -> Mono.empty());
            Mono<Void> telemetry = executeColdPartitionPurge(telemetryCutoff)
                    .doOnSuccess(v -> log.info("[COLD PURGE] Telemetry retention completed."))
                    .doOnError(e -> log.error("[COLD PURGE] Error during telemetry retention", e))
                    .onErrorResume(e -> Mono.empty());
            Mono<Void> outbox = outboxEventRepository.deleteProcessedOldEvents(outboxCutoff)
                    .doOnSuccess(count -> log.info("[OUTBOX PURGE] Purged {} processed old outbox events.", count))
                    .doOnError(e -> log.error("[OUTBOX PURGE] Error purging processed outbox events", e))
                    .then()
                    .onErrorResume(e -> Mono.empty());

            Mono<Void> alerts = alertOutboxRepository.deletePublishedBefore(outboxCutoff)
                    .doOnSuccess(count -> log.info("[ALERT OUTBOX PURGE] Purged {} published alert outbox rows.", count))
                    .doOnError(e -> log.error("[ALERT OUTBOX PURGE] Error purging published alert outbox rows", e))
                    .then()
                    .onErrorResume(e -> Mono.empty());

            return partitions.then(telemetry).then(outbox).then(alerts);
        });
    }

    /**
     * Drops every monthly partition whose whole range is older than the cutoff (no WAL-heavy deletes), then
     * deletes remaining expired rows from the partition straddling the cutoff and the default partition.
     * Rows newer than the cutoff are never touched.
     */
    public Mono<Void> executeColdPartitionPurge(OffsetDateTime cutoffDate) {
        if (databaseClient == null) {
            log.warn("[COLD PURGE] DatabaseClient unavailable; falling back to transactional delete.");
            return telemetryEventRepository.deleteOldTelemetryEvents(cutoffDate).then();
        }

        return databaseClient.sql(LIST_PARTITIONS_SQL)
                .map((row, metadata) -> row.get("relname", String.class))
                .all()
                .collectList()
                .flatMapMany(names -> Flux.fromIterable(expiredPartitions(names, cutoffDate.toInstant())))
                .concatMap(this::dropPartition)
                .then(Mono.defer(() -> telemetryEventRepository.deleteOldTelemetryEvents(cutoffDate)))
                .doOnSuccess(deleted -> log.info("[COLD PURGE] Deleted {} expired rows outside dropped partitions.", deleted))
                .then();
    }

    Mono<Void> ensureFuturePartitions(YearMonth current) {
        if (databaseClient == null) {
            return Mono.empty();
        }
        return Flux.fromIterable(partitionsToCreate(current, partitionsAhead))
                .concatMap(month -> databaseClient.sql(createPartitionDdl(month)).then()
                        .onErrorResume(e -> {
                            log.warn("[PARTITIONS] Could not create {}: {}", partitionName(month), e.getMessage());
                            return Mono.empty();
                        }))
                .then();
    }

    private Mono<Void> dropPartition(String partitionName) {
        log.info("[COLD PURGE] Dropping expired partition: {}", partitionName);
        return databaseClient.sql("DROP TABLE IF EXISTS " + partitionName).then();
    }

    static List<String> expiredPartitions(List<String> partitionNames, Instant cutoff) {
        return partitionNames.stream()
                .filter(name -> {
                    YearMonth month = parseMonth(name);
                    return month != null && !monthEnd(month).isAfter(cutoff);
                })
                .sorted()
                .toList();
    }

    static List<YearMonth> partitionsToCreate(YearMonth current, int ahead) {
        return IntStream.rangeClosed(0, Math.max(ahead, 0))
                .mapToObj(current::plusMonths)
                .toList();
    }

    static String partitionName(YearMonth month) {
        return String.format("telemetry_events_y%04dm%02d", month.getYear(), month.getMonthValue());
    }

    static String createPartitionDdl(YearMonth month) {
        return "CREATE TABLE IF NOT EXISTS " + partitionName(month)
                + " PARTITION OF telemetry_events FOR VALUES FROM ('" + month.atDay(1) + " 00:00:00+00') TO ('"
                + month.plusMonths(1).atDay(1) + " 00:00:00+00')";
    }

    // Only names matching the strict monthly pattern are ever used in DDL, so catalog output cannot inject SQL.
    private static YearMonth parseMonth(String name) {
        if (name == null) {
            return null;
        }
        Matcher matcher = PARTITION_NAME.matcher(name);
        if (!matcher.matches()) {
            return null;
        }
        int month = Integer.parseInt(matcher.group(2));
        return month < 1 || month > 12 ? null : YearMonth.of(Integer.parseInt(matcher.group(1)), month);
    }

    private static Instant monthEnd(YearMonth month) {
        return month.plusMonths(1).atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC);
    }
}
