package com.eventdrivenmicroservices.platform.application;

import com.eventdrivenmicroservices.platform.infrastructure.kafka.AnomalyAlertOutboxRepository;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

@Service
@Slf4j
public class DataComplianceService {

    private static final Pattern PARTITION_NAME =
            Pattern.compile("^telemetry_events_y(\\d{4})m(\\d{2})d(\\d{2})h(\\d{2})$");
    private static final String LIST_PARTITIONS_SQL = """
            SELECT c.relname AS relname
            FROM pg_inherits i
            JOIN pg_class c ON c.oid = i.inhrelid
            JOIN pg_class p ON p.oid = i.inhparent
            WHERE p.relname = 'telemetry_events'
            """;

    private final OutboxEventRepository outboxEventRepository;
    private final AnomalyAlertOutboxRepository alertOutboxRepository;
    private final DatabaseClient databaseClient;

    @Value("${eventdrivenmicroservices.compliance.retention-days:7}")
    private int retentionDays;

    @Value("${eventdrivenmicroservices.compliance.partitions-ahead-hours:168}")
    private int partitionsAheadHours;

    @Autowired
    public DataComplianceService(OutboxEventRepository outboxEventRepository,
                                 AnomalyAlertOutboxRepository alertOutboxRepository,
                                 DatabaseClient databaseClient) {
        this.outboxEventRepository = outboxEventRepository;
        this.alertOutboxRepository = alertOutboxRepository;
        this.databaseClient = databaseClient;
    }

    /**
     * Hourly lifecycle job. Spring subscribes to the returned Mono, so the whole chain runs in order and
     * stays attached to the scheduler instead of a detached subscription.
     * Each stage is isolated so one failing stage never skips retention of the others.
     */
    @Scheduled(cron = "0 0 * * * ?", zone = "UTC")
    public Mono<Void> executeDataPurge() {
        return Mono.defer(() -> {
            log.info("Starting Data Compliance Lifecycle Purge. Retention period: {} days", retentionDays);
            OffsetDateTime telemetryCutoff = OffsetDateTime.now(ZoneOffset.UTC).minusDays(retentionDays);
            Instant outboxCutoff = Instant.now().minus(retentionDays, ChronoUnit.DAYS);

            Mono<Void> partitions = ensureFuturePartitions(LocalDateTime.now(ZoneOffset.UTC)
                            .truncatedTo(ChronoUnit.HOURS))
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
     * Drops every hourly partition whose whole range is older than the cutoff. The default partition is
     * reserved for timestamps outside the managed range, so only its expired overflow rows are deleted.
     */
    public Mono<Void> executeColdPartitionPurge(OffsetDateTime cutoffDate) {
        if (databaseClient == null) {
            return Mono.error(new IllegalStateException("DatabaseClient is required for hourly partition retention."));
        }

        return databaseClient.sql(LIST_PARTITIONS_SQL)
                .map((row, metadata) -> row.get("relname", String.class))
                .all()
                .collectList()
                .flatMapMany(names -> Flux.fromIterable(expiredPartitions(names, cutoffDate.toInstant())))
                .concatMap(this::dropPartition)
                .then(databaseClient.sql("DELETE FROM telemetry_events_default WHERE timestamp < :cutoff")
                        .bind("cutoff", cutoffDate)
                        .fetch()
                        .rowsUpdated()
                        .doOnNext(deleted -> {
                            if (deleted > 0) {
                                log.warn("[COLD PURGE] Deleted {} expired out-of-range telemetry rows from the default partition.",
                                        deleted);
                            }
                        })
                        .then())
                .then();
    }

    Mono<Void> ensureFuturePartitions(LocalDateTime currentHour) {
        if (databaseClient == null) {
            return Mono.error(new IllegalStateException("DatabaseClient is required to provision hourly partitions."));
        }
        return Flux.fromIterable(partitionsToCreate(currentHour, partitionsAheadHours))
                .concatMap(hour -> databaseClient.sql(createPartitionDdl(hour)).then()
                        .onErrorResume(e -> {
                            log.warn("[PARTITIONS] Could not create {}: {}", partitionName(hour), e.getMessage());
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
                    LocalDateTime hour = parseHour(name);
                    return hour != null && !hour.plusHours(1).toInstant(ZoneOffset.UTC).isAfter(cutoff);
                })
                .sorted()
                .toList();
    }

    static List<LocalDateTime> partitionsToCreate(LocalDateTime currentHour, int aheadHours) {
        LocalDateTime start = currentHour.truncatedTo(ChronoUnit.HOURS);
        return IntStream.rangeClosed(0, Math.max(aheadHours, 0))
                .mapToObj(start::plusHours)
                .toList();
    }

    static String partitionName(LocalDateTime hour) {
        return "telemetry_events_y" + hour.format(java.time.format.DateTimeFormatter.ofPattern("yyyy'm'MM'd'dd'h'HH"));
    }

    static String createPartitionDdl(LocalDateTime hour) {
        LocalDateTime start = hour.truncatedTo(ChronoUnit.HOURS);
        return "CREATE TABLE IF NOT EXISTS " + partitionName(start)
                + " PARTITION OF telemetry_events FOR VALUES FROM ('" + partitionBound(start) + "') TO ('"
                + partitionBound(start.plusHours(1)) + "')";
    }

    private static String partitionBound(LocalDateTime hour) {
        return hour.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "+00";
    }

    // Only names matching the strict hourly pattern are ever used in DDL, so catalog output cannot inject SQL.
    private static LocalDateTime parseHour(String name) {
        if (name == null) {
            return null;
        }
        Matcher matcher = PARTITION_NAME.matcher(name);
        if (!matcher.matches()) {
            return null;
        }
        try {
            return LocalDateTime.of(Integer.parseInt(matcher.group(1)), Integer.parseInt(matcher.group(2)),
                    Integer.parseInt(matcher.group(3)), Integer.parseInt(matcher.group(4)), 0);
        } catch (java.time.DateTimeException e) {
            return null;
        }
    }
}
