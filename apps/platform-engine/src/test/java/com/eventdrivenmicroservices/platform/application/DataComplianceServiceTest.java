package com.eventdrivenmicroservices.platform.application;

import com.eventdrivenmicroservices.platform.infrastructure.kafka.AnomalyAlertOutboxRepository;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEventRepository;
import com.eventdrivenmicroservices.platform.infrastructure.postgres.TelemetryEventRepository;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.YearMonth;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DataComplianceServiceTest {

    private static final Instant CUTOFF = Instant.parse("2026-10-04T02:00:00Z");

    @Test
    void selectsEveryPartitionFullyOlderThanCutoff() {
        List<String> expired = DataComplianceService.expiredPartitions(List.of(
                "telemetry_events_y2026m06", "telemetry_events_y2026m07", "telemetry_events_y2026m09",
                "telemetry_events_y2026m10", "telemetry_events_y2026m11", "telemetry_events_default"), CUTOFF);

        assertThat(expired).containsExactly(
                "telemetry_events_y2026m06", "telemetry_events_y2026m07", "telemetry_events_y2026m09");
    }

    @Test
    void keepsPartitionEndingExactlyAfterCutoffAndDropsOneEndingAtCutoff() {
        Instant boundary = Instant.parse("2026-10-01T00:00:00Z");

        assertThat(DataComplianceService.expiredPartitions(List.of("telemetry_events_y2026m09"), boundary))
                .containsExactly("telemetry_events_y2026m09");
        assertThat(DataComplianceService.expiredPartitions(List.of("telemetry_events_y2026m09"),
                boundary.minusSeconds(1))).isEmpty();
    }

    @Test
    void ignoresNamesThatAreNotStrictMonthlyPartitions() {
        List<String> expired = DataComplianceService.expiredPartitions(java.util.Arrays.asList(
                "telemetry_events_y2020m13", "telemetry_events_y2020m01; DROP TABLE loan_applications",
                "loan_applications", null), CUTOFF);

        assertThat(expired).isEmpty();
    }

    @Test
    void plansCurrentAndFutureMonths() {
        assertThat(DataComplianceService.partitionsToCreate(YearMonth.of(2026, 11), 3))
                .containsExactly(YearMonth.of(2026, 11), YearMonth.of(2026, 12),
                        YearMonth.of(2027, 1), YearMonth.of(2027, 2));
    }

    @Test
    void buildsUtcBoundedCreateStatement() {
        assertThat(DataComplianceService.createPartitionDdl(YearMonth.of(2026, 12)))
                .isEqualTo("CREATE TABLE IF NOT EXISTS telemetry_events_y2026m12 PARTITION OF telemetry_events "
                        + "FOR VALUES FROM ('2026-12-01 00:00:00+00') TO ('2027-01-01 00:00:00+00')");
    }

    @Test
    void jobRunsOutboxPurgeEvenWhenTelemetryRetentionFails() {
        TelemetryEventRepository telemetry = mock(TelemetryEventRepository.class);
        OutboxEventRepository outbox = mock(OutboxEventRepository.class);
        AnomalyAlertOutboxRepository alerts = mock(AnomalyAlertOutboxRepository.class);
        when(alerts.deletePublishedBefore(any(Instant.class))).thenReturn(Mono.just(1));
        when(telemetry.deleteOldTelemetryEvents(any(OffsetDateTime.class)))
                .thenReturn(Mono.error(new IllegalStateException("db down")));
        when(outbox.deleteProcessedOldEvents(any(Instant.class))).thenReturn(Mono.just(2));

        StepVerifier.create(new DataComplianceService(telemetry, outbox, alerts, null).executeDataPurge())
                .verifyComplete();

        verify(outbox).deleteProcessedOldEvents(any(Instant.class));
        verify(alerts).deletePublishedBefore(any(Instant.class));
    }
}
