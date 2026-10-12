package com.eventdrivenmicroservices.platform.application;

import com.eventdrivenmicroservices.platform.infrastructure.kafka.AnomalyAlertOutboxRepository;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEventRepository;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DataComplianceServiceTest {

    private static final Instant CUTOFF = Instant.parse("2026-10-04T02:00:00Z");

    @Test
    void selectsEveryHourlyPartitionFullyOlderThanCutoff() {
        List<String> expired = DataComplianceService.expiredPartitions(List.of(
                "telemetry_events_y2026m10d03h23", "telemetry_events_y2026m10d04h00",
                "telemetry_events_y2026m10d04h01", "telemetry_events_y2026m10d04h02",
                "telemetry_events_default"), CUTOFF);

        assertThat(expired).containsExactly(
                "telemetry_events_y2026m10d03h23", "telemetry_events_y2026m10d04h00",
                "telemetry_events_y2026m10d04h01");
    }

    @Test
    void keepsPartitionEndingAfterCutoffAndDropsOneEndingExactlyAtCutoff() {
        Instant boundary = Instant.parse("2026-10-04T02:00:00Z");

        assertThat(DataComplianceService.expiredPartitions(List.of("telemetry_events_y2026m10d04h01"), boundary))
                .containsExactly("telemetry_events_y2026m10d04h01");
        assertThat(DataComplianceService.expiredPartitions(List.of("telemetry_events_y2026m10d10h01"),
                boundary.minusSeconds(1))).isEmpty();
    }

    @Test
    void ignoresNamesThatAreNotStrictHourlyPartitions() {
        List<String> expired = DataComplianceService.expiredPartitions(java.util.Arrays.asList(
                "telemetry_events_y2020m13d01h00", "telemetry_events_y2020m01d32h00",
                "telemetry_events_y2020m01d01h24", "telemetry_events_y2020m01d01h00; DROP TABLE loan_applications",
                "loan_applications", null), CUTOFF);

        assertThat(expired).isEmpty();
    }

    @Test
    void plansCurrentAndFutureHoursFromTheHourBoundary() {
        assertThat(DataComplianceService.partitionsToCreate(LocalDateTime.parse("2026-11-01T12:34:56"), 3))
                .containsExactly(LocalDateTime.parse("2026-11-01T12:00:00"),
                        LocalDateTime.parse("2026-11-01T13:00:00"),
                        LocalDateTime.parse("2026-11-01T14:00:00"),
                        LocalDateTime.parse("2026-11-01T15:00:00"));
    }

    @Test
    void buildsUtcBoundedCreateStatement() {
        assertThat(DataComplianceService.createPartitionDdl(LocalDateTime.parse("2026-12-01T12:34:56")))
                .isEqualTo("CREATE TABLE IF NOT EXISTS telemetry_events_y2026m12d01h12 "
                        + "PARTITION OF telemetry_events FOR VALUES FROM ('2026-12-01 12:00:00+00') "
                        + "TO ('2026-12-01 13:00:00+00')");
    }

    @Test
    void jobRunsOutboxPurgeEvenWhenTelemetryRetentionFails() {
        OutboxEventRepository outbox = mock(OutboxEventRepository.class);
        AnomalyAlertOutboxRepository alerts = mock(AnomalyAlertOutboxRepository.class);
        when(alerts.deletePublishedBefore(any(Instant.class))).thenReturn(Mono.just(1));
        when(outbox.deleteProcessedOldEvents(any(Instant.class))).thenReturn(Mono.just(2));

        StepVerifier.create(new DataComplianceService(outbox, alerts, null).executeDataPurge())
                .verifyComplete();

        verify(outbox).deleteProcessedOldEvents(any(Instant.class));
        verify(alerts).deletePublishedBefore(any(Instant.class));
    }
}
