package com.eventdrivenmicroservices.platform.api;

import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEvent;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEventRepository;
import com.eventdrivenmicroservices.platform.infrastructure.kafka.AnomalyAlertOutboxEvent;
import com.eventdrivenmicroservices.platform.infrastructure.kafka.AnomalyAlertOutboxRepository;
import com.eventdrivenmicroservices.platform.domain.LedgerEvent;
import com.eventdrivenmicroservices.platform.infrastructure.postgres.LedgerEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/outbox")
@RequiredArgsConstructor
public class OutboxController {

    private final OutboxEventRepository outboxEventRepository;
    private final AnomalyAlertOutboxRepository alertOutboxRepository;
    private final LedgerEventRepository ledgerEventRepository;

    @GetMapping("/status")
    public Mono<OutboxStatus> status() {
        return outboxEventRepository.countByProcessedFalse()
                .defaultIfEmpty(0L)
                .flatMap(pendingCount -> outboxEventRepository.findFirstByProcessedFalseOrderByCreatedAtAsc()
                        .map(oldest -> new OutboxStatus(pendingCount, oldest.getCreatedAt(), ageInSeconds(oldest.getCreatedAt())))
                        .defaultIfEmpty(new OutboxStatus(pendingCount, null, 0L)));
    }

    @GetMapping("/recent")
    public Flux<OutboxEventSummary> recentEvents() {
        return outboxEventRepository.findTop20ByOrderByCreatedAtDesc()
                .map(event -> new OutboxEventSummary(
                        event.getId(),
                        event.getEventType(),
                        event.getAggregateType(),
                        event.getCreatedAt(),
                        event.getProcessedAt(),
                        event.isProcessed(),
                        event.getRetryCount(),
                        event.getLastError() != null,
                        traceId(event.getTraceparent()),
                        event.getCorrelationId()));
    }

    @GetMapping("/timeline")
    public Mono<WorkflowTimeline> workflowTimeline(
            @RequestParam(required = false) UUID eventId,
            @RequestParam(required = false) String correlationId) {
        if ((eventId == null) == (correlationId == null)
                || correlationId != null && (correlationId.isBlank() || correlationId.length() > 255)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Provide exactly one eventId or correlationId (maximum 255 characters)");
        }

        Mono<List<OutboxEvent>> events = eventId != null
                ? outboxEventRepository.findById(eventId)
                        .map(List::of)
                        .defaultIfEmpty(List.of())
                : outboxEventRepository.findTop20ByCorrelationIdOrderByCreatedAtDesc(correlationId)
                        .collectList();

        return events.flatMap(matchingEvents -> Flux.fromIterable(matchingEvents)
                .flatMapSequential(this::workflowEvent)
                .collectList()
                .map(timeline -> new WorkflowTimeline(
                        eventId != null ? eventId.toString() : correlationId,
                        eventId != null ? "eventId" : "correlationId",
                        timeline)));
    }

    private Mono<WorkflowEvent> workflowEvent(OutboxEvent event) {
        Mono<Optional<AnomalyAlertOutboxEvent>> alert = alertOutboxRepository.findByEventId(event.getId())
                .map(Optional::of)
                .defaultIfEmpty(Optional.empty());
        Mono<Optional<LedgerEvent>> ledger = ledgerEventRepository.findFirstByOutboxEventId(event.getId())
                .map(Optional::of)
                .defaultIfEmpty(Optional.empty());

        return Mono.zip(alert, ledger)
                .map(result -> {
                    AnomalyAlertOutboxEvent alertEvent = result.getT1().orElse(null);
                    LedgerEvent ledgerEvent = result.getT2().orElse(null);
                    boolean anomaly = event.getEventType() != null
                            && event.getEventType().toLowerCase(java.util.Locale.ROOT).contains("anomaly");
                    boolean expectsLedger = anomaly || "LoanApplication".equals(event.getAggregateType());
                    String publishedStatus = event.isProcessed() ? "complete"
                            : event.getLastError() != null ? "failed" : "pending";
                    String alertStatus = !anomaly ? "not-applicable"
                            : alertEvent == null ? "pending"
                            : alertEvent.publishedAt() != null ? "complete"
                            : alertEvent.lastError() != null ? "failed"
                            : alertEvent.retryCount() > 0 ? "retrying" : "pending";
                    List<WorkflowStep> steps = List.of(
                            new WorkflowStep("accepted", "complete", event.getCreatedAt(), "Request accepted"),
                            new WorkflowStep("persisted", "complete", event.getCreatedAt(),
                                    "Domain record and outbox intent persisted"),
                            new WorkflowStep("published", publishedStatus,
                                    event.getProcessedAt(), event.isProcessed() ? "Published to Kafka"
                                            : event.getLastError() != null ? "Last publish attempt failed; retry pending"
                                                    : "Waiting for Kafka acknowledgement"),
                            new WorkflowStep("retries", event.getRetryCount() > 0 ? "complete" : "not-applicable",
                                    null, event.getRetryCount() + " publish retries"),
                            new WorkflowStep("anomaly-alert", alertStatus,
                                    alertEvent == null ? null
                                            : alertEvent.publishedAt() != null ? alertEvent.publishedAt() : alertEvent.createdAt(),
                                    !anomaly ? "No anomaly alert expected"
                                            : alertEvent == null ? "Waiting for consumer alert intent"
                                            : alertEvent.publishedAt() != null ? "Alert published"
                                                    : alertEvent.lastError() != null ? "Alert publish failed; retry scheduled"
                                                            : alertEvent.retryCount() + " alert publish retries"),
                            new WorkflowStep("ledger",
                                    ledgerEvent != null ? "complete" : expectsLedger ? "pending" : "not-applicable",
                                    ledgerEvent == null ? null : ledgerEvent.getCreatedAt() == null
                                            ? null : ledgerEvent.getCreatedAt().toInstant(),
                                    ledgerEvent != null ? "Ledger entry recorded"
                                            : expectsLedger ? "Waiting for ledger entry" : "No ledger entry expected"),
                            new WorkflowStep("trace", traceId(event.getTraceparent()) != null ? "available" : "unavailable",
                                    null, traceId(event.getTraceparent())));

                    return new WorkflowEvent(
                            event.getId(),
                            event.getCorrelationId(),
                            event.getEventType(),
                            event.getAggregateType(),
                            event.getCreatedAt(),
                            event.getProcessedAt(),
                            event.getRetryCount(),
                            event.getLastError() != null,
                            traceId(event.getTraceparent()),
                            ledgerEvent == null ? null : ledgerEvent.getId(),
                            steps);
                });
    }

    private String traceId(String traceparent) {
        if (traceparent == null) {
            return null;
        }
        String[] parts = traceparent.split("-");
        if (parts.length < 4 || !parts[1].matches("[0-9a-fA-F]{32}")
                || parts[1].equalsIgnoreCase("00000000000000000000000000000000")) {
            return null;
        }
        return parts[1].toLowerCase(java.util.Locale.ROOT);
    }

    private long ageInSeconds(Instant createdAt) {
        return createdAt == null ? 0L : Math.max(0L, Instant.now().getEpochSecond() - createdAt.getEpochSecond());
    }

    public record OutboxStatus(long pendingCount, Instant oldestCreatedAt, long oldestAgeSeconds) {
    }

    public record OutboxEventSummary(UUID id, String eventType, String aggregateType, Instant createdAt,
                                     Instant processedAt, boolean processed, int retryCount,
                                     boolean lastAttemptFailed, String traceId, String correlationId) {
    }

    public record WorkflowTimeline(String lookupKey, String lookupType, List<WorkflowEvent> events) {
    }

    public record WorkflowEvent(UUID eventId, String correlationId, String eventType, String aggregateType,
                                Instant createdAt, Instant processedAt, int retryCount, boolean lastAttemptFailed,
                                String traceId, UUID ledgerEventId, List<WorkflowStep> steps) {
    }

    public record WorkflowStep(String name, String status, Instant occurredAt, String detail) {
    }
}
