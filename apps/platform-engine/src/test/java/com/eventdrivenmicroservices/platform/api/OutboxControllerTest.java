package com.eventdrivenmicroservices.platform.api;

import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEvent;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEventRepository;
import com.eventdrivenmicroservices.platform.infrastructure.kafka.AnomalyAlertOutboxEvent;
import com.eventdrivenmicroservices.platform.infrastructure.kafka.AnomalyAlertOutboxRepository;
import com.eventdrivenmicroservices.platform.domain.LedgerEvent;
import com.eventdrivenmicroservices.platform.infrastructure.postgres.LedgerEventRepository;
import com.eventdrivenmicroservices.platform.security.SecurityConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;

import java.time.Instant;
import java.util.UUID;
import java.time.OffsetDateTime;

import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;

@WebFluxTest(OutboxController.class)
@Import(SecurityConfig.class)
@TestPropertySource(properties = "spring.security.user.password=test")
class OutboxControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockBean
    private OutboxEventRepository outboxEventRepository;

    @MockBean
    private AnomalyAlertOutboxRepository alertOutboxRepository;

    @MockBean
    private LedgerEventRepository ledgerEventRepository;

    @Test
    void returnsPendingStatus() {
        Instant createdAt = Instant.now().minusSeconds(30);
        OutboxEvent oldest = new OutboxEvent();
        oldest.setCreatedAt(createdAt);

        when(outboxEventRepository.countByProcessedFalse()).thenReturn(Mono.just(2L));
        when(outboxEventRepository.findFirstByProcessedFalseOrderByCreatedAtAsc()).thenReturn(Mono.just(oldest));

        webTestClient.get()
            .uri("/api/v1/outbox/status")
            .headers(headers -> headers.setBasicAuth("admin", "test"))
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.pendingCount").isEqualTo(2)
                .jsonPath("$.oldestCreatedAt").isNotEmpty()
                .jsonPath("$.oldestAgeSeconds").value(age -> org.junit.jupiter.api.Assertions.assertTrue(((Number) age).longValue() > 0L));
    }

    @Test
    void returnsRecentEventSummariesWithoutPayloads() {
        OutboxEvent event = new OutboxEvent();
        event.setId(UUID.randomUUID());
        event.setEventType("TransactionAnomalyDetected");
        event.setAggregateType("FinancialTransaction");
        event.setCreatedAt(Instant.now());
        event.setProcessedAt(Instant.now());
        event.setCorrelationId("correlation-01");
        event.setTraceparent("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01");
        event.setPayload("sensitive-payload");
        event.setRetryCount(1);
        event.setLastError("sensitive error");
        when(outboxEventRepository.findTop20ByOrderByCreatedAtDesc()).thenReturn(Flux.just(event));

        webTestClient.get()
                .uri("/api/v1/outbox/recent")
                .headers(headers -> headers.setBasicAuth("admin", "test"))
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$[0].eventType").isEqualTo("TransactionAnomalyDetected")
                .jsonPath("$[0].retryCount").isEqualTo(1)
                .jsonPath("$[0].correlationId").isEqualTo("correlation-01")
                .jsonPath("$[0].traceId").isEqualTo("0123456789abcdef0123456789abcdef")
                .jsonPath("$[0].lastAttemptFailed").isEqualTo(true)
                .jsonPath("$[0].lastError").doesNotExist()
                .jsonPath("$[0].payload").doesNotExist();
    }

    @Test
    void returnsSanitizedWorkflowTimelineByCorrelationId() {
        UUID eventId = UUID.randomUUID();
        OutboxEvent event = new OutboxEvent();
        event.setId(eventId);
        event.setEventType("TransactionAnomalyDetected");
        event.setAggregateType("FinancialTransaction");
        event.setCorrelationId("correlation-02");
        event.setCreatedAt(Instant.now());
        event.setProcessed(true);
        event.setPayload("private business payload");
        event.setLastError("private error detail");
        when(outboxEventRepository.findTop20ByCorrelationIdOrderByCreatedAtDesc("correlation-02"))
                .thenReturn(Flux.just(event));
        when(alertOutboxRepository.findByEventId(eventId)).thenReturn(Mono.just(
                new AnomalyAlertOutboxEvent(eventId, "aggregate", "private envelope", "traceparent",
                        Instant.now(), Instant.now(), 0, "private alert error", null)));
        when(ledgerEventRepository.findFirstByOutboxEventId(eventId)).thenReturn(Mono.just(
                LedgerEvent.builder().id(UUID.randomUUID()).createdAt(OffsetDateTime.now())
                        .payload("private ledger payload").build()));

        webTestClient.get()
                .uri("/api/v1/outbox/timeline?correlationId=correlation-02")
                .headers(headers -> headers.setBasicAuth("admin", "test"))
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.lookupType").isEqualTo("correlationId")
                .jsonPath("$.events[0].eventId").isEqualTo(eventId.toString())
                .jsonPath("$.events[0].steps[0].name").isEqualTo("accepted")
                .jsonPath("$.events[0].steps[4].name").isEqualTo("anomaly-alert")
                .jsonPath("$.events[0].steps[4].status").isEqualTo("complete")
                .jsonPath("$.events[0].steps[5].status").isEqualTo("complete")
                .jsonPath("$.events[0].payload").doesNotExist()
                .jsonPath("$.events[0].steps[4].detail").value(value ->
                        org.junit.jupiter.api.Assertions.assertFalse(value.toString().contains("private")));
    }

    @Test
    void rejectsTimelineRequestsWithoutExactlyOneLookupKey() {
        webTestClient.get()
                .uri("/api/v1/outbox/timeline")
                .headers(headers -> headers.setBasicAuth("admin", "test"))
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void returnsPendingTimelineByEventIdWithoutBusinessPayload() {
        UUID eventId = UUID.randomUUID();
        OutboxEvent event = new OutboxEvent();
        event.setId(eventId);
        event.setEventType("TransactionCreated");
        event.setAggregateType("FinancialTransaction");
        event.setCreatedAt(Instant.now());
        event.setPayload("private business payload");
        when(outboxEventRepository.findById(eventId)).thenReturn(Mono.just(event));
        when(alertOutboxRepository.findByEventId(eventId)).thenReturn(Mono.empty());
        when(ledgerEventRepository.findFirstByOutboxEventId(eventId)).thenReturn(Mono.empty());

        webTestClient.get()
                .uri("/api/v1/outbox/timeline?eventId=" + eventId)
                .headers(headers -> headers.setBasicAuth("admin", "test"))
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.lookupType").isEqualTo("eventId")
                .jsonPath("$.events[0].steps[2].status").isEqualTo("pending")
                .jsonPath("$.events[0].steps[4].status").isEqualTo("not-applicable")
                .jsonPath("$.events[0].steps[5].status").isEqualTo("not-applicable")
                .jsonPath("$.events[0].payload").doesNotExist()
                .jsonPath("$.events[0].traceparent").doesNotExist();
    }
}
