package com.eventdrivenmicroservices.platform.api;

import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEvent;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEventRepository;
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

import static org.mockito.Mockito.when;

@WebFluxTest(OutboxController.class)
@Import(SecurityConfig.class)
@TestPropertySource(properties = "spring.security.user.password=test")
class OutboxControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockBean
    private OutboxEventRepository outboxEventRepository;

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
        event.setPayload("sensitive-payload");
        event.setRetryCount(1);
        when(outboxEventRepository.findTop20ByOrderByCreatedAtDesc()).thenReturn(Flux.just(event));

        webTestClient.get()
                .uri("/api/v1/outbox/recent")
                .headers(headers -> headers.setBasicAuth("admin", "test"))
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$[0].eventType").isEqualTo("TransactionAnomalyDetected")
                .jsonPath("$[0].retryCount").isEqualTo(1)
                .jsonPath("$[0].payload").doesNotExist();
    }
}
