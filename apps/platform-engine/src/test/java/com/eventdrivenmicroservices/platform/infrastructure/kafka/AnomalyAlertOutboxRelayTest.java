package com.eventdrivenmicroservices.platform.infrastructure.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AnomalyAlertOutboxRelayTest {

    private static final UUID EVENT_ID = UUID.fromString("11111111-1111-4111-8111-111111111101");
    private static final String TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    private AnomalyAlertOutboxRepository repository;
    private KafkaTemplate<String, String> kafkaTemplate;
    private AnomalyAlertOutboxRelay relay;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        repository = mock(AnomalyAlertOutboxRepository.class);
        kafkaTemplate = mock(KafkaTemplate.class);
        relay = new AnomalyAlertOutboxRelay(repository, kafkaTemplate);
    }

    @Test
    @SuppressWarnings("unchecked")
    void publishesAlertAndMarksIntentOnlyAfterKafkaAcknowledges() {
        AnomalyAlertOutboxEvent event = event();
        when(repository.claimNext(any(Instant.class), any(Instant.class))).thenReturn(Mono.just(event));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        when(repository.markPublished(eq(EVENT_ID), any(Instant.class))).thenReturn(Mono.just(1));

        relay.publishNextAlert();

        var recordCaptor = org.mockito.ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(recordCaptor.capture());
        ProducerRecord<?, ?> record = recordCaptor.getValue();
        assertEquals("eventdrivenmicroservices-anomaly-alerts", record.topic());
        assertEquals("aggregate-1", record.key());
        assertEquals(TRACEPARENT, new String(record.headers().lastHeader("traceparent").value()));
        verify(repository).markPublished(eq(EVENT_ID), any(Instant.class));
        verify(repository, never()).recordFailure(any(UUID.class), any(), any(Instant.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void recordsRetryAndLeavesIntentUnpublishedWhenKafkaFails() {
        AnomalyAlertOutboxEvent event = event();
        CompletableFuture<Object> failedSend = new CompletableFuture<>();
        failedSend.completeExceptionally(new IllegalStateException("broker unavailable"));
        when(repository.claimNext(any(Instant.class), any(Instant.class))).thenReturn(Mono.just(event));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failedSend);
        when(repository.recordFailure(eq(EVENT_ID), any(), any(Instant.class))).thenReturn(Mono.just(1));

        relay.publishNextAlert();

        verify(repository).recordFailure(eq(EVENT_ID), eq("broker unavailable"), any(Instant.class));
        verify(repository, never()).markPublished(any(UUID.class), any(Instant.class));
    }

    private AnomalyAlertOutboxEvent event() {
        return new AnomalyAlertOutboxEvent(
                EVENT_ID,
                "aggregate-1",
                "{\"eventId\":\"11111111-1111-4111-8111-111111111101\"}",
                TRACEPARENT,
                Instant.now(),
                null,
                0,
                null,
                null);
    }
}