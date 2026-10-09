package com.eventdrivenmicroservices.platform.infrastructure.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.EventEnvelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import reactor.core.publisher.Mono;

class AnomalyEventListenerTest {

    @Test
        void forwardsTraceparentToAnomalyAlert() throws Exception {
        AnomalyAlertOutboxRepository repository = mock(AnomalyAlertOutboxRepository.class);
        when(repository.insertIfAbsent(any(UUID.class), any(String.class), any(String.class),
                any(String.class), any(Instant.class))).thenReturn(Mono.just(1));
        ObjectMapper objectMapper = objectMapper();
        AnomalyEventListener listener = new AnomalyEventListener(objectMapper, repository);
        RecordHeaders headers = new RecordHeaders();
        headers.add("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
                .getBytes(StandardCharsets.UTF_8));
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "eventdrivenmicroservices-outbox-events", 0, 0L, "aggregate-1",
                envelope(objectMapper, "TransactionAnomalyDetected", "{}"));
        record.headers().add(headers.lastHeader("traceparent"));

        listener.consumeOutboxEvent(record);

        var envelopeCaptor = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(repository).insertIfAbsent(
                eq(UUID.fromString("11111111-1111-4111-8111-111111111101")),
                eq("aggregate-1"), envelopeCaptor.capture(),
                eq("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"), any(Instant.class));
        assertEquals("TransactionAnomalyDetected", objectMapper.readTree(envelopeCaptor.getValue())
                .path("eventType").asText());
    }

    @Test
    @SuppressWarnings("unchecked")
    void ignoresNonAnomalyEventTypesRegardlessOfPayloadText() throws Exception {
                AnomalyAlertOutboxRepository repository = mock(AnomalyAlertOutboxRepository.class);
                AnomalyEventListener listener = new AnomalyEventListener(objectMapper(), repository);
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "eventdrivenmicroservices-outbox-events", 0, 0L, "aggregate-1",
                envelope(objectMapper(), "TransactionCreated", "{\"note\":\"ANOMALY\"}"));

        listener.consumeOutboxEvent(record);

        verifyNoInteractions(repository);
    }

    @Test
    void propagatesDatabaseFailureSoTheInputIsNotAcknowledged() throws Exception {
        AnomalyAlertOutboxRepository repository = mock(AnomalyAlertOutboxRepository.class);
        when(repository.insertIfAbsent(any(UUID.class), any(String.class), any(String.class),
                any(String.class), any(Instant.class)))
                .thenReturn(Mono.error(new IllegalStateException("database unavailable")));
        ObjectMapper objectMapper = objectMapper();
        AnomalyEventListener listener = new AnomalyEventListener(objectMapper, repository);
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "eventdrivenmicroservices-outbox-events", 0, 0L, "aggregate-1",
                envelope(objectMapper, "TelemetryAnomalyDetected", "{}"));

        assertThrows(IllegalStateException.class, () -> listener.consumeOutboxEvent(record));
    }

    private ObjectMapper objectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    private String envelope(ObjectMapper objectMapper, String eventType, String payload) throws Exception {
        return objectMapper.writeValueAsString(new EventEnvelope(
                UUID.fromString("11111111-1111-4111-8111-111111111101"),
                eventType,
                1,
                "FinancialTransaction",
                "aggregate-1",
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                Instant.parse("2026-10-09T15:00:00Z"),
                payload));
    }
}
