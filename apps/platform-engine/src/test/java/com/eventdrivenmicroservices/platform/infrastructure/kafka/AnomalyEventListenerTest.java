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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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
        @SuppressWarnings("unchecked")
        void forwardsTraceparentToAnomalyAlert() throws Exception {
        AnomalyAlertOutboxRepository repository = mock(AnomalyAlertOutboxRepository.class);
                KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        when(repository.insertIfAbsent(any(UUID.class), any(String.class), any(String.class),
                any(String.class), any(Instant.class))).thenReturn(Mono.just(1));
        ObjectMapper objectMapper = objectMapper();
                AnomalyEventListener listener = new AnomalyEventListener(objectMapper, kafkaTemplate, repository);
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
    void redeliveryAfterIntentCommitKeepsTheSameEventIdempotent() throws Exception {
        AnomalyAlertOutboxRepository repository = mock(AnomalyAlertOutboxRepository.class);
        KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        when(repository.insertIfAbsent(any(UUID.class), any(String.class), any(String.class),
                any(String.class), any(Instant.class)))
                .thenReturn(Mono.just(1), Mono.just(0));
        ObjectMapper objectMapper = objectMapper();
        AnomalyEventListener listener = new AnomalyEventListener(objectMapper, kafkaTemplate, repository);
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "eventdrivenmicroservices-outbox-events", 0, 17L, "aggregate-1",
                envelope(objectMapper, "TransactionAnomalyDetected", "{}"));

        listener.consumeOutboxEvent(record);
        listener.consumeOutboxEvent(record);

        var envelopeCaptor = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(repository, org.mockito.Mockito.times(2)).insertIfAbsent(
                eq(UUID.fromString("11111111-1111-4111-8111-111111111101")),
                eq("aggregate-1"), envelopeCaptor.capture(), any(String.class), any(Instant.class));
        assertEquals(envelopeCaptor.getAllValues().get(0), envelopeCaptor.getAllValues().get(1));
    }

    @Test
    @SuppressWarnings("unchecked")
    void ignoresNonAnomalyEventTypesRegardlessOfPayloadText() throws Exception {
                AnomalyAlertOutboxRepository repository = mock(AnomalyAlertOutboxRepository.class);
                KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
                AnomalyEventListener listener = new AnomalyEventListener(objectMapper(), kafkaTemplate, repository);
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "eventdrivenmicroservices-outbox-events", 0, 0L, "aggregate-1",
                envelope(objectMapper(), "TransactionCreated", "{\"note\":\"ANOMALY\"}"));

        listener.consumeOutboxEvent(record);

        verifyNoInteractions(repository);
    }

    @Test
        @SuppressWarnings("unchecked")
    void propagatesDatabaseFailureSoTheInputIsNotAcknowledged() throws Exception {
        AnomalyAlertOutboxRepository repository = mock(AnomalyAlertOutboxRepository.class);
                KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        when(repository.insertIfAbsent(any(UUID.class), any(String.class), any(String.class),
                any(String.class), any(Instant.class)))
                .thenReturn(Mono.error(new IllegalStateException("database unavailable")));
        ObjectMapper objectMapper = objectMapper();
        AnomalyEventListener listener = new AnomalyEventListener(objectMapper, kafkaTemplate, repository);
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "eventdrivenmicroservices-outbox-events", 0, 0L, "aggregate-1",
                envelope(objectMapper, "TelemetryAnomalyDetected", "{}"));

        assertThrows(IllegalStateException.class, () -> listener.consumeOutboxEvent(record));
    }

    @Test
    @SuppressWarnings("unchecked")
    void deadLettersMalformedEnvelopeWithOriginalCoordinatesAfterKafkaAcknowledges() throws Exception {
        AnomalyAlertOutboxRepository repository = mock(AnomalyAlertOutboxRepository.class);
        KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        AnomalyEventListener listener = new AnomalyEventListener(objectMapper(), kafkaTemplate, repository);
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "eventdrivenmicroservices-outbox-events", 3, 17L, "aggregate-1", "not-json-sensitive-value");

        listener.consumeOutboxEvent(record);

        @SuppressWarnings("unchecked")
        var captor = org.mockito.ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<String, String> deadLetter = captor.getValue();
        assertEquals("eventdrivenmicroservices-outbox-events.DLT", deadLetter.topic());
        assertEquals("aggregate-1", deadLetter.key());
        assertEquals("not-json-sensitive-value", deadLetter.value());
        assertEquals("eventdrivenmicroservices-outbox-events",
                new String(deadLetter.headers().lastHeader("x-original-topic").value(), StandardCharsets.UTF_8));
        assertEquals("3", new String(deadLetter.headers().lastHeader("x-original-partition").value(), StandardCharsets.UTF_8));
        assertEquals("17", new String(deadLetter.headers().lastHeader("x-original-offset").value(), StandardCharsets.UTF_8));
        assertEquals("Malformed JSON event envelope", new String(
                deadLetter.headers().lastHeader("x-exception-message").value(), StandardCharsets.UTF_8));
        verifyNoInteractions(repository);
    }

    @Test
    @SuppressWarnings("unchecked")
    void propagatesDeadLetterPublishFailureSoTheInputIsNotAcknowledged() throws Exception {
        AnomalyAlertOutboxRepository repository = mock(AnomalyAlertOutboxRepository.class);
        KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        CompletableFuture<Object> failedSend = new CompletableFuture<>();
        failedSend.completeExceptionally(new IllegalStateException("broker unavailable"));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failedSend);
        AnomalyEventListener listener = new AnomalyEventListener(objectMapper(), kafkaTemplate, repository);

        assertThrows(ExecutionException.class, () -> listener.consumeOutboxEvent(
                new ConsumerRecord<>("eventdrivenmicroservices-outbox-events", 0, 0L, "key", "not-json")));
        verifyNoInteractions(repository);
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
