package com.eventdrivenmicroservices.platform.infrastructure.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEventRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import reactor.core.publisher.Mono;

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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutboxCdcBridgeTest {

    private static final UUID EVENT_ID = UUID.fromString("11111111-1111-4111-8111-111111111101");
    private static final String TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    private static final String CDC_CHANGE = """
            {"op":"c","after":{"id":"11111111-1111-4111-8111-111111111101","aggregate_type":"LoanApplication","aggregate_id":"loan-1","event_type":"ApplicationSubmitted","payload":"{\\"amount\\":1000}","traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01","created_at":"2026-10-09T15:24:59.197946Z"}}
            """;

    private KafkaTemplate<String, String> kafkaTemplate;
    private OutboxEventRepository outboxRepository;
    private ObjectMapper objectMapper;
    private OutboxCdcBridge bridge;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        outboxRepository = mock(OutboxEventRepository.class);
        objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        bridge = new OutboxCdcBridge(objectMapper, kafkaTemplate, outboxRepository);
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(outboxRepository.markAsProcessed(eq(EVENT_ID), any(Instant.class)))
                .thenReturn(Mono.just(1));
    }

    @Test
    void forwardsInsertAsCanonicalEnvelopeAndMarksItPublished() throws Exception {
        bridge.forward(record(CDC_CHANGE));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<String, String> published = captor.getValue();
        var envelope = objectMapper.readTree(published.value());

        assertEquals("eventdrivenmicroservices-outbox-events", published.topic());
        assertEquals("loan-1", published.key());
        assertEquals(EVENT_ID.toString(), envelope.path("eventId").asText());
        assertEquals("ApplicationSubmitted", envelope.path("eventType").asText());
        assertEquals(1, envelope.path("eventVersion").asInt());
        assertEquals("{\"amount\":1000}", envelope.path("payload").asText());
        assertEquals(TRACEPARENT, new String(
                published.headers().lastHeader("traceparent").value(), StandardCharsets.UTF_8));
        verify(outboxRepository).markAsProcessed(eq(EVENT_ID), any(Instant.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void doesNotMarkEventProcessedWhenKafkaPublishFails() {
        CompletableFuture<Object> failedSend = new CompletableFuture<>();
        failedSend.completeExceptionally(new IllegalStateException("broker unavailable"));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failedSend);

        assertThrows(ExecutionException.class, () -> bridge.forward(record(CDC_CHANGE)));

        verify(outboxRepository, never()).markAsProcessed(any(UUID.class), any(Instant.class));
    }

    private ConsumerRecord<String, String> record(String value) {
        return new ConsumerRecord<>("eventdrivenmicroservices_cdc.public.outbox_events", 0, 0L, "", value);
    }
}