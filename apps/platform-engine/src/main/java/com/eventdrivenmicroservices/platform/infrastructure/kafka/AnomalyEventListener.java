package com.eventdrivenmicroservices.platform.infrastructure.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.EventEnvelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.TimeoutException;

@Component
@Slf4j
public class AnomalyEventListener {

    private static final Set<String> ANOMALY_EVENT_TYPES = Set.of(
            "TransactionAnomalyDetected",
            "TelemetryAnomalyDetected",
            "ANOMALY_VELOCITY_SPIKE");
    private static final Duration DATABASE_TIMEOUT = Duration.ofSeconds(10);

    private final ObjectMapper objectMapper;
    private final AnomalyAlertOutboxRepository alertOutboxRepository;

    public AnomalyEventListener(ObjectMapper objectMapper,
                                AnomalyAlertOutboxRepository alertOutboxRepository) {
        this.objectMapper = objectMapper;
        this.alertOutboxRepository = alertOutboxRepository;
    }

    @KafkaListener(topics = "eventdrivenmicroservices-outbox-events", groupId = "eventdrivenmicroservices-anomaly-cep-group")
    public void consumeOutboxEvent(ConsumerRecord<String, String> record) throws Exception {
        EventEnvelope envelope = objectMapper.readValue(record.value(), EventEnvelope.class);
        if (envelope.eventVersion() != 1) {
            throw new IllegalArgumentException("Unsupported event envelope version: " + envelope.eventVersion());
        }
        if (!ANOMALY_EVENT_TYPES.contains(envelope.eventType())) {
            return;
        }

        Header traceparentHeader = record.headers().lastHeader("traceparent");
        String traceparent = traceparentHeader == null
                ? envelope.traceparent()
                : new String(traceparentHeader.value(), StandardCharsets.UTF_8);

        String value = objectMapper.writeValueAsString(envelope);
        try {
            Integer rowsInserted = alertOutboxRepository.insertIfAbsent(
                    envelope.eventId(), envelope.aggregateId(), value, traceparent, Instant.now())
                    .block(DATABASE_TIMEOUT);
            if (rowsInserted == null) {
                throw new IllegalStateException("Timed out persisting anomaly alert intent: " + envelope.eventId());
            }
            log.debug("Persisted anomaly alert intent {} (inserted={})", envelope.eventId(), rowsInserted == 1);
        } catch (RuntimeException exception) {
            throw new IllegalStateException("Failed to persist anomaly alert intent: " + envelope.eventId(), exception);
        }
    }
}
