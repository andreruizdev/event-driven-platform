package com.eventdrivenmicroservices.platform.infrastructure.kafka;

import static com.eventdrivenmicroservices.platform.config.LogSanitizer.safe;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.EventEnvelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Slf4j
public class AnomalyEventListener {

    private static final String DEAD_LETTER_TOPIC = "eventdrivenmicroservices-outbox-events.DLT";
    private static final Set<String> ANOMALY_EVENT_TYPES = Set.of(
            "TransactionAnomalyDetected",
            "TelemetryAnomalyDetected",
            "ANOMALY_VELOCITY_SPIKE");
    private static final Duration DATABASE_TIMEOUT = Duration.ofSeconds(10);
    private static final long DEAD_LETTER_SEND_TIMEOUT_SECONDS = 30;

    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final AnomalyAlertOutboxRepository alertOutboxRepository;

    public AnomalyEventListener(ObjectMapper objectMapper,
                                KafkaTemplate<String, String> kafkaTemplate,
                                AnomalyAlertOutboxRepository alertOutboxRepository) {
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
        this.alertOutboxRepository = alertOutboxRepository;
    }

    @KafkaListener(topics = "eventdrivenmicroservices-outbox-events", groupId = "eventdrivenmicroservices-anomaly-cep-group")
    public void consumeOutboxEvent(ConsumerRecord<String, String> record) throws Exception {
        EventEnvelope envelope;
        try {
            envelope = objectMapper.readValue(record.value(), EventEnvelope.class);
            validateEnvelope(envelope);
        } catch (JsonProcessingException | IllegalArgumentException exception) {
            publishDeadLetter(record, exception);
            return;
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

    private void validateEnvelope(EventEnvelope envelope) {
        if (envelope == null || envelope.eventVersion() != 1
                || envelope.eventId() == null
                || envelope.eventType() == null || envelope.eventType().isBlank()
                || envelope.aggregateId() == null || envelope.aggregateId().isBlank()
                || envelope.occurredAt() == null) {
            throw new IllegalArgumentException("Invalid or unsupported event envelope");
        }
    }

    private void publishDeadLetter(ConsumerRecord<String, String> record, Exception cause)
            throws ExecutionException, TimeoutException {
        ProducerRecord<String, String> deadLetter = new ProducerRecord<>(
                DEAD_LETTER_TOPIC, record.key(), record.value());
        record.headers().forEach(header -> deadLetter.headers().add(header.key(), header.value()));
        addHeader(deadLetter, "x-original-topic", record.topic());
        addHeader(deadLetter, "x-original-partition", Integer.toString(record.partition()));
        addHeader(deadLetter, "x-original-offset", Long.toString(record.offset()));
        addHeader(deadLetter, "x-exception-type", cause.getClass().getName());
        String message = cause instanceof JsonProcessingException
            ? "Malformed JSON event envelope"
            : cause.getMessage();
        if (message != null && message.length() > 500) {
            message = message.substring(0, 500);
        }
        if (message != null) {
            addHeader(deadLetter, "x-exception-message", message);
        }

        try {
            kafkaTemplate.send(deadLetter).get(DEAD_LETTER_SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing invalid event to DLT", exception);
        }
        log.error("Routed invalid event from {}-{}@{} to {}: {}",
            record.topic(), record.partition(), record.offset(), DEAD_LETTER_TOPIC, safe(message));
    }

    private void addHeader(ProducerRecord<String, String> record, String name, String value) {
        record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
    }
}
