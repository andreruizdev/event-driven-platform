package com.eventdrivenmicroservices.platform.infrastructure.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.eventdrivenmicroservices.platform.infrastructure.observability.OutboxMetrics;
import com.eventdrivenmicroservices.platform.infrastructure.observability.TraceSupport;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.EventEnvelope;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
@ConditionalOnProperty(prefix = "app.outbox.publisher", name = "mode", havingValue = "cdc")
public class OutboxCdcBridge {

    private static final String CANONICAL_TOPIC = "eventdrivenmicroservices-outbox-events";
    private static final Duration DATABASE_TIMEOUT = Duration.ofSeconds(10);
    private static final long KAFKA_SEND_TIMEOUT_SECONDS = 30;

    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OutboxEventRepository outboxRepository;
    private final OutboxMetrics metrics;
    private final TraceSupport traceSupport;

    public OutboxCdcBridge(ObjectMapper objectMapper,
                           KafkaTemplate<String, String> kafkaTemplate,
                           OutboxEventRepository outboxRepository,
                           OutboxMetrics metrics) {
        this(objectMapper, kafkaTemplate, outboxRepository, metrics, TraceSupport.noop());
    }

    @Autowired
    public OutboxCdcBridge(ObjectMapper objectMapper,
                           KafkaTemplate<String, String> kafkaTemplate,
                           OutboxEventRepository outboxRepository,
                           OutboxMetrics metrics,
                           TraceSupport traceSupport) {
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
        this.outboxRepository = outboxRepository;
        this.metrics = metrics;
        this.traceSupport = traceSupport;
    }

    @KafkaListener(
            topics = "${app.outbox.cdc-bridge.source-topic}",
            groupId = "${app.outbox.cdc-bridge.group-id}")
    public void forward(ConsumerRecord<String, String> record) throws Exception {
        try {
            doForward(record);
        } catch (Exception e) {
            metrics.recordProcessingFailure();
            throw e;
        }
    }

    private void doForward(ConsumerRecord<String, String> record) throws Exception {
        JsonNode change = objectMapper.readTree(record.value());
        if (!"c".equals(requiredText(change, "op"))) {
            throw new IllegalArgumentException("CDC outbox stream contains a non-insert operation");
        }

        JsonNode row = change.path("after");
        UUID eventId = UUID.fromString(requiredText(row, "id"));
        String traceparent = optionalText(row, "traceparent");
        EventEnvelope envelope = new EventEnvelope(
                eventId,
                requiredText(row, "event_type"),
                1,
                requiredText(row, "aggregate_type"),
                requiredText(row, "aggregate_id"),
                traceparent,
                Instant.parse(requiredText(row, "created_at")),
                requiredText(row, "payload"));

        ProducerRecord<String, String> output = new ProducerRecord<>(
                CANONICAL_TOPIC,
                envelope.aggregateId(),
                objectMapper.writeValueAsString(envelope));
        if (traceparent != null && !traceparent.isBlank()) {
            output.headers().add("traceparent", traceparent.getBytes(StandardCharsets.UTF_8));
        }

        traceSupport.runInSpan("outbox.cdc.publish", traceparent, "outbox.event.id", eventId.toString(), () -> {
            Instant publishStarted = Instant.now();
            kafkaTemplate.send(output).get(KAFKA_SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            metrics.recordPublishDuration(Duration.between(publishStarted, Instant.now()));
            Integer rowsUpdated = outboxRepository.markAsProcessed(eventId, Instant.now())
                    .block(DATABASE_TIMEOUT);
            if (rowsUpdated == null || rowsUpdated != 1) {
                throw new IllegalStateException("Published CDC outbox event could not be marked as processed: " + eventId);
            }
        });
        metrics.recordPublishSuccess();
        log.debug("Published CDC outbox event {} to canonical topic", eventId);
    }

    private String requiredText(JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (value.isMissingNode() || value.isNull() || value.asText().isBlank()) {
            throw new IllegalArgumentException("CDC outbox event is missing required field: " + field);
        }
        return value.asText();
    }

    private String optionalText(JsonNode node, String field) {
        JsonNode value = node.path(field);
        return value.isMissingNode() || value.isNull() ? null : value.asText();
    }
}