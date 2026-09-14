package com.eventdrivenmicroservices.platform.infrastructure.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Service
@Slf4j
public class OutboxProcessorService {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public OutboxProcessorService(OutboxEventRepository outboxEventRepository,
                                  @Autowired(required = false) KafkaTemplate<String, String> kafkaTemplate,
                                  ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Scheduled(fixedDelay = 5000)
    public void processOutboxEvents() {
        outboxEventRepository.findByProcessedFalse()
                .flatMap(this::publishEvent)
                .flatMap(this::markAsProcessed)
                .subscribe(
                        event -> log.debug("Successfully processed outbox event: {}", event.getId()),
                        error -> log.error("Error processing outbox events", error)
                );
    }

    private Mono<OutboxEvent> publishEvent(OutboxEvent event) {
        if (kafkaTemplate != null) {
            log.info("Publishing outbox event to Kafka -> Topic: eventdrivenmicroservices-outbox-events, Type: {}, AggregateId: {}",
                    event.getEventType(), event.getAggregateId());
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    "eventdrivenmicroservices-outbox-events", event.getAggregateId(), envelope(event));
            if (event.getTraceparent() != null && !event.getTraceparent().isBlank()) {
                record.headers().add("traceparent", event.getTraceparent().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
            return Mono.fromFuture(kafkaTemplate.send(record))
                    .thenReturn(event)
                    .doOnError(err -> log.error("Failed to publish outbox event to Kafka: {}", event.getId(), err));
        } else {
            log.info("[Local Dev / Test] KafkaTemplate not configured. Simulating event publish -> Type: {}, AggregateId: {}",
                    event.getEventType(), event.getAggregateId());
            return Mono.just(event);
        }
    }

    private String envelope(OutboxEvent event) {
        EventEnvelope envelope = new EventEnvelope(
                event.getId(),
                event.getEventType(),
                1,
                event.getAggregateType(),
                event.getAggregateId(),
                event.getTraceparent(),
                event.getCreatedAt(),
                event.getPayload());
        try {
            return objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Failed to serialize outbox event envelope", exception);
        }
    }

    private Mono<OutboxEvent> markAsProcessed(OutboxEvent event) {
        event.setProcessed(true);
        event.setProcessedAt(Instant.now());
        return outboxEventRepository.save(event);
    }
}

