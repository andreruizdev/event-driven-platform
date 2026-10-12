package com.eventdrivenmicroservices.platform.infrastructure.outbox;

import static com.eventdrivenmicroservices.platform.config.LogSanitizer.safe;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.eventdrivenmicroservices.platform.infrastructure.observability.OutboxMetrics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Service
@Slf4j
@ConditionalOnProperty(prefix = "app.outbox.publisher", name = "mode", havingValue = "poller")
public class OutboxProcessorService {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final OutboxMetrics metrics;
    private final String instanceId = "instance-" + UUID.randomUUID();

    private static final Duration LOCK_DURATION = Duration.ofSeconds(30);
    private static final Duration MAX_RETRY_DELAY = Duration.ofMinutes(5);

    public OutboxProcessorService(OutboxEventRepository outboxEventRepository,
                                  @Autowired(required = false) KafkaTemplate<String, String> kafkaTemplate,
                                  ObjectMapper objectMapper,
                                  OutboxMetrics metrics) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelay = 5000)
    public void processOutboxEvents() {
        Instant now = Instant.now();
        Instant lockUntil = now.plus(LOCK_DURATION);

        refreshBacklog()
            .thenMany(outboxEventRepository.findPendingForClaim(now, 50))
                .concatMap(event ->
                        outboxEventRepository.claimLock(event.getId(), instanceId, lockUntil, now)
                                .flatMap(rowsUpdated -> {
                                    if (rowsUpdated > 0) {
                                        return publishEvent(event)
                                                .flatMap(this::markAsProcessed)
                                                .doOnSuccess(ev -> log.debug("Successfully processed outbox event: {}", ev.getId()))
                                                .onErrorResume(error -> recordFailure(event, error));
                                    }
                                    log.debug("Event {} was claimed by another instance", event.getId());
                                    return Mono.empty();
                                })
                )
                .doOnError(error -> log.error("Failed to refresh or process the outbox batch", error))
                .subscribe();
    }

            private Mono<Void> refreshBacklog() {
            return outboxEventRepository.countByProcessedFalse()
                .defaultIfEmpty(0L)
                .flatMap(pendingCount -> outboxEventRepository.findFirstByProcessedFalseOrderByCreatedAtAsc()
                    .map(oldest -> Math.max(0L, Instant.now().getEpochSecond() - oldest.getCreatedAt().getEpochSecond()))
                    .defaultIfEmpty(0L)
                    .doOnNext(oldestAge -> metrics.updateBacklog(pendingCount, oldestAge)))
                .then();
            }

    private Mono<OutboxEvent> recordFailure(OutboxEvent event, Throwable error) {
        log.error("Failed to process outbox event: {}", event.getId(), error);
        metrics.recordProcessingFailure();
        event.setRetryCount(event.getRetryCount() + 1);
        long delaySeconds = Math.min(5L << Math.min(event.getRetryCount() - 1, 6), MAX_RETRY_DELAY.toSeconds());
        String errorMsg = error.getMessage();
        if (errorMsg != null && errorMsg.length() > 500) {
            errorMsg = errorMsg.substring(0, 500);
        }
        event.setLastError(errorMsg);
        event.setLockedUntil(Instant.now().plusSeconds(delaySeconds));
        return outboxEventRepository.save(event);
    }

    private Mono<OutboxEvent> publishEvent(OutboxEvent event) {
        if (kafkaTemplate != null) {
            log.info("Publishing outbox event to Kafka -> Topic: eventdrivenmicroservices-outbox-events, Type: {}, AggregateId: {}",
                    safe(event.getEventType()), safe(event.getAggregateId()));
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    "eventdrivenmicroservices-outbox-events", event.getAggregateId(), envelope(event));
            if (event.getTraceparent() != null && !event.getTraceparent().isBlank()) {
                record.headers().add("traceparent", event.getTraceparent().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
                Instant publishStarted = Instant.now();
                return Mono.fromFuture(kafkaTemplate.send(record))
                    .doOnSuccess(result -> metrics.recordPublishSuccess())
                    .doFinally(signal -> metrics.recordPublishDuration(Duration.between(publishStarted, Instant.now())))
                    .thenReturn(event)
                    .doOnError(err -> log.error("Failed to publish outbox event to Kafka: {}", event.getId(), err));
        } else {
            log.info("[Local Dev / Test] KafkaTemplate not configured. Simulating event publish -> Type: {}, AggregateId: {}",
                    safe(event.getEventType()), safe(event.getAggregateId()));
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
        event.setLockedUntil(null);
        return outboxEventRepository.save(event);
    }
}

