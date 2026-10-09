package com.eventdrivenmicroservices.platform.infrastructure.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class AnomalyAlertOutboxRelay {

    private static final String ALERT_TOPIC = "eventdrivenmicroservices-anomaly-alerts";
    private static final Duration LOCK_DURATION = Duration.ofMinutes(2);
    private static final Duration DATABASE_TIMEOUT = Duration.ofSeconds(10);

    private final AnomalyAlertOutboxRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public AnomalyAlertOutboxRelay(AnomalyAlertOutboxRepository repository,
                                   KafkaTemplate<String, String> kafkaTemplate) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelayString = "${app.anomaly-alert-outbox.poll-interval-ms:1000}")
    public void publishNextAlert() {
        Instant now = Instant.now();
        repository.claimNext(now, now.plus(LOCK_DURATION))
                .flatMap(this::publish)
                .doOnError(error -> log.error("Failed to publish anomaly alert outbox event", error))
                .subscribe();
    }

    private Mono<Void> publish(AnomalyAlertOutboxEvent event) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                ALERT_TOPIC, event.aggregateId(), event.envelope());
        if (event.traceparent() != null && !event.traceparent().isBlank()) {
            record.headers().add("traceparent", event.traceparent().getBytes(StandardCharsets.UTF_8));
        }

        return Mono.fromFuture(kafkaTemplate.send(record))
                .then(Mono.defer(() -> repository.markPublished(event.eventId(), Instant.now())
                        .flatMap(rowsUpdated -> rowsUpdated == 1
                            ? Mono.<Void>empty()
                            : Mono.<Void>error(new IllegalStateException(
                                        "Published anomaly alert could not be marked delivered: " + event.eventId())))))
                    .onErrorResume(error -> recordFailure(event, error).then(Mono.<Void>error(error)));
    }

    private Mono<Integer> recordFailure(AnomalyAlertOutboxEvent event, Throwable error) {
        int shift = Math.min(event.retryCount(), 8);
        long delaySeconds = Math.min(1L << shift, 300L);
        String message = error.getMessage();
        if (message != null && message.length() > 500) {
            message = message.substring(0, 500);
        }
        return repository.recordFailure(
                event.eventId(), message, Instant.now().plusSeconds(delaySeconds))
                .timeout(DATABASE_TIMEOUT);
    }
}