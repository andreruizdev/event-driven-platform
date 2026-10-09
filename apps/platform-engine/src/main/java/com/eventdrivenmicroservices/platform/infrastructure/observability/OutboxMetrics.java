package com.eventdrivenmicroservices.platform.infrastructure.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class OutboxMetrics {

    private final Counter publishSuccesses;
    private final Counter processingFailures;
    private final Counter retries;
    private final Timer publishDuration;
    private final AtomicLong pendingCount = new AtomicLong();
    private final AtomicLong oldestPendingAgeSeconds = new AtomicLong();

    public OutboxMetrics(MeterRegistry registry) {
        publishSuccesses = Counter.builder("eventdrivenmicroservices.outbox.publish.success")
                .description("Outbox events acknowledged by Kafka")
                .register(registry);
        processingFailures = Counter.builder("eventdrivenmicroservices.outbox.processing.failure")
                .description("Outbox publish or processed-state persistence failures")
                .register(registry);
        retries = Counter.builder("eventdrivenmicroservices.outbox.retry")
                .description("Outbox events scheduled for retry")
                .register(registry);
        publishDuration = Timer.builder("eventdrivenmicroservices.outbox.publish.duration")
                .description("Time spent waiting for Kafka publish acknowledgement")
                .publishPercentileHistogram()
                .register(registry);

        Gauge.builder("eventdrivenmicroservices.outbox.pending", pendingCount, AtomicLong::doubleValue)
                .description("Current unprocessed outbox event count")
                .register(registry);
        Gauge.builder("eventdrivenmicroservices.outbox.oldest.pending.age.seconds", oldestPendingAgeSeconds, AtomicLong::doubleValue)
                .description("Age in seconds of the oldest unprocessed outbox event")
                .register(registry);
    }

    public void recordPublishSuccess() {
        publishSuccesses.increment();
    }

    public void recordProcessingFailure() {
        processingFailures.increment();
        retries.increment();
    }

    public void recordPublishDuration(Duration duration) {
        publishDuration.record(duration);
    }

    public void updateBacklog(long pending, long oldestAgeSeconds) {
        pendingCount.set(pending);
        this.oldestPendingAgeSeconds.set(oldestAgeSeconds);
    }
}