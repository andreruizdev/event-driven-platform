package com.eventdrivenmicroservices.platform.infrastructure.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Gauge;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicReference;

@Component
public class AnomalyMetrics {

    private final Counter anomalyCounter;
    private final AtomicReference<Double> latestZScore = new AtomicReference<>(0.0);

    public AnomalyMetrics(MeterRegistry registry) {
        this.anomalyCounter = Counter.builder("eventdrivenmicroservices.anomalies.detected.total")
                .description("Total number of streaming anomalies detected across transactions and telemetry")
                .tag("service", "platform-engine")
                .register(registry);

        Gauge.builder("eventdrivenmicroservices.anomaly.latest.zscore", latestZScore, AtomicReference::get)
                .description("Latest telemetry streaming Z-Score calculation")
                .tag("service", "platform-engine")
                .register(registry);
    }

    public void recordAnomaly(String type) {
        anomalyCounter.increment();
    }

    public void updateZScore(double zScore) {
        latestZScore.set(zScore);
    }
}
