package com.eventdrivenmicroservices.platform.anomaly;

import com.eventdrivenmicroservices.platform.infrastructure.observability.AnomalyMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

class StreamingAnomalyDetectorTest {

    private StreamingAnomalyDetector detector;

    @BeforeEach
    void setUp() {
        AnomalyMetrics metrics = new AnomalyMetrics(new SimpleMeterRegistry());
        detector = new StreamingAnomalyDetector(metrics);
    }

    @Test
    void testTransactionVelocity_NormalVolume_ReturnsNormal() {
        StepVerifier.create(detector.evaluateTransactionVelocity("merchant-1", new BigDecimal("100.00")))
                .expectNextMatches(score -> !score.isAnomaly())
                .verifyComplete();
    }

    @Test
    void testTransactionVelocity_HighAmountSpike_FlagsAnomaly() {
        StepVerifier.create(detector.evaluateTransactionVelocity("merchant-2", new BigDecimal("15000.00")))
                .expectNextMatches(score -> score.isAnomaly() && "FINANCIAL_VELOCITY_SPIKE".equals(score.getAnomalyType()))
                .verifyComplete();
    }

    @Test
    void testTransactionVelocity_HighFrequencyBurst_FlagsAnomaly() {
        for (int i = 0; i < 4; i++) {
            detector.evaluateTransactionVelocity("merchant-3", new BigDecimal("50.00")).block();
        }

        StepVerifier.create(detector.evaluateTransactionVelocity("merchant-3", new BigDecimal("50.00")))
                .expectNextMatches(score -> score.isAnomaly() && "FINANCIAL_VELOCITY_SPIKE".equals(score.getAnomalyType()))
                .verifyComplete();
    }

    @Test
    void testTelemetryZScore_ConsistentBaseline_ReturnsNormal() {
        for (int i = 0; i < 15; i++) {
            detector.evaluateTelemetryZScore("device-100", 50.0 + (i % 3) * 0.2).block();
        }

        StepVerifier.create(detector.evaluateTelemetryZScore("device-100", 50.3))
                .expectNextMatches(score -> !score.isAnomaly())
                .verifyComplete();
    }

    @Test
    void testTelemetryZScore_MassiveOutlierSpike_FlagsAnomaly() {
        // Feed baseline readings
        for (int i = 0; i < 15; i++) {
            detector.evaluateTelemetryZScore("device-200", 10.0 + (i % 2)).block();
        }

        // Feed extreme outlier (300.0 vs baseline ~10.0)
        StepVerifier.create(detector.evaluateTelemetryZScore("device-200", 300.0))
                .expectNextMatches(score -> score.isAnomaly() && "TELEMETRY_Z_SCORE_OUTLIER".equals(score.getAnomalyType()))
                .verifyComplete();
    }
}
