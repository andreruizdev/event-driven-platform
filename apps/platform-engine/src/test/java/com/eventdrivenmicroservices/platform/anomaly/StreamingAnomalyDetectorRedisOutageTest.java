package com.eventdrivenmicroservices.platform.anomaly;

import com.eventdrivenmicroservices.platform.infrastructure.observability.AnomalyMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveZSetOperations;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;

import static org.mockito.Mockito.mock;

/**
 * A Redis outage must degrade to the per-process window, not fail ingestion. The fallback state is local to
 * each replica, so detection during an outage is per-replica rather than global.
 */
class StreamingAnomalyDetectorRedisOutageTest {

    private StreamingAnomalyDetector detector;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        org.mockito.stubbing.Answer<Object> outage = invocation -> reactor.core.publisher.Flux.class.isAssignableFrom(invocation.getMethod().getReturnType())
                ? reactor.core.publisher.Flux.error(new IllegalStateException("redis connection refused"))
                : Mono.class.isAssignableFrom(invocation.getMethod().getReturnType())
                ? Mono.error(new IllegalStateException("redis connection refused"))
                : null;
        ReactiveRedisTemplate<String, String> redis = mock(ReactiveRedisTemplate.class, outage);
        ReactiveZSetOperations<String, String> zset = mock(ReactiveZSetOperations.class, outage);
        org.mockito.Mockito.doReturn(zset).when(redis).opsForZSet();
        org.mockito.Mockito.doReturn(mock(org.springframework.data.redis.core.ReactiveListOperations.class, outage)).when(redis).opsForList();
        detector = new StreamingAnomalyDetector(new AnomalyMetrics(new SimpleMeterRegistry()), redis);
    }

    @Test
    void velocitySpikeIsStillDetectedWhenRedisIsDown() {
        StepVerifier.create(detector.evaluateTransactionVelocity("acct-outage-1", new BigDecimal("15000.00")))
                .expectNextMatches(score -> score.isAnomaly() && "FINANCIAL_VELOCITY_SPIKE".equals(score.getAnomalyType()))
                .verifyComplete();
    }

    @Test
    void velocityBurstAccumulatesInFallbackWindowWhenRedisIsDown() {
        for (int i = 0; i < 4; i++) {
            StepVerifier.create(detector.evaluateTransactionVelocity("acct-outage-2", new BigDecimal("50.00")))
                    .expectNextMatches(score -> !score.isAnomaly())
                    .verifyComplete();
        }
        StepVerifier.create(detector.evaluateTransactionVelocity("acct-outage-2", new BigDecimal("50.00")))
                .expectNextMatches(AnomalyScore::isAnomaly)
                .verifyComplete();
    }

    @Test
    void telemetryOutlierIsStillDetectedWhenRedisIsDown() {
        for (int i = 0; i < 19; i++) {
            StepVerifier.create(detector.evaluateTelemetryZScore("device-outage", 50.0 + (i % 2)))
                    .expectNextCount(1)
                    .verifyComplete();
        }
        StepVerifier.create(detector.evaluateTelemetryZScore("device-outage", 500.0))
                .expectNextMatches(score -> score.isAnomaly())
                .verifyComplete();
    }
}
