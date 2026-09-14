package com.eventdrivenmicroservices.platform.anomaly;

import com.eventdrivenmicroservices.platform.infrastructure.observability.AnomalyMetrics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
public class StreamingAnomalyDetector {

    private final AnomalyMetrics metrics;
    private final ReactiveRedisTemplate<String, String> redisTemplate;

    // In-memory fallback tracking for standalone/local testing without Redis
    private final Map<String, Deque<TransactionTimestamp>> velocityWindows = new ConcurrentHashMap<>();
    private final Map<String, Deque<Double>> telemetryWindows = new ConcurrentHashMap<>();

    private static final Duration VELOCITY_WINDOW_DURATION = Duration.ofSeconds(5);
    private static final BigDecimal VELOCITY_AMOUNT_THRESHOLD = new BigDecimal("10000.00");
    private static final int VELOCITY_COUNT_THRESHOLD = 5;
    private static final int TELEMETRY_WINDOW_SIZE = 20;
    private static final double Z_SCORE_THRESHOLD = 3.0;

    public StreamingAnomalyDetector(AnomalyMetrics metrics) {
        this(metrics, null);
    }

    @Autowired
    public StreamingAnomalyDetector(AnomalyMetrics metrics,
                                    @Autowired(required = false) @Qualifier("reactiveRedisTemplate") ReactiveRedisTemplate<String, String> redisTemplate) {
        this.metrics = metrics;
        this.redisTemplate = redisTemplate;
        if (redisTemplate != null) {
            log.info("[ANOMALY ENGINE] Initialized with Distributed Reactive Redis Cluster state backend.");
        } else {
            log.info("[ANOMALY ENGINE] Initialized with High-Performance In-Memory Sliding Window backend.");
        }
    }

    /**
     * Evaluates financial transaction velocity anomalies across distributed nodes.
     */
    public Mono<AnomalyScore> evaluateTransactionVelocity(String accountId, BigDecimal amount) {
        if (accountId == null || amount == null) {
            return Mono.just(AnomalyScore.normal());
        }

        if (redisTemplate != null) {
            return evaluateVelocityWithRedis(accountId, amount)
                    .onErrorResume(e -> {
                        log.warn("[REDIS FALLBACK] Redis velocity evaluation failed ({}); using in-memory window", e.getMessage());
                        return evaluateVelocityInMemory(accountId, amount);
                    });
        }

        return evaluateVelocityInMemory(accountId, amount);
    }

    /**
     * Evaluates telemetry metric Z-Score outliers across distributed nodes.
     */
    public Mono<AnomalyScore> evaluateTelemetryZScore(String deviceId, double metricValue) {
        if (deviceId == null) {
            return Mono.just(AnomalyScore.normal());
        }

        if (redisTemplate != null) {
            return evaluateTelemetryWithRedis(deviceId, metricValue)
                    .onErrorResume(e -> {
                        log.warn("[REDIS FALLBACK] Redis telemetry evaluation failed ({}); using in-memory window", e.getMessage());
                        return evaluateTelemetryInMemory(deviceId, metricValue);
                    });
        }

        return evaluateTelemetryInMemory(deviceId, metricValue);
    }

    // ==========================================
    // Distributed Redis Backend (Sorted Sets)
    // ==========================================

    private Mono<AnomalyScore> evaluateVelocityWithRedis(String accountId, BigDecimal amount) {
        String key = "eventdrivenmicroservices:anomaly:velocity:" + accountId;
        long nowMillis = System.currentTimeMillis();
        long windowStartMillis = nowMillis - VELOCITY_WINDOW_DURATION.toMillis();
        String member = nowMillis + ":" + amount.toPlainString() + ":" + UUID.randomUUID().toString();

        return redisTemplate.opsForZSet()
                .removeRangeByScore(key, Range.closed(Double.NEGATIVE_INFINITY, (double) windowStartMillis))
                .then(redisTemplate.opsForZSet().add(key, member, (double) nowMillis))
                .then(redisTemplate.expire(key, Duration.ofSeconds(60)))
                .then(redisTemplate.opsForZSet().range(key, Range.unbounded()).collectList())
                .map(members -> {
                    BigDecimal totalSum = BigDecimal.ZERO;
                    int count = members.size();

                    for (String entry : members) {
                        String[] parts = entry.split(":");
                        if (parts.length >= 2) {
                            try {
                                totalSum = totalSum.add(new BigDecimal(parts[1]));
                            } catch (NumberFormatException ignored) {}
                        }
                    }

                    if (count >= VELOCITY_COUNT_THRESHOLD || totalSum.compareTo(VELOCITY_AMOUNT_THRESHOLD) > 0) {
                        String details = String.format("Distributed Velocity Spike Detected: %d transactions totaling $%s in window", count, totalSum);
                        log.warn("[DISTRIBUTED ANOMALY DETECTED] Account: {} | {}", accountId, details);
                        metrics.recordAnomaly("VELOCITY_SPIKE");
                        return AnomalyScore.flagged("FINANCIAL_VELOCITY_SPIKE", (double) count, details);
                    }

                    return AnomalyScore.normal();
                });
    }

    private Mono<AnomalyScore> evaluateTelemetryWithRedis(String deviceId, double metricValue) {
        String key = "eventdrivenmicroservices:anomaly:telemetry:" + deviceId;

        return redisTemplate.opsForList().leftPush(key, String.valueOf(metricValue))
                .then(redisTemplate.opsForList().trim(key, 0, TELEMETRY_WINDOW_SIZE - 1))
                .then(redisTemplate.expire(key, Duration.ofMinutes(10)))
                .then(redisTemplate.opsForList().range(key, 0, -1).collectList())
                .map(entries -> calculateZScoreFromList(deviceId, metricValue, entries));
    }

    private AnomalyScore calculateZScoreFromList(String deviceId, double metricValue, List<String> entries) {
        if (entries == null || entries.size() < 5) {
            return AnomalyScore.normal(); // Warmup phase
        }

        double sum = 0.0;
        int count = 0;
        for (String entry : entries) {
            try {
                sum += Double.parseDouble(entry);
                count++;
            } catch (NumberFormatException ignored) {}
        }

        if (count < 5) return AnomalyScore.normal();
        double mean = sum / count;

        double varianceSum = 0.0;
        for (String entry : entries) {
            try {
                double val = Double.parseDouble(entry);
                varianceSum += Math.pow(val - mean, 2);
            } catch (NumberFormatException ignored) {}
        }
        double stddev = Math.sqrt(varianceSum / count);

        if (stddev > 0.0001) {
            double zScore = Math.abs(metricValue - mean) / stddev;
            metrics.updateZScore(zScore);

            if (zScore >= Z_SCORE_THRESHOLD) {
                String details = String.format("Distributed Z-Score Outlier: Value=%.2f, Mean=%.2f, StdDev=%.2f, Z-Score=%.2f", metricValue, mean, stddev, zScore);
                log.warn("[DISTRIBUTED ANOMALY DETECTED] Device: {} | {}", deviceId, details);
                metrics.recordAnomaly("Z_SCORE_OUTLIER");
                return AnomalyScore.flagged("TELEMETRY_Z_SCORE_OUTLIER", zScore, details);
            }
        }

        return AnomalyScore.normal();
    }

    // ==========================================
    // In-Memory Fallback Backend
    // ==========================================

    private Mono<AnomalyScore> evaluateVelocityInMemory(String accountId, BigDecimal amount) {
        return Mono.fromSupplier(() -> {
            Instant now = Instant.now();
            Deque<TransactionTimestamp> window = velocityWindows.computeIfAbsent(accountId, k -> new ArrayDeque<>());

            synchronized (window) {
                while (!window.isEmpty() && Duration.between(window.peekFirst().timestamp, now).compareTo(VELOCITY_WINDOW_DURATION) > 0) {
                    window.pollFirst();
                }

                window.addLast(new TransactionTimestamp(now, amount));

                BigDecimal totalSum = BigDecimal.ZERO;
                for (TransactionTimestamp tx : window) {
                    totalSum = totalSum.add(tx.amount);
                }

                int count = window.size();

                if (count >= VELOCITY_COUNT_THRESHOLD || totalSum.compareTo(VELOCITY_AMOUNT_THRESHOLD) > 0) {
                    String details = String.format("Velocity Spike Detected: %d transactions totaling $%s in window", count, totalSum);
                    log.warn("[ANOMALY DETECTED] Account: {} | {}", accountId, details);
                    metrics.recordAnomaly("VELOCITY_SPIKE");
                    return AnomalyScore.flagged("FINANCIAL_VELOCITY_SPIKE", (double) count, details);
                }
            }

            return AnomalyScore.normal();
        });
    }

    private Mono<AnomalyScore> evaluateTelemetryInMemory(String deviceId, double metricValue) {
        return Mono.fromSupplier(() -> {
            Deque<Double> window = telemetryWindows.computeIfAbsent(deviceId, k -> new ArrayDeque<>());

            synchronized (window) {
                if (window.size() >= TELEMETRY_WINDOW_SIZE) {
                    window.pollFirst();
                }
                window.addLast(metricValue);

                if (window.size() < 5) {
                    return AnomalyScore.normal();
                }

                double sum = 0.0;
                for (double val : window) {
                    sum += val;
                }
                double mean = sum / window.size();

                double varianceSum = 0.0;
                for (double val : window) {
                    varianceSum += Math.pow(val - mean, 2);
                }
                double stddev = Math.sqrt(varianceSum / window.size());

                if (stddev > 0.0001) {
                    double zScore = Math.abs(metricValue - mean) / stddev;
                    metrics.updateZScore(zScore);

                    if (zScore >= Z_SCORE_THRESHOLD) {
                        String details = String.format("Z-Score Outlier: Value=%.2f, Mean=%.2f, StdDev=%.2f, Z-Score=%.2f", metricValue, mean, stddev, zScore);
                        log.warn("[ANOMALY DETECTED] Device: {} | {}", deviceId, details);
                        metrics.recordAnomaly("Z_SCORE_OUTLIER");
                        return AnomalyScore.flagged("TELEMETRY_Z_SCORE_OUTLIER", zScore, details);
                    }
                }
            }

            return AnomalyScore.normal();
        });
    }

    private static class TransactionTimestamp {
        final Instant timestamp;
        final BigDecimal amount;

        TransactionTimestamp(Instant timestamp, BigDecimal amount) {
            this.timestamp = timestamp;
            this.amount = amount;
        }
    }
}
