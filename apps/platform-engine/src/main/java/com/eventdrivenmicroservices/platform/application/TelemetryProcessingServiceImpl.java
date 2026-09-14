package com.eventdrivenmicroservices.platform.application;

import com.eventdrivenmicroservices.platform.anomaly.StreamingAnomalyDetector;
import com.eventdrivenmicroservices.platform.model.TelemetryEvent;
import com.eventdrivenmicroservices.platform.model.TelemetryPayload;
import com.eventdrivenmicroservices.platform.infrastructure.postgres.TelemetryEventRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.annotation.Transactional;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEvent;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEventRepository;
import reactor.core.publisher.Mono;
import java.time.Instant;

@Service
@Slf4j
public class TelemetryProcessingServiceImpl implements TelemetryProcessingService {

    private final TelemetryEventRepository repository;
    private final OutboxEventRepository outboxRepository;
    private final LedgerService ledgerService;
    private final StreamingAnomalyDetector anomalyDetector;
    private final ObjectMapper objectMapper;

    @Autowired
    public TelemetryProcessingServiceImpl(TelemetryEventRepository repository,
                                          OutboxEventRepository outboxRepository,
                                          LedgerService ledgerService,
                                          StreamingAnomalyDetector anomalyDetector,
                                          ObjectMapper objectMapper) {
        this.repository = repository;
        this.outboxRepository = outboxRepository;
        this.ledgerService = ledgerService;
        this.anomalyDetector = anomalyDetector;
        this.objectMapper = objectMapper;
    }

    @Override
    @Transactional
    // ENTERPRISE ARCHITECTURE: Transactional Outbox Pattern
    // The @Transactional annotation ensures that saving the core business event (TelemetryEvent) 
    // and the outgoing messaging event (OutboxEvent) happen atomically. 
    // This perfectly resolves the distributed systems 'dual-write' problem—if the database commits, 
    // Debezium will read the WAL and Kafka will receive the event. If it fails, neither commits.
    public Mono<Void> processPayloadAsync(TelemetryPayload payload) {
        log.info("[{}] Processing telemetry payload async for device: {}", Thread.currentThread().getName(), payload.getDeviceId());
        
        TelemetryEvent event = new TelemetryEvent(
            payload.getDeviceId(),
            payload.getTimestamp(),
            payload.getLogLevel(),
            payload.getRawPayload()
        );
        
        return repository.save(event)
            .flatMap(savedEvent -> {
                log.info("[{}] Saved telemetry event to database. Event ID: {}", Thread.currentThread().getName(), savedEvent.getId());
                
                String payloadStr;
                try {
                    payloadStr = objectMapper.writeValueAsString(savedEvent);
                } catch (JsonProcessingException e) {
                    return Mono.error(new RuntimeException("Failed to serialize telemetry event", e));
                }

                // Extract metric value for Z-Score evaluation
                double metricVal = parseMetricValue(payload);

                return anomalyDetector.evaluateTelemetryZScore(payload.getDeviceId(), metricVal)
                    .flatMap(anomalyScore -> {
                        String eventType = anomalyScore.isAnomaly() ? "TelemetryAnomalyDetected" : "TelemetryIngested";

                        OutboxEvent outboxEvent = new OutboxEvent();
                        outboxEvent.setAggregateType("TelemetryEvent");
                        outboxEvent.setAggregateId(savedEvent.getId().toString());
                        outboxEvent.setEventType(eventType);
                        outboxEvent.setPayload(payloadStr);
                        outboxEvent.setCreatedAt(Instant.now());

                        Mono<OutboxEvent> saveOutbox = outboxRepository.save(outboxEvent);

                        if (anomalyScore.isAnomaly()) {
                            log.warn("[ANOMALY REACTION] Telemetry Z-Score outlier flagged for device: {}", payload.getDeviceId());
                            return ledgerService.appendEvent("TELEMETRY_ANOMALY", payloadStr)
                                    .then(saveOutbox);
                        }

                        return saveOutbox;
                    }).thenReturn(savedEvent);
            })
            .doOnError(e -> log.error("[{}] Error processing telemetry async: {}", Thread.currentThread().getName(), e.getMessage(), e))
            .then();
    }

    private double parseMetricValue(TelemetryPayload payload) {
        if (payload == null || payload.getRawPayload() == null) {
            return 0.0;
        }
        try {
            if (payload.getRawPayload().contains("cpu_load")) {
                // Quick extraction of cpu_load number from raw json payload
                String raw = payload.getRawPayload();
                int idx = raw.indexOf("cpu_load");
                if (idx != -1) {
                    String sub = raw.substring(idx + 10).replaceAll("[^0-9.]", " ");
                    String[] tokens = sub.trim().split("\\s+");
                    if (tokens.length > 0 && !tokens[0].isEmpty()) {
                        return Double.parseDouble(tokens[0]);
                    }
                }
            }
        } catch (Exception e) {
            log.debug("Metric parse fallback: {}", e.getMessage());
        }
        return payload.getRawPayload().length();
    }
}
