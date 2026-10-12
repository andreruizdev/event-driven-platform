package com.eventdrivenmicroservices.platform.application;

import static com.eventdrivenmicroservices.platform.config.LogSanitizer.safe;
import com.eventdrivenmicroservices.platform.anomaly.StreamingAnomalyDetector;
import com.eventdrivenmicroservices.platform.model.FinancialTransaction;
import com.eventdrivenmicroservices.platform.model.FinancialTransactionPayload;
import com.eventdrivenmicroservices.platform.infrastructure.postgres.FinancialTransactionRepository;
import com.eventdrivenmicroservices.platform.infrastructure.observability.TraceSupport;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEvent;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEventRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import reactor.core.publisher.Mono;
import java.time.Instant;
import java.time.OffsetDateTime;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class TransactionProcessingServiceImpl implements TransactionProcessingService {

    private final FinancialTransactionRepository repository;
    private final OutboxEventRepository outboxRepository;
    private final LedgerService ledgerService;
    private final StreamingAnomalyDetector anomalyDetector;
    private final ObjectMapper objectMapper;
    private TraceSupport traceSupport = TraceSupport.noop();

    @Autowired
    public TransactionProcessingServiceImpl(FinancialTransactionRepository repository,
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

    @Autowired(required = false)
    void setTraceSupport(TraceSupport traceSupport) {
        this.traceSupport = traceSupport;
    }

    @Override
    @Transactional
    public Mono<Void> processTransactionAsync(FinancialTransactionPayload payload, String traceparent, String requestCorrelationId) {
        log.info("Processing financial transaction: {} amount: {}", safe(payload.getTransactionId()), payload.getAmount());

        String correlationId = payload.getCorrelationId() == null || payload.getCorrelationId().isBlank()
                ? requestCorrelationId
                : payload.getCorrelationId();
        FinancialTransaction event = new FinancialTransaction();
        event.setTransactionId(payload.getTransactionId());
        event.setAmount(payload.getAmount());
        event.setCurrency(payload.getCurrency());
        event.setMerchantId(payload.getMerchantId());
        event.setIpAddress(payload.getIpAddress());
        event.setGeolocation(payload.getGeolocation());
        event.setDeviceFingerprint(payload.getDeviceFingerprint());
        event.setCorrelationId(correlationId);
        event.setSettlementStatus(payload.getSettlementStatus());
        event.setTimestamp(payload.getTimestamp() != null ? payload.getTimestamp() : OffsetDateTime.now());
        
        return repository.save(event)
            .flatMap(savedEvent -> {
                String payloadStr;
                try {
                    payloadStr = objectMapper.writeValueAsString(savedEvent);
                } catch (JsonProcessingException e) {
                    return Mono.error(new RuntimeException("Failed to serialize transaction event", e));
                }

                // Evaluate streaming anomaly velocity check
                String trackerKey = payload.getMerchantId() != null ? payload.getMerchantId() : "global";
                return anomalyDetector.evaluateTransactionVelocity(trackerKey, payload.getAmount())
                    .flatMap(anomalyScore -> {
                        String eventType = anomalyScore.isAnomaly() ? "TransactionAnomalyDetected" : "TransactionCreated";

                        OutboxEvent outboxEvent = new OutboxEvent();
                        outboxEvent.setAggregateType("FinancialTransaction");
                        outboxEvent.setAggregateId(savedEvent.getId().toString());
                        outboxEvent.setEventType(eventType);
                        outboxEvent.setPayload(payloadStr);
                        outboxEvent.setCorrelationId(correlationId);
                        outboxEvent.setCreatedAt(Instant.now());

                        return traceSupport.currentTraceparent().defaultIfEmpty(traceparent == null ? "" : traceparent)
                                .flatMap(currentTrace -> {
                                    outboxEvent.setTraceparent(currentTrace.isEmpty() ? null : currentTrace);
                                    Mono<OutboxEvent> saveOutbox = outboxRepository.save(outboxEvent);
                                    if (anomalyScore.isAnomaly()) {
                                        log.warn("[ANOMALY REACTION] Financial velocity anomaly flagged for merchant: {}", safe(trackerKey));
                                        return saveOutbox.flatMap(savedOutbox -> traceSupport.traced("ledger.append",
                                                ledgerService.appendToLedger("ANOMALY_FLAGGED", payloadStr,
                                                        savedOutbox.getId(), correlationId))
                                                .thenReturn(savedOutbox));
                                    }
                                    return saveOutbox;
                                });
                    }).thenReturn(savedEvent);
            })
            .doOnError(e -> log.error("Error processing transaction async: {}", e.getMessage(), e))
            .then();
    }
}
