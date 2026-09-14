package com.eventdrivenmicroservices.platform.application;

import com.eventdrivenmicroservices.platform.anomaly.StreamingAnomalyDetector;
import com.eventdrivenmicroservices.platform.model.FinancialTransaction;
import com.eventdrivenmicroservices.platform.model.FinancialTransactionPayload;
import com.eventdrivenmicroservices.platform.infrastructure.postgres.FinancialTransactionRepository;
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

    @Override
    @Transactional
    public Mono<Void> processTransactionAsync(FinancialTransactionPayload payload, String traceparent) {
        log.info("Processing financial transaction: {} amount: {}", payload.getTransactionId(), payload.getAmount());
        
        FinancialTransaction event = new FinancialTransaction();
        event.setTransactionId(payload.getTransactionId());
        event.setAmount(payload.getAmount());
        event.setCurrency(payload.getCurrency());
        event.setMerchantId(payload.getMerchantId());
        event.setIpAddress(payload.getIpAddress());
        event.setGeolocation(payload.getGeolocation());
        event.setDeviceFingerprint(payload.getDeviceFingerprint());
        event.setCorrelationId(payload.getCorrelationId());
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
                        outboxEvent.setTraceparent(traceparent);
                        outboxEvent.setCreatedAt(Instant.now());

                        Mono<OutboxEvent> saveOutbox = outboxRepository.save(outboxEvent);

                        if (anomalyScore.isAnomaly()) {
                            log.warn("[ANOMALY REACTION] Financial velocity anomaly flagged for merchant: {}", trackerKey);
                            return ledgerService.appendEvent("ANOMALY_FLAGGED", payloadStr)
                                    .then(saveOutbox);
                        }

                        return saveOutbox;
                    }).thenReturn(savedEvent);
            })
            .doOnError(e -> log.error("Error processing transaction async: {}", e.getMessage(), e))
            .then();
    }
}
