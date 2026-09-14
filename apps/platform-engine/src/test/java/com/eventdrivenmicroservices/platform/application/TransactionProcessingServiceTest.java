package com.eventdrivenmicroservices.platform.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.eventdrivenmicroservices.platform.anomaly.AnomalyScore;
import com.eventdrivenmicroservices.platform.anomaly.StreamingAnomalyDetector;
import com.eventdrivenmicroservices.platform.model.FinancialTransaction;
import com.eventdrivenmicroservices.platform.model.FinancialTransactionPayload;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEvent;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEventRepository;
import com.eventdrivenmicroservices.platform.infrastructure.postgres.FinancialTransactionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class TransactionProcessingServiceTest {

    @Mock
    private FinancialTransactionRepository repository;
    @Mock
    private OutboxEventRepository outboxRepository;
    @Mock
    private LedgerService ledgerService;
    @Mock
    private StreamingAnomalyDetector anomalyDetector;
    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private TransactionProcessingServiceImpl service;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testProcessTransactionAsync_SerializationFailure_RollsBack() throws JsonProcessingException {
        // Arrange
        FinancialTransactionPayload payload = new FinancialTransactionPayload();
        payload.setTransactionId("tx-001");
        payload.setAmount(new BigDecimal("100.00"));
        payload.setCurrency("USD");
        payload.setTimestamp(OffsetDateTime.now());

        FinancialTransaction savedEvent = new FinancialTransaction();
        savedEvent.setId(UUID.randomUUID());
        savedEvent.setTransactionId(payload.getTransactionId());

        when(repository.save(any(FinancialTransaction.class))).thenReturn(Mono.just(savedEvent));
        when(objectMapper.writeValueAsString(any())).thenThrow(new JsonProcessingException("Mock failure") {});
        when(anomalyDetector.evaluateTransactionVelocity(any(), any())).thenReturn(Mono.just(AnomalyScore.normal()));

        // Act & Assert
        StepVerifier.create(service.processTransactionAsync(payload, "trace-01"))
            .expectErrorMessage("Failed to serialize transaction event")
            .verify();
            
        verify(outboxRepository, never()).save(any(OutboxEvent.class));
    }
}
