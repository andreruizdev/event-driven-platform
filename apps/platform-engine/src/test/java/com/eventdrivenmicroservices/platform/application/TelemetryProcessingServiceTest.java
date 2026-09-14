package com.eventdrivenmicroservices.platform.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.eventdrivenmicroservices.platform.anomaly.AnomalyScore;
import com.eventdrivenmicroservices.platform.anomaly.StreamingAnomalyDetector;
import com.eventdrivenmicroservices.platform.model.TelemetryEvent;
import com.eventdrivenmicroservices.platform.model.TelemetryPayload;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEvent;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEventRepository;
import com.eventdrivenmicroservices.platform.infrastructure.postgres.TelemetryEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.OffsetDateTime;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class TelemetryProcessingServiceTest {

    @Mock
    private TelemetryEventRepository repository;
    @Mock
    private OutboxEventRepository outboxRepository;
    @Mock
    private LedgerService ledgerService;
    @Mock
    private StreamingAnomalyDetector anomalyDetector;
    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private TelemetryProcessingServiceImpl service;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testProcessPayloadAsync_SerializationFailure_RollsBack() throws JsonProcessingException {
        // Arrange
        TelemetryPayload payload = new TelemetryPayload();
        payload.setDeviceId("device-1");
        payload.setTimestamp(OffsetDateTime.now());
        payload.setLogLevel("INFO");
        payload.setRawPayload("{\"cpu\":90}");

        TelemetryEvent savedEvent = new TelemetryEvent(
            payload.getDeviceId(), payload.getTimestamp(), payload.getLogLevel(), payload.getRawPayload()
        );
        savedEvent.setId(UUID.randomUUID());

        when(repository.save(any(TelemetryEvent.class))).thenReturn(Mono.just(savedEvent));
        when(objectMapper.writeValueAsString(any())).thenThrow(new JsonProcessingException("Mock failure") {});
        when(anomalyDetector.evaluateTelemetryZScore(any(), anyDouble())).thenReturn(Mono.just(AnomalyScore.normal()));

        // Act & Assert
        StepVerifier.create(service.processPayloadAsync(payload))
            .expectErrorMessage("Failed to serialize telemetry event")
            .verify();
            
        verify(outboxRepository, never()).save(any(OutboxEvent.class));
    }
}
