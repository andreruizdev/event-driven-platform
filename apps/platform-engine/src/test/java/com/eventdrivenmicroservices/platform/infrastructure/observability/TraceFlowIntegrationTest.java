package com.eventdrivenmicroservices.platform.infrastructure.observability;

import com.eventdrivenmicroservices.platform.anomaly.AnomalyScore;
import com.eventdrivenmicroservices.platform.anomaly.StreamingAnomalyDetector;
import com.eventdrivenmicroservices.platform.infrastructure.kafka.AnomalyEventListener;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.EventEnvelope;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEvent;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEventRepository;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxProcessorService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class TraceFlowIntegrationTest {

    private CorrelationIdWebFilter correlationIdWebFilter;
    private StreamingAnomalyDetector anomalyDetector;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        correlationIdWebFilter = new CorrelationIdWebFilter();
        AnomalyMetrics metrics = new AnomalyMetrics(new SimpleMeterRegistry());
        anomalyDetector = new StreamingAnomalyDetector(metrics);
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Test
    void testCorrelationFilter_GeneratesOrPreservesCorrelationId() {
        MockServerWebExchange exchangeWithoutHeader = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/v1/ledger").build());
        WebFilterChain filterChain = serverWebExchange -> Mono.empty();

        correlationIdWebFilter.filter(exchangeWithoutHeader, filterChain).block();
        String generatedId = exchangeWithoutHeader.getResponse().getHeaders().getFirst(CorrelationIdWebFilter.CORRELATION_ID_HEADER);
        assertNotNull(generatedId, "Filter must generate X-Correlation-Id when missing");
        assertFalse(generatedId.isBlank());

        String clientCorrelationId = "client-trace-12345";
        MockServerWebExchange exchangeWithHeader = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/v1/ledger")
                        .header(CorrelationIdWebFilter.CORRELATION_ID_HEADER, clientCorrelationId)
                        .build());

        correlationIdWebFilter.filter(exchangeWithHeader, filterChain).block();
        String preservedId = exchangeWithHeader.getResponse().getHeaders().getFirst(CorrelationIdWebFilter.CORRELATION_ID_HEADER);
        assertEquals(clientCorrelationId, preservedId, "Filter must preserve existing X-Correlation-Id");
    }

    @Test
    void testEndToEndTraceContextPropagation_ThroughOutboxAndKafkaListener() throws Exception {
        String testTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        String aggregateId = "account-999";

        // 1. Create OutboxEvent with traceparent
        OutboxEvent outboxEvent = new OutboxEvent();
        outboxEvent.setId(UUID.randomUUID());
        outboxEvent.setAggregateType("Transaction");
        outboxEvent.setAggregateId(aggregateId);
        outboxEvent.setEventType("ANOMALY_VELOCITY_SPIKE");
        outboxEvent.setPayload("{\"account\":\"account-999\",\"amount\":15000.00}");
        outboxEvent.setTraceparent(testTraceparent);
        outboxEvent.setProcessed(false);
        outboxEvent.setCreatedAt(Instant.now());

        // 2. Mock OutboxProcessor dependencies
        OutboxEventRepository repo = mock(OutboxEventRepository.class);
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        when(repo.save(any(OutboxEvent.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));

        OutboxProcessorService outboxProcessor = new OutboxProcessorService(repo, kafkaTemplate, objectMapper);

        // 3. Trigger outbox publication
        when(repo.findByProcessedFalse()).thenReturn(reactor.core.publisher.Flux.just(outboxEvent));
        outboxProcessor.processOutboxEvents();

        // 4. Capture record published to Kafka by OutboxProcessor
        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, String>> outboxCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate, timeout(2000).atLeastOnce()).send(outboxCaptor.capture());

        ProducerRecord<String, String> publishedRecord = outboxCaptor.getValue();
        assertEquals("eventdrivenmicroservices-outbox-events", publishedRecord.topic());
        assertEquals(aggregateId, publishedRecord.key());

        Header outboxTraceHeader = publishedRecord.headers().lastHeader("traceparent");
        assertNotNull(outboxTraceHeader, "Outbox Kafka record must include traceparent header");
        assertEquals(testTraceparent, new String(outboxTraceHeader.value(), StandardCharsets.UTF_8));

        // 5. Deserialize published EventEnvelope payload
        EventEnvelope envelope = objectMapper.readValue(publishedRecord.value(), EventEnvelope.class);
        assertEquals(testTraceparent, envelope.traceparent());
        assertEquals("ANOMALY_VELOCITY_SPIKE", envelope.eventType());

        // 6. Simulate Kafka Consumer consumption by AnomalyEventListener
        RecordHeaders consumerHeaders = new RecordHeaders();
        consumerHeaders.add(new RecordHeader("traceparent", testTraceparent.getBytes(StandardCharsets.UTF_8)));

        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>(
                "eventdrivenmicroservices-outbox-events", 0, 0L, aggregateId, publishedRecord.value());
        consumerHeaders.forEach(h -> consumerRecord.headers().add(h));

        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> alertKafkaTemplate = mock(KafkaTemplate.class);
        AnomalyEventListener listener = new AnomalyEventListener(alertKafkaTemplate);

        listener.consumeOutboxEvent(consumerRecord);

        // 7. Verify AnomalyEventListener forwards traceparent to anomaly-alerts topic
        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, String>> alertCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(alertKafkaTemplate, times(1)).send(alertCaptor.capture());

        ProducerRecord<String, String> alertRecord = alertCaptor.getValue();
        assertEquals("eventdrivenmicroservices-anomaly-alerts", alertRecord.topic());
        Header alertTraceHeader = alertRecord.headers().lastHeader("traceparent");
        assertNotNull(alertTraceHeader, "Alert record must preserve traceparent header");
        assertEquals(testTraceparent, new String(alertTraceHeader.value(), StandardCharsets.UTF_8));
    }

    @Test
    void testAnomalyDetector_VelocityAndTelemetryEvaluation_ProducesExpectedScores() {
        // High amount anomaly evaluation
        StepVerifier.create(anomalyDetector.evaluateTransactionVelocity("merchant-999", new BigDecimal("20000.00")))
                .expectNextMatches(score -> score.isAnomaly() && "FINANCIAL_VELOCITY_SPIKE".equals(score.getAnomalyType()))
                .verifyComplete();

        // Telemetry normal evaluation
        for (int i = 0; i < 15; i++) {
            anomalyDetector.evaluateTelemetryZScore("device-trace-test", 20.0 + (i % 3) * 0.2).block();
        }
        StepVerifier.create(anomalyDetector.evaluateTelemetryZScore("device-trace-test", 20.3))
                .expectNextMatches(score -> !score.isAnomaly())
                .verifyComplete();
    }
}
