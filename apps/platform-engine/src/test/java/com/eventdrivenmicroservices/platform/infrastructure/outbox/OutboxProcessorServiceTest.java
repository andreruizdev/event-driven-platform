package com.eventdrivenmicroservices.platform.infrastructure.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.eventdrivenmicroservices.platform.infrastructure.observability.OutboxMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class OutboxProcessorServiceTest {

    private OutboxEventRepository repository;
    private KafkaTemplate<String, String> kafkaTemplate;
    private ObjectMapper objectMapper;
    private SimpleMeterRegistry meterRegistry;
    private OutboxMetrics metrics;
    private OutboxProcessorService processorService;

    @BeforeEach
    void setUp() {
        repository = mock(OutboxEventRepository.class);
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> template = mock(KafkaTemplate.class);
        kafkaTemplate = template;
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        meterRegistry = new SimpleMeterRegistry();
        metrics = new OutboxMetrics(meterRegistry);
        when(repository.countByProcessedFalse()).thenReturn(Mono.just(0L));
        when(repository.findFirstByProcessedFalseOrderByCreatedAtAsc()).thenReturn(Mono.empty());

        processorService = new OutboxProcessorService(repository, kafkaTemplate, objectMapper, metrics);
    }

    @Test
    void processOutboxEvents_SuccessfulPublish_MarksEventsAsProcessed() {
        OutboxEvent event1 = createEvent("agg-1", "LoanApplication");
        OutboxEvent event2 = createEvent("agg-2", "FinancialTransaction");

        when(repository.findPendingForClaim(any(Instant.class), eq(50))).thenReturn(Flux.just(event1, event2));
        when(repository.claimLock(any(UUID.class), anyString(), any(Instant.class), any(Instant.class))).thenReturn(Mono.just(1));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        when(repository.save(any(OutboxEvent.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        processorService.processOutboxEvents();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate, times(2)).send(captor.capture());

        List<ProducerRecord<String, String>> sentRecords = captor.getAllValues();
        assertEquals(2, sentRecords.size());
        assertEquals("agg-1", sentRecords.get(0).key());
        assertEquals("agg-2", sentRecords.get(1).key());

        verify(repository, times(2)).save(argThat(OutboxEvent::isProcessed));
        assertEquals(2.0, meterRegistry.counter("eventdrivenmicroservices.outbox.publish.success").count());
        assertEquals(2L, meterRegistry.timer("eventdrivenmicroservices.outbox.publish.duration").count());
    }

    @Test
    void processOutboxEvents_KafkaFailureOnFirstEvent_IsolatesErrorAndProcessesSecondEvent() {
        OutboxEvent failingEvent = createEvent("failing-agg", "LoanApplication");
        OutboxEvent successfulEvent = createEvent("success-agg", "FinancialTransaction");
        Instant retryCheckStarted = Instant.now();

        when(repository.findPendingForClaim(any(Instant.class), eq(50))).thenReturn(Flux.just(failingEvent, successfulEvent));
        when(repository.claimLock(any(UUID.class), anyString(), any(Instant.class), any(Instant.class))).thenReturn(Mono.just(1));

        CompletableFuture<org.springframework.kafka.support.SendResult<String, String>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Kafka broker connection timeout"));

        when(kafkaTemplate.send(any(ProducerRecord.class))).thenAnswer(invocation -> {
            ProducerRecord<String, String> record = invocation.getArgument(0);
            if ("failing-agg".equals(record.key())) {
                return failedFuture;
            }
            return CompletableFuture.completedFuture(null);
        });

        when(repository.save(any(OutboxEvent.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        processorService.processOutboxEvents();

        verify(repository, times(1)).save(argThat(e -> "success-agg".equals(e.getAggregateId()) && e.isProcessed()));
        verify(repository, times(1)).save(argThat(e -> "failing-agg".equals(e.getAggregateId())
            && !e.isProcessed()
            && e.getRetryCount() == 1
            && e.getLockedUntil().isAfter(retryCheckStarted.plusSeconds(4))));
        assertEquals(1.0, meterRegistry.counter("eventdrivenmicroservices.outbox.processing.failure").count());
        assertEquals(1.0, meterRegistry.counter("eventdrivenmicroservices.outbox.retry").count());
        assertEquals(1.0, meterRegistry.counter("eventdrivenmicroservices.outbox.publish.success").count());
    }

    @Test
    void processOutboxEvents_WhenClaimLockFails_SkipsPublishing() {
        OutboxEvent alreadyClaimedEvent = createEvent("claimed-agg", "LoanApplication");

        when(repository.findPendingForClaim(any(Instant.class), eq(50))).thenReturn(Flux.just(alreadyClaimedEvent));
        when(repository.claimLock(any(UUID.class), anyString(), any(Instant.class), any(Instant.class))).thenReturn(Mono.just(0));

        processorService.processOutboxEvents();

        verify(kafkaTemplate, never()).send(any(ProducerRecord.class));
        verify(repository, never()).save(any(OutboxEvent.class));
    }

    @Test
    void processOutboxEvents_UpdatesBacklogGauges() {
        OutboxEvent oldest = createEvent("oldest-agg", "LoanApplication");
        oldest.setCreatedAt(Instant.now().minusSeconds(12));
        when(repository.countByProcessedFalse()).thenReturn(Mono.just(3L));
        when(repository.findFirstByProcessedFalseOrderByCreatedAtAsc()).thenReturn(Mono.just(oldest));
        when(repository.findPendingForClaim(any(Instant.class), eq(50))).thenReturn(Flux.empty());

        processorService.processOutboxEvents();

        assertEquals(3.0, meterRegistry.get("eventdrivenmicroservices.outbox.pending").gauge().value());
        assertTrue(meterRegistry.get("eventdrivenmicroservices.outbox.oldest.pending.age.seconds").gauge().value() >= 11.0);
    }

    private OutboxEvent createEvent(String aggregateId, String aggregateType) {
        OutboxEvent event = new OutboxEvent();
        event.setId(UUID.randomUUID());
        event.setAggregateId(aggregateId);
        event.setAggregateType(aggregateType);
        event.setEventType("TEST_EVENT");
        event.setPayload("{}");
        event.setProcessed(false);
        event.setCreatedAt(Instant.now());
        return event;
    }
}
