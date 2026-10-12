package com.eventdrivenmicroservices.platform.infrastructure.outbox;

import com.eventdrivenmicroservices.platform.infrastructure.observability.OutboxMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OutboxBacklogMonitorTest {

    @Test
    void publishesDepthAndOldestAge() {
        OutboxEventRepository repository = mock(OutboxEventRepository.class);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OutboxEvent oldest = new OutboxEvent();
        oldest.setCreatedAt(Instant.now().minusSeconds(120));
        when(repository.countByProcessedFalse()).thenReturn(Mono.just(4L));
        when(repository.findFirstByProcessedFalseOrderByCreatedAtAsc()).thenReturn(Mono.just(oldest));

        StepVerifier.create(new OutboxBacklogMonitor(repository, new OutboxMetrics(registry)).refreshBacklog())
                .verifyComplete();

        assertEquals(4.0, registry.get("eventdrivenmicroservices.outbox.pending").gauge().value());
        assertTrue(registry.get("eventdrivenmicroservices.outbox.oldest.pending.age.seconds").gauge().value() >= 119);
    }

    @Test
    void reportsZeroWhenNothingIsPending() {
        OutboxEventRepository repository = mock(OutboxEventRepository.class);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        when(repository.countByProcessedFalse()).thenReturn(Mono.empty());
        when(repository.findFirstByProcessedFalseOrderByCreatedAtAsc()).thenReturn(Mono.empty());

        StepVerifier.create(new OutboxBacklogMonitor(repository, new OutboxMetrics(registry)).refreshBacklog())
                .verifyComplete();

        assertEquals(0.0, registry.get("eventdrivenmicroservices.outbox.pending").gauge().value());
    }
}
