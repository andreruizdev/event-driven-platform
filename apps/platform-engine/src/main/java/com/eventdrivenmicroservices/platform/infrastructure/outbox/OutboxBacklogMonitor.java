package com.eventdrivenmicroservices.platform.infrastructure.outbox;

import com.eventdrivenmicroservices.platform.infrastructure.observability.OutboxMetrics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Instant;

/**
 * Publishes outbox depth and oldest-pending age in CDC mode. In poller mode the relay already refreshes
 * these gauges on every cycle, so this component is not created there.
 */
@Component
@Slf4j
@ConditionalOnProperty(prefix = "app.outbox.publisher", name = "mode", havingValue = "cdc")
public class OutboxBacklogMonitor {

    private final OutboxEventRepository repository;
    private final OutboxMetrics metrics;

    public OutboxBacklogMonitor(OutboxEventRepository repository, OutboxMetrics metrics) {
        this.repository = repository;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "${app.outbox.backlog-monitor.interval-ms:5000}")
    public void refresh() {
        refreshBacklog()
                .doOnError(error -> log.warn("Outbox backlog refresh failed", error))
                .onErrorResume(error -> Mono.empty())
                .subscribe();
    }

    Mono<Void> refreshBacklog() {
        return repository.countByProcessedFalse()
                .defaultIfEmpty(0L)
                .flatMap(pending -> repository.findFirstByProcessedFalseOrderByCreatedAtAsc()
                        .map(oldest -> Math.max(0L, Instant.now().getEpochSecond() - oldest.getCreatedAt().getEpochSecond()))
                        .defaultIfEmpty(0L)
                        .doOnNext(age -> metrics.updateBacklog(pending, age)))
                .then();
    }
}
