package com.eventdrivenmicroservices.platform.api;

import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEvent;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.Instant;

@RestController
@RequestMapping("/api/v1/outbox")
@RequiredArgsConstructor
public class OutboxController {

    private final OutboxEventRepository outboxEventRepository;

    @GetMapping("/status")
    public Mono<OutboxStatus> status() {
        return outboxEventRepository.countByProcessedFalse()
                .defaultIfEmpty(0L)
                .flatMap(pendingCount -> outboxEventRepository.findFirstByProcessedFalseOrderByCreatedAtAsc()
                        .map(oldest -> new OutboxStatus(pendingCount, oldest.getCreatedAt(), ageInSeconds(oldest.getCreatedAt())))
                        .defaultIfEmpty(new OutboxStatus(pendingCount, null, 0L)));
    }

    private long ageInSeconds(Instant createdAt) {
        return createdAt == null ? 0L : Math.max(0L, Instant.now().getEpochSecond() - createdAt.getEpochSecond());
    }

    public record OutboxStatus(long pendingCount, Instant oldestCreatedAt, long oldestAgeSeconds) {
    }
}
