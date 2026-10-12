package com.eventdrivenmicroservices.platform.api;

import com.eventdrivenmicroservices.platform.application.LedgerService;
import com.eventdrivenmicroservices.platform.application.LedgerVerification;
import com.eventdrivenmicroservices.platform.domain.LedgerEvent;
import com.eventdrivenmicroservices.platform.infrastructure.postgres.LedgerEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/ledger")
@RequiredArgsConstructor
public class LedgerController {

    static final int MAX_LIST_LIMIT = 1000;
    static final long MAX_VERIFY_WINDOW = 100_000;

    private final LedgerEventRepository ledgerEventRepository;
    private final LedgerService ledgerService;

    @GetMapping("/verify")
    public Mono<LedgerVerification> verify(@RequestParam(required = false) Long recent) {
        if (recent == null) {
            return ledgerService.verifyChain();
        }
        return ledgerService.verifyRecent(Math.max(1, Math.min(recent, MAX_VERIFY_WINDOW)));
    }

    @GetMapping
    public Flux<LedgerEvent> list(@RequestParam(defaultValue = "100") int limit) {
        return ledgerEventRepository.findAllByOrderByCreatedAtDesc().take(Math.max(1, Math.min(limit, MAX_LIST_LIMIT)));
    }

    @GetMapping("/latest-hash")
    public Mono<Map<String, String>> latestHash() {
        return ledgerEventRepository.findLatestHash()
                .map(hash -> Map.of("currentHash", hash))
                .defaultIfEmpty(Map.of("currentHash", ""));
    }
}
