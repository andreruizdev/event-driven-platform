package com.eventdrivenmicroservices.platform.api;

import com.eventdrivenmicroservices.platform.domain.LedgerEvent;
import com.eventdrivenmicroservices.platform.infrastructure.postgres.LedgerEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/ledger")
@RequiredArgsConstructor
public class LedgerController {

    private final LedgerEventRepository ledgerEventRepository;

    @GetMapping
    public Flux<LedgerEvent> list() {
        return ledgerEventRepository.findAllByOrderByCreatedAtDesc();
    }

    @GetMapping("/latest-hash")
    public Mono<Map<String, String>> latestHash() {
        return ledgerEventRepository.findLatestHash()
                .map(hash -> Map.of("currentHash", hash))
                .defaultIfEmpty(Map.of("currentHash", ""));
    }
}
