package com.eventdrivenmicroservices.platform.application;

import com.eventdrivenmicroservices.platform.domain.LedgerEvent;
import com.eventdrivenmicroservices.platform.infrastructure.postgres.LedgerEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LedgerServiceVerificationTest {

    private static final String GENESIS = "0".repeat(64);

    private final LedgerEventRepository repository = mock(LedgerEventRepository.class);
    private final LedgerService service =
            new LedgerService(repository, mock(DatabaseClient.class), mock(TransactionalOperator.class));

    @Test
    void acceptsAnIntactChain() {
        LedgerEvent first = event("A", "{}", GENESIS);
        LedgerEvent second = event("B", "{\"x\":1}", first.getCurrentHash());
        when(repository.findAllInChainOrder()).thenReturn(Flux.just(first, second));

        StepVerifier.create(service.verifyChain())
                .expectNextMatches(result -> result.valid() && result.eventsChecked() == 2)
                .verifyComplete();
    }

    @Test
    void acceptsAnEmptyLedger() {
        when(repository.findAllInChainOrder()).thenReturn(Flux.empty());

        StepVerifier.create(service.verifyChain())
                .expectNextMatches(result -> result.valid() && result.eventsChecked() == 0)
                .verifyComplete();
    }

    @Test
    void reportsTamperedPayload() {
        LedgerEvent first = event("A", "{}", GENESIS);
        LedgerEvent second = event("B", "{\"x\":1}", first.getCurrentHash());
        second.setPayload("{\"x\":2}");
        LedgerEvent third = event("C", "{}", second.getCurrentHash());
        when(repository.findAllInChainOrder()).thenReturn(Flux.just(first, second, third));

        StepVerifier.create(service.verifyChain())
                .expectNextMatches(result -> !result.valid()
                        && result.eventsChecked() == 3
                        && second.getId().equals(result.firstInvalidEventId())
                        && result.reason().contains("content"))
                .verifyComplete();
    }

    @Test
    void reportsBrokenLink() {
        LedgerEvent first = event("A", "{}", GENESIS);
        LedgerEvent orphan = event("B", "{}", "f".repeat(64));
        when(repository.findAllInChainOrder()).thenReturn(Flux.just(first, orphan));

        StepVerifier.create(service.verifyChain())
                .expectNextMatches(result -> !result.valid()
                        && orphan.getId().equals(result.firstInvalidEventId())
                        && result.reason().contains("link"))
                .verifyComplete();
    }

    @Test
    void reportsEventsNotStartingAtGenesis() {
        LedgerEvent headless = event("A", "{}", "e".repeat(64));
        when(repository.findAllInChainOrder()).thenReturn(Flux.just(headless));

        StepVerifier.create(service.verifyChain())
                .expectNextMatches(result -> !result.valid() && headless.getId().equals(result.firstInvalidEventId()))
                .verifyComplete();
    }

    private static LedgerEvent event(String type, String payload, String previousHash) {
        return LedgerEvent.builder()
                .id(UUID.randomUUID())
                .transactionType(type)
                .payload(payload)
                .previousHash(previousHash)
                .currentHash(sha256(previousHash + type + payload))
                .build();
    }

    private static String sha256(String input) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(input.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
