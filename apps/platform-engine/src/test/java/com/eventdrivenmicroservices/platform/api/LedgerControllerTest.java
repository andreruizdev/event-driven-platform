package com.eventdrivenmicroservices.platform.api;

import com.eventdrivenmicroservices.platform.domain.LedgerEvent;
import com.eventdrivenmicroservices.platform.infrastructure.postgres.LedgerEventRepository;
import com.eventdrivenmicroservices.platform.security.SecurityConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.UUID;

import static org.mockito.Mockito.when;

@WebFluxTest(LedgerController.class)
@Import(SecurityConfig.class)
@TestPropertySource(properties = "spring.security.user.password=test")
class LedgerControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockBean
    private LedgerEventRepository ledgerEventRepository;

    @MockBean
    private com.eventdrivenmicroservices.platform.application.LedgerService ledgerService;

    @Test
    void returnsChainVerification() {
        when(ledgerService.verifyChain()).thenReturn(Mono.just(
                new com.eventdrivenmicroservices.platform.application.LedgerVerification(true, 3, null, null)));

        webTestClient.get()
            .uri("/api/v1/ledger/verify")
            .headers(headers -> headers.setBasicAuth("admin", "test"))
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.valid").isEqualTo(true)
                .jsonPath("$.eventsChecked").isEqualTo(3);
    }

    @Test
    void limitsLedgerListAndBoundsVerifyWindow() {
        LedgerEvent[] events = new LedgerEvent[5];
        for (int i = 0; i < events.length; i++) {
            events[i] = LedgerEvent.builder().id(UUID.randomUUID()).transactionType("T" + i).payload("{}")
                    .previousHash("p").currentHash("c" + i).createdAt(OffsetDateTime.now()).build();
        }
        when(ledgerEventRepository.findAllByOrderByCreatedAtDesc()).thenReturn(Flux.just(events));
        when(ledgerService.verifyRecent(1_000)).thenReturn(Mono.just(
                new com.eventdrivenmicroservices.platform.application.LedgerVerification(true, 1_000, null, null, "RECENT:1000")));

        webTestClient.get().uri("/api/v1/ledger?limit=2")
                .headers(headers -> headers.setBasicAuth("admin", "test"))
                .exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.length()").isEqualTo(2);

        webTestClient.get().uri("/api/v1/ledger/verify?recent=1000")
                .headers(headers -> headers.setBasicAuth("admin", "test"))
                .exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.scope").isEqualTo("RECENT:1000");
    }

    @Test
    void listsLedgerEvents() {
        LedgerEvent event = LedgerEvent.builder()
                .id(UUID.randomUUID())
                .transactionType("ApplicationSubmitted")
                .payload("{}")
                .previousHash("previous")
                .currentHash("current")
                .createdAt(OffsetDateTime.now())
                .build();
        when(ledgerEventRepository.findAllByOrderByCreatedAtDesc()).thenReturn(Flux.just(event));

        webTestClient.get()
            .uri("/api/v1/ledger")
            .headers(headers -> headers.setBasicAuth("admin", "test"))
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$[0].transactionType").isEqualTo("ApplicationSubmitted")
                .jsonPath("$[0].currentHash").isEqualTo("current");
    }

    @Test
    void returnsLatestHash() {
        when(ledgerEventRepository.findLatestHash()).thenReturn(Mono.just("latest"));

        webTestClient.get()
            .uri("/api/v1/ledger/latest-hash")
            .headers(headers -> headers.setBasicAuth("admin", "test"))
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.currentHash").isEqualTo("latest");
    }
}
