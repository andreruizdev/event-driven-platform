package com.eventdrivenmicroservices.platform.api;

import com.eventdrivenmicroservices.platform.application.LedgerService;
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

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

import static org.mockito.Mockito.when;

@WebFluxTest(LedgerController.class)
@Import(SecurityConfig.class)
@TestPropertySource(properties = "JWT_SECRET_KEY=http-level-bearer-test-secret")
class BearerAuthenticationHttpTest {

    private static final String SECRET = "http-level-bearer-test-secret";

    @Autowired
    private WebTestClient webTestClient;

    @MockBean
    private LedgerEventRepository ledgerEventRepository;

    @MockBean
    private LedgerService ledgerService;

    @Test
    void validBearerTokenIsAuthenticated() throws Exception {
        when(ledgerEventRepository.findAllByOrderByCreatedAtDesc()).thenReturn(Flux.empty());

        webTestClient.get().uri("/api/v1/ledger")
                .headers(h -> h.setBearerAuth(token(SECRET, Instant.now().getEpochSecond() + 600)))
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    void expiredBearerTokenIsRejected() throws Exception {
        webTestClient.get().uri("/api/v1/ledger")
                .headers(h -> h.setBearerAuth(token(SECRET, Instant.now().getEpochSecond() - 600)))
                .exchange()
                .expectStatus().isUnauthorized();
    }

    @Test
    void wrongSecretBearerTokenIsRejected() throws Exception {
        webTestClient.get().uri("/api/v1/ledger")
                .headers(h -> h.setBearerAuth(token("another-secret", Instant.now().getEpochSecond() + 600)))
                .exchange()
                .expectStatus().isUnauthorized();
    }

    @Test
    void missingCredentialsAreRejected() {
        webTestClient.get().uri("/api/v1/ledger").exchange().expectStatus().isUnauthorized();
    }

    @Test
    void bearerTokenCannotReachAdminEndpoints() throws Exception {
        webTestClient.get().uri("/actuator/info")
                .headers(h -> h.setBearerAuth(token(SECRET, Instant.now().getEpochSecond() + 600)))
                .exchange()
                .expectStatus().isForbidden();
    }

    private static String token(String secret, long exp) {
        try {
            return sign(secret, exp);
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String sign(String secret, long exp) throws java.security.GeneralSecurityException {
        String headerAndPayload = encode("{\"alg\":\"HS256\"}") + "." + encode("{\"sub\":\"edge\",\"exp\":" + exp + "}");
        Mac hmac = Mac.getInstance("HmacSHA256");
        hmac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return headerAndPayload + "." + Base64.getUrlEncoder().withoutPadding()
                .encodeToString(hmac.doFinal(headerAndPayload.getBytes(StandardCharsets.UTF_8)));
    }

    private static String encode(String value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
}
