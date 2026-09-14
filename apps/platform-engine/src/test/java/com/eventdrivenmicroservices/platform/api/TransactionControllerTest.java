package com.eventdrivenmicroservices.platform.api;

import com.eventdrivenmicroservices.platform.security.SecurityConfig;
import com.eventdrivenmicroservices.platform.model.FinancialTransactionPayload;
import com.eventdrivenmicroservices.platform.application.TransactionProcessingService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

@WebFluxTest(controllers = TransactionController.class)
@Import(SecurityConfig.class)
public class TransactionControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockBean
    private TransactionProcessingService processingService;

    @Test
    public void testReceiveTransaction_ReturnsAccepted() {
        FinancialTransactionPayload payload = new FinancialTransactionPayload();
        payload.setTransactionId("tx-123");
        payload.setAmount(new BigDecimal("1500.00"));
        payload.setCurrency("USD");
        payload.setTimestamp(OffsetDateTime.now());

        Mockito.when(processingService.processTransactionAsync(any(FinancialTransactionPayload.class), eq("trace-123")))
                .thenReturn(Mono.empty());

        String password = System.getenv().getOrDefault("SPRING_SECURITY_USER_PASSWORD", "local-dev-password");

        webTestClient
                .mutateWith(SecurityMockServerConfigurers.mockAuthentication(
                        new org.springframework.security.authentication.UsernamePasswordAuthenticationToken("admin", password)
                ))
                .post()
                .uri("/api/v1/transactions")
                .header("traceparent", "trace-123")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(payload)
                .exchange()
                .expectStatus().isAccepted();

        Mockito.verify(processingService, Mockito.times(1)).processTransactionAsync(any(FinancialTransactionPayload.class), eq("trace-123"));
    }
}
