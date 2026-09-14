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
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

@WebFluxTest(controllers = TransactionController.class)
@Import(SecurityConfig.class)
@TestPropertySource(properties = "spring.security.user.password=test")
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

        webTestClient.post()
                .uri("/api/v1/transactions")
                .headers(headers -> headers.setBasicAuth("admin", "test"))
                .header("traceparent", "trace-123")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(payload)
                .exchange()
                .expectStatus().isAccepted();

        Mockito.verify(processingService, Mockito.times(1)).processTransactionAsync(any(FinancialTransactionPayload.class), eq("trace-123"));
    }
}
