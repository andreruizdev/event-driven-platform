package com.eventdrivenmicroservices.platform.api;

import com.eventdrivenmicroservices.platform.application.LoanService;
import com.eventdrivenmicroservices.platform.model.LoanApplication;
import com.eventdrivenmicroservices.platform.model.LoanApplicationRequest;
import com.eventdrivenmicroservices.platform.security.SecurityConfig;
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
import java.time.Instant;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;

@WebFluxTest(controllers = LoanController.class)
@Import(SecurityConfig.class)
@TestPropertySource(properties = "spring.security.user.password=test")
public class LoanControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockBean
    private LoanService loanService;

    @Test
    void testSubmitLoanApplication_Success() {
        String applicantId = UUID.randomUUID().toString();
        LoanApplicationRequest request = new LoanApplicationRequest(applicantId, new BigDecimal("10000.00"), 24);

        LoanApplication response = new LoanApplication();
        response.setId(UUID.randomUUID());
        response.setApplicantId(applicantId);
        response.setAmount(new BigDecimal("10000.00"));
        response.setTermMonths(24);
        response.setStatus("PENDING_REVIEW");
        response.setCreatedAt(Instant.now());

        Mockito.when(loanService.submitApplication(any(LoanApplicationRequest.class)))
                .thenReturn(Mono.just(response));

        webTestClient.post()
                .uri("/api/v1/loans")
                .headers(headers -> headers.setBasicAuth("admin", "test"))
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.status").isEqualTo("PENDING_REVIEW")
                .jsonPath("$.applicantId").isEqualTo(applicantId);
    }

    @Test
    void testSubmitLoanApplication_ValidationError_InvalidUUID() {
        LoanApplicationRequest request = new LoanApplicationRequest("invalid-uuid", new BigDecimal("10000.00"), 24);

        webTestClient.post()
                .uri("/api/v1/loans")
                .headers(headers -> headers.setBasicAuth("admin", "test"))
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void testSubmitLoanApplication_Unauthorized() {
        String applicantId = UUID.randomUUID().toString();
        LoanApplicationRequest request = new LoanApplicationRequest(applicantId, new BigDecimal("10000.00"), 24);

        webTestClient.post()
                .uri("/api/v1/loans")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isUnauthorized();
    }
}
