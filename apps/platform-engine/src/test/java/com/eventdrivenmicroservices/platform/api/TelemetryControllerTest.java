package com.eventdrivenmicroservices.platform.api;

import com.eventdrivenmicroservices.platform.model.TelemetryPayload;
import com.eventdrivenmicroservices.platform.application.TelemetryProcessingService;
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

import java.time.OffsetDateTime;

import static org.mockito.ArgumentMatchers.any;

@WebFluxTest(controllers = TelemetryController.class)
@Import(SecurityConfig.class)
@TestPropertySource(properties = "spring.security.user.password=test")
public class TelemetryControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockBean
    private TelemetryProcessingService processingService;

    @Test
    public void testReceiveTelemetry_ReturnsAccepted() {
        TelemetryPayload payload = new TelemetryPayload(
                "device-001",
                OffsetDateTime.now(),
                "INFO",
                "{\"cpu_load\": 45.2, \"mem_usage\": 12.8}"
        );

        Mockito.when(processingService.processPayloadAsync(any(TelemetryPayload.class)))
                .thenReturn(Mono.empty());

        webTestClient.post()
                .uri("/api/v1/telemetry")
                .headers(headers -> headers.setBasicAuth("admin", "test"))
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(payload)
                .exchange()
                .expectStatus().isAccepted();

        Mockito.verify(processingService, Mockito.times(1)).processPayloadAsync(any(TelemetryPayload.class));
    }
}
