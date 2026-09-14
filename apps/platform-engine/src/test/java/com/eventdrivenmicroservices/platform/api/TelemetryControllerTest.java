package com.eventdrivenmicroservices.platform.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.eventdrivenmicroservices.platform.model.TelemetryPayload;
import com.eventdrivenmicroservices.platform.application.TelemetryProcessingService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import java.time.OffsetDateTime;

import static org.mockito.ArgumentMatchers.any;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(TelemetryController.class)
public class TelemetryControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private TelemetryProcessingService processingService;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    public void testReceiveTelemetry_ReturnsAccepted() throws Exception {
        TelemetryPayload payload = new TelemetryPayload(
                "device-001",
                OffsetDateTime.now(),
                "INFO",
                "{\"cpu_load\": 45.2, \"mem_usage\": 12.8}"
        );

        String password = System.getenv().getOrDefault("SPRING_SECURITY_USER_PASSWORD", "local-dev-password");

        mockMvc.perform(post("/api/v1/telemetry")
                .with(org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic("admin", password))
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(payload)))
                .andExpect(status().isAccepted());

        Mockito.verify(processingService, Mockito.times(1)).processPayloadAsync(any(TelemetryPayload.class));
    }
}
