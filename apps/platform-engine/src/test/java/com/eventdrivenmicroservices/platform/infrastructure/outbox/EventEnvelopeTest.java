package com.eventdrivenmicroservices.platform.infrastructure.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EventEnvelopeTest {

    @Test
    void serializesStableEventMetadataAndPayload() throws Exception {
        EventEnvelope envelope = new EventEnvelope(
                UUID.fromString("11111111-1111-4111-8111-111111111111"),
                "TransactionAnomalyDetected",
                1,
                "FinancialTransaction",
                "aggregate-1",
                "00-trace-span-flags",
                Instant.parse("2026-09-14T12:00:00Z"),
                "{\"amount\":2000}");

        ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
        var json = objectMapper.readTree(objectMapper.writeValueAsString(envelope));

        assertEquals("TransactionAnomalyDetected", json.get("eventType").asText());
        assertEquals(1, json.get("eventVersion").asInt());
        assertEquals("FinancialTransaction", json.get("aggregateType").asText());
        assertEquals("00-trace-span-flags", json.get("traceparent").asText());
        assertEquals("{\"amount\":2000}", json.get("payload").asText());
    }
}
