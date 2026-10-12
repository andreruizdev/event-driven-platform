package com.eventdrivenmicroservices.platform.infrastructure.debezium;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

class ConnectorStateMonitorTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    private ConnectorStateMonitor monitor(HttpStatus status, String body) {
        WebClient client = WebClient.builder()
                .exchangeFunction(request -> Mono.just(ClientResponse.create(status)
                        .header("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                        .body(body).build()))
                .build();
        return new ConnectorStateMonitor(client, registry, "c");
    }

    private double gauge(String name) {
        return registry.get("eventdrivenmicroservices.connector." + name).gauge().value();
    }

    @Test
    void reportsRunningConnectorAndTasks() {
        monitor(HttpStatus.OK, "{\"connector\":{\"state\":\"RUNNING\"},\"tasks\":[{\"id\":0,\"state\":\"RUNNING\"}]}").poll().block();

        assertEquals(1, gauge("running"));
        assertEquals(1, gauge("tasks.running"));
        assertEquals(0, gauge("tasks.failed"));
    }

    @Test
    void reportsFailedTaskWhileConnectorStillRunning() {
        monitor(HttpStatus.OK, "{\"connector\":{\"state\":\"RUNNING\"},\"tasks\":[{\"id\":0,\"state\":\"FAILED\"}]}").poll().block();

        assertEquals(1, gauge("running"));
        assertEquals(0, gauge("tasks.running"));
        assertEquals(1, gauge("tasks.failed"));
    }

    @Test
    void reportsPausedConnectorAsNotRunning() {
        monitor(HttpStatus.OK, "{\"connector\":{\"state\":\"PAUSED\"},\"tasks\":[]}").poll().block();

        assertEquals(0, gauge("running"));
    }

    @Test
    void reportsMissingConnectorOrUnreachableConnectAsNotRunning() {
        ConnectorStateMonitor missing = monitor(HttpStatus.NOT_FOUND, "{\"error_code\":404}");
        missing.apply(new com.fasterxml.jackson.databind.ObjectMapper().createObjectNode()
                .set("connector", new com.fasterxml.jackson.databind.ObjectMapper().createObjectNode().put("state", "RUNNING")));
        assertEquals(1, gauge("running"));

        missing.poll().block();

        assertEquals(0, gauge("running"));
        assertEquals(0, gauge("tasks.running"));
    }
}
