package com.eventdrivenmicroservices.platform.infrastructure.debezium;

import com.fasterxml.jackson.databind.JsonNode;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Exposes the Debezium connector and task state reported by the Kafka Connect REST API. A connector or task
 * in FAILED state stops capturing changes while the process stays up, which health probes cannot see.
 */
@Component
@Slf4j
@ConditionalOnProperty(prefix = "app.outbox.publisher", name = "mode", havingValue = "cdc")
public class ConnectorStateMonitor {

    private final WebClient webClient;
    private final String connectorName;
    private final AtomicLong connectorRunning = new AtomicLong();
    private final AtomicLong tasksRunning = new AtomicLong();
    private final AtomicLong tasksFailed = new AtomicLong();

    @Autowired
    public ConnectorStateMonitor(
            WebClient.Builder builder,
            MeterRegistry registry,
            @Value("${app.outbox.connect.url:http://localhost:8083}") String connectUrl,
            @Value("${app.outbox.connect.connector-name:eventdrivenmicroservices-postgres-outbox-connector}") String connectorName) {
        this(builder.baseUrl(connectUrl).build(), registry, connectorName);
    }

    ConnectorStateMonitor(WebClient webClient, MeterRegistry registry, String connectorName) {
        this.webClient = webClient;
        this.connectorName = connectorName;
        Gauge.builder("eventdrivenmicroservices.connector.running", connectorRunning, AtomicLong::doubleValue)
                .description("1 when the Debezium connector reports RUNNING, 0 when it is failed, paused, missing or unreachable")
                .register(registry);
        Gauge.builder("eventdrivenmicroservices.connector.tasks.running", tasksRunning, AtomicLong::doubleValue)
                .description("Debezium connector tasks in RUNNING state")
                .register(registry);
        Gauge.builder("eventdrivenmicroservices.connector.tasks.failed", tasksFailed, AtomicLong::doubleValue)
                .description("Debezium connector tasks in FAILED state")
                .register(registry);
    }

    @Scheduled(fixedDelayString = "${app.outbox.connect.interval-ms:15000}")
    public void refresh() {
        poll().subscribe();
    }

    Mono<Void> poll() {
        return webClient.get()
                .uri("/connectors/{name}/status", connectorName)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .timeout(Duration.ofSeconds(5))
                .doOnNext(this::apply)
                .doOnError(error -> {
                    log.warn("Connector status refresh failed: {}", error.getMessage());
                    connectorRunning.set(0);
                    tasksRunning.set(0);
                })
                .onErrorResume(error -> Mono.empty())
                .then();
    }

    void apply(JsonNode status) {
        connectorRunning.set("RUNNING".equals(status.path("connector").path("state").asText()) ? 1 : 0);
        long running = 0;
        long failed = 0;
        for (JsonNode task : status.path("tasks")) {
            String state = task.path("state").asText();
            if ("RUNNING".equals(state)) running++;
            if ("FAILED".equals(state)) failed++;
        }
        tasksRunning.set(running);
        tasksFailed.set(failed);
    }
}
