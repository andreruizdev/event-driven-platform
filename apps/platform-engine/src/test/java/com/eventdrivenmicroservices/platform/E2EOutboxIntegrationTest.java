package com.eventdrivenmicroservices.platform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.eventdrivenmicroservices.platform.infrastructure.kafka.AnomalyAlertOutboxRepository;
import com.eventdrivenmicroservices.platform.infrastructure.kafka.AnomalyEventListener;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.EventEnvelope;
import com.eventdrivenmicroservices.platform.infrastructure.postgres.LedgerEventRepository;
import com.eventdrivenmicroservices.platform.infrastructure.postgres.LoanApplicationRepository;
import com.eventdrivenmicroservices.platform.model.LoanApplication;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.r2dbc.core.DatabaseClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Supplier;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers(disabledWithoutDocker = true)
public class E2EOutboxIntegrationTest {

    private static final String TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:15"))
        .withDatabaseName("eventdrivenmicroservices_test")
        .withUsername("testuser")
        .withPassword("testpass");

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.r2dbc.url", () -> envOrDefault("SPRING_R2DBC_URL",
                () -> "r2dbc:postgresql://" + postgres.getHost() + ":" + postgres.getFirstMappedPort() + "/eventdrivenmicroservices_test"));
        registry.add("spring.r2dbc.username", () -> envOrDefault("SPRING_R2DBC_USERNAME", postgres::getUsername));
        registry.add("spring.r2dbc.password", () -> envOrDefault("SPRING_R2DBC_PASSWORD", postgres::getPassword));
        registry.add("spring.flyway.url", () -> envOrDefault("SPRING_FLYWAY_URL", postgres::getJdbcUrl));
        registry.add("spring.flyway.user", () -> envOrDefault("SPRING_FLYWAY_USER", postgres::getUsername));
        registry.add("spring.flyway.password", () -> envOrDefault("SPRING_FLYWAY_PASSWORD", postgres::getPassword));
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.data.redis.host", () -> envOrDefault("SPRING_DATA_REDIS_HOST", redis::getHost));
        registry.add("spring.data.redis.port", () -> envOrDefault("SPRING_DATA_REDIS_PORT",
            () -> Integer.toString(redis.getFirstMappedPort())));
    }

    private static String envOrDefault(String name, Supplier<String> fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback.get() : value;
    }

    @Autowired
    private org.springframework.test.web.reactive.server.WebTestClient webTestClient;

    @Autowired
    private com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEventRepository outboxEventRepository;

    @Autowired
    private LoanApplicationRepository loanApplicationRepository;

    @Autowired
    private LedgerEventRepository ledgerEventRepository;

    @Autowired
    private AnomalyEventListener anomalyEventListener;

    @Autowired
    private AnomalyAlertOutboxRepository anomalyAlertOutboxRepository;

    @Autowired
    private DatabaseClient databaseClient;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void loanSubmissionPersistsPublishesAndAppendsLedgerEvent() throws Exception {
        Assertions.assertTrue(postgres.isRunning());
        Assertions.assertTrue(kafka.isRunning());

        var request = new com.eventdrivenmicroservices.platform.model.LoanApplicationRequest(
            UUID.randomUUID().toString(), new java.math.BigDecimal("25000.00"), 36);

        String password = System.getenv().getOrDefault("SPRING_SECURITY_USER_PASSWORD", "local-dev-password");

        LoanApplication submittedApplication = webTestClient.post()
                .uri("/api/v1/loans")
                .headers(headers -> headers.setBasicAuth("admin", password))
                .bodyValue(request)
                .exchange()
            .expectStatus().isOk()
            .expectBody(LoanApplication.class)
            .returnResult()
            .getResponseBody();

        Assertions.assertNotNull(submittedApplication);
        Assertions.assertNotNull(loanApplicationRepository.findById(submittedApplication.getId())
            .block(Duration.ofSeconds(5)));

        var outboxEvent = outboxEventRepository.findFirstByAggregateTypeAndAggregateIdOrderByCreatedAtDesc(
                "LoanApplication", submittedApplication.getId().toString())
            .block(Duration.ofSeconds(5));
        Assertions.assertNotNull(outboxEvent);
        Assertions.assertEquals("ApplicationSubmitted", outboxEvent.getEventType());

        ConsumerRecord<String, String> publishedRecord = consumeKafkaRecord(
            "eventdrivenmicroservices-outbox-events", outboxEvent.getId());
        EventEnvelope envelope = objectMapper.readValue(publishedRecord.value(), EventEnvelope.class);
        Assertions.assertEquals(submittedApplication.getId().toString(), publishedRecord.key());
        Assertions.assertEquals(outboxEvent.getId(), envelope.eventId());
        Assertions.assertEquals("ApplicationSubmitted", envelope.eventType());
        awaitProcessed(outboxEvent.getId());

        var ledgerEvent = ledgerEventRepository.findAllByOrderByCreatedAtDesc()
            .filter(event -> "ApplicationSubmitted".equals(event.getTransactionType()))
            .filter(event -> event.getPayload().contains(submittedApplication.getId().toString()))
            .next()
            .block(Duration.ofSeconds(5));
        Assertions.assertNotNull(ledgerEvent);
        Assertions.assertEquals(submittedApplication.getId().toString(), envelope.aggregateId());
        Assertions.assertEquals(64, ledgerEvent.getCurrentHash().length());

        System.out.println("[E2E] Loan -> PostgreSQL/outbox -> Kafka -> ledger verified.");
    }

    @Test
    void highValueTransactionPublishesAnomalyAlertWithTraceparent() throws Exception {
        Assertions.assertTrue(kafka.isRunning());
        Assertions.assertTrue(redis.isRunning());

        String merchantId = "e2e-merchant-" + UUID.randomUUID();
        String transactionId = UUID.randomUUID().toString();
        webTestClient.post()
            .uri("/api/v1/transactions")
            .headers(headers -> headers.setBasicAuth("admin", System.getenv().getOrDefault(
                "SPRING_SECURITY_USER_PASSWORD", "local-dev-password")))
            .header("traceparent", TRACEPARENT)
            .bodyValue(Map.of(
                "transactionId", transactionId,
                "amount", new java.math.BigDecimal("15000.00"),
                "currency", "USD",
                "merchantId", merchantId,
                "correlationId", transactionId,
                "settlementStatus", "PENDING"))
            .exchange()
            .expectStatus().isAccepted();

        var anomalyEvent = outboxEventRepository
            .findFirstByEventTypeAndAggregateTypeOrderByCreatedAtDesc(
                "TransactionAnomalyDetected", "FinancialTransaction")
            .block(Duration.ofSeconds(5));
        Assertions.assertNotNull(anomalyEvent);
        Assertions.assertTrue(anomalyEvent.getPayload().contains(transactionId));
        Assertions.assertEquals(TRACEPARENT, anomalyEvent.getTraceparent());

        ConsumerRecord<String, String> outboxRecord = consumeKafkaRecord(
            "eventdrivenmicroservices-outbox-events", anomalyEvent.getId());
        EventEnvelope envelope = objectMapper.readValue(outboxRecord.value(), EventEnvelope.class);
        Assertions.assertEquals("TransactionAnomalyDetected", envelope.eventType());
        Assertions.assertEquals(TRACEPARENT, traceparent(outboxRecord));

        awaitAlertIntent(anomalyEvent.getId());
        Assertions.assertEquals(1, countAlertIntents(anomalyEvent.getId()));
        anomalyEventListener.consumeOutboxEvent(outboxRecord);
        Assertions.assertEquals(1, countAlertIntents(anomalyEvent.getId()));

        ConsumerRecord<String, String> alertRecord = consumeKafkaRecord(
            "eventdrivenmicroservices-anomaly-alerts", anomalyEvent.getId());
        Assertions.assertEquals(envelope.eventId(), objectMapper.readValue(alertRecord.value(), EventEnvelope.class).eventId());
        Assertions.assertEquals(TRACEPARENT, traceparent(alertRecord));
        awaitProcessed(anomalyEvent.getId());

        var ledgerEvent = ledgerEventRepository.findAllByOrderByCreatedAtDesc()
                .filter(event -> "ANOMALY_FLAGGED".equals(event.getTransactionType()))
                .filter(event -> event.getPayload().contains(transactionId))
                .next()
                .block(Duration.ofSeconds(5));
        Assertions.assertNotNull(ledgerEvent);
    }

    private ConsumerRecord<String, String> consumeKafkaRecord(String topic, UUID eventId) throws Exception {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "e2e-outbox-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plusSeconds(30);
            while (Instant.now().isBefore(deadline)) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                    EventEnvelope envelope = objectMapper.readValue(record.value(), EventEnvelope.class);
                    if (eventId.equals(envelope.eventId())) {
                        return record;
                    }
            }
            }
        }

        throw new AssertionError("Event was not published to topic " + topic + " within 30 seconds: " + eventId);
    }

    private String traceparent(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader("traceparent");
        return header == null ? null : new String(header.value(), java.nio.charset.StandardCharsets.UTF_8);
    }

    private void awaitProcessed(UUID eventId) {
        reactor.test.StepVerifier.create(outboxEventRepository.findById(eventId)
                        .filter(event -> event.isProcessed())
                        .repeatWhenEmpty(retries -> retries.delayElements(Duration.ofMillis(100)).take(50)))
                .expectNextMatches(event -> event.isProcessed())
                .thenCancel()
                .verify(Duration.ofSeconds(6));
    }

    private void awaitAlertIntent(UUID eventId) {
        reactor.test.StepVerifier.create(anomalyAlertOutboxRepository.findById(eventId)
                        .repeatWhenEmpty(retries -> retries.delayElements(Duration.ofMillis(100)).take(50)))
                .expectNextCount(1)
                .thenCancel()
                .verify(Duration.ofSeconds(6));
    }

    private long countAlertIntents(UUID eventId) {
        Long count = databaseClient.sql("""
                        SELECT COUNT(*) AS intent_count
                        FROM anomaly_alert_outbox
                        WHERE event_id = :eventId
                        """)
                .bind("eventId", eventId)
                .map((row, metadata) -> row.get("intent_count", Long.class))
                .one()
                .block(Duration.ofSeconds(5));
        Assertions.assertNotNull(count);
        return count;
    }
}
