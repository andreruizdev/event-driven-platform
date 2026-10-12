package com.eventdrivenmicroservices.platform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import com.eventdrivenmicroservices.platform.infrastructure.kafka.AnomalyAlertOutboxRepository;
import com.eventdrivenmicroservices.platform.infrastructure.kafka.OutboxCdcBridge;
import com.eventdrivenmicroservices.platform.infrastructure.observability.OutboxMetrics;
import com.eventdrivenmicroservices.platform.infrastructure.outbox.EventEnvelope;
import com.eventdrivenmicroservices.platform.infrastructure.postgres.LedgerEventRepository;
import com.eventdrivenmicroservices.platform.infrastructure.postgres.LoanApplicationRepository;
import com.eventdrivenmicroservices.platform.model.LoanApplication;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.UUID;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers(disabledWithoutDocker = true)
@Import(E2EOutboxIntegrationTest.FaultInjectingTraceConfiguration.class)
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
        registry.add("app.outbox.publisher.mode", () -> "poller");
        registry.add("spring.kafka.producer.properties.max.block.ms", () -> "5000");
        registry.add("spring.kafka.producer.properties.request.timeout.ms", () -> "4000");
        registry.add("spring.kafka.producer.properties.delivery.timeout.ms", () -> "8000");
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

    @LocalServerPort
    private int localServerPort;

    @Autowired
    private com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEventRepository outboxEventRepository;

    @Autowired
    private LoanApplicationRepository loanApplicationRepository;

    @Autowired
    private LedgerEventRepository ledgerEventRepository;

    @Autowired
    private AnomalyAlertOutboxRepository anomalyAlertOutboxRepository;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private DatabaseClient databaseClient;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private FaultInjectingTraceSupport faultInjectingTraceSupport;

    @Test
    void loanSubmissionPersistsPublishesAndAppendsLedgerEvent() throws Exception {
        Assertions.assertTrue(postgres.isRunning());
        Assertions.assertTrue(kafka.isRunning());

        var request = new com.eventdrivenmicroservices.platform.model.LoanApplicationRequest(
            UUID.randomUUID().toString(), new java.math.BigDecimal("25000.00"), 36);

        String password = System.getenv().getOrDefault("SPRING_SECURITY_USER_PASSWORD", "local-dev-password");

        var response = webTestClient.post()
                .uri("/api/v1/loans")
                .headers(headers -> headers.setBasicAuth("admin", password))
                .bodyValue(request)
                .exchange()
            .expectStatus().isOk()
            .expectBody(LoanApplication.class)
            .returnResult();
        LoanApplication submittedApplication = response.getResponseBody();

        Assertions.assertNotNull(submittedApplication);
        String correlationId = response.getResponseHeaders().getFirst("X-Correlation-Id");
        Assertions.assertNotNull(correlationId);
        Assertions.assertEquals(correlationId, submittedApplication.getCorrelationId());
        Assertions.assertNotNull(loanApplicationRepository.findById(submittedApplication.getId())
            .block(Duration.ofSeconds(5)));

        var outboxEvent = outboxEventRepository.findFirstByAggregateTypeAndAggregateIdOrderByCreatedAtDesc(
                "LoanApplication", submittedApplication.getId().toString())
            .block(Duration.ofSeconds(5));
        Assertions.assertNotNull(outboxEvent);
        Assertions.assertEquals("ApplicationSubmitted", outboxEvent.getEventType());
        Assertions.assertEquals(correlationId, outboxEvent.getCorrelationId());

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
        Assertions.assertEquals(outboxEvent.getId(), ledgerEvent.getOutboxEventId());
        Assertions.assertEquals(correlationId, ledgerEvent.getCorrelationId());
        Assertions.assertEquals(64, ledgerEvent.getCurrentHash().length());

        System.out.println("[E2E] Loan -> PostgreSQL/outbox -> Kafka -> ledger verified.");
    }

    @Test
    void browserCompletesLoanAndVelocityWorkflowsAndShowsEvidence() {
        String password = System.getenv().getOrDefault("SPRING_SECURITY_USER_PASSWORD", "local-dev-password");
        String applicantId = UUID.randomUUID().toString();

        try (Playwright playwright = Playwright.create();
             Browser browser = playwright.chromium().launch(new BrowserType.LaunchOptions().setHeadless(true));
             Page page = browser.newPage()) {
            page.navigate("http://localhost:" + localServerPort);
            page.locator("#username").fill("admin");
            page.locator("#password").fill(password);
            page.locator("#saveCredentials").click();

            page.locator("#applicantId").fill(applicantId);
            page.locator("#submitLoan").click();
            page.locator("#loanResult").waitFor(new com.microsoft.playwright.Locator.WaitForOptions()
                    .setTimeout(30_000));
            page.waitForFunction("id => document.querySelector('#loanResult').textContent.includes(id)", applicantId);
            page.locator("#recentEventsBody").getByText("ApplicationSubmitted")
                    .waitFor(new com.microsoft.playwright.Locator.WaitForOptions().setTimeout(30_000));
            page.locator("#recentEventsBody tr").filter(new com.microsoft.playwright.Locator.FilterOptions()
                            .setHasText("ApplicationSubmitted"))
                    .locator("button")
                    .click();
            page.locator("#workflowTimeline .workflow-steps li").first().waitFor();
            Assertions.assertTrue(page.locator("#workflowTimeline").textContent().contains("accepted"));
            Assertions.assertFalse(page.locator("#workflowTimeline").textContent().contains(applicantId));

            page.locator("#merchantId").fill("browser-test-" + UUID.randomUUID());
            page.locator("#sendBurst").click();
            page.waitForFunction("() => document.querySelector('#transactionResult').textContent.startsWith('Submitted 6 transactions')",
                    null, new Page.WaitForFunctionOptions().setTimeout(30_000));
            page.locator("#recentEventsBody").getByText("TransactionAnomalyDetected").first()
                    .waitFor(new com.microsoft.playwright.Locator.WaitForOptions().setTimeout(30_000));
        }
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
        Assertions.assertEquals(transactionId, anomalyEvent.getCorrelationId());
        // The test context does not adopt inbound headers; live adoption is asserted by scripts/cdc-drill.sh.
        String propagatedTrace = anomalyEvent.getTraceparent();
        Assertions.assertTrue(propagatedTrace != null && propagatedTrace.matches("00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]"),
            "outbox row must carry a W3C traceparent but was " + propagatedTrace);

        ConsumerRecord<String, String> outboxRecord = consumeKafkaRecord(
            "eventdrivenmicroservices-outbox-events", anomalyEvent.getId());
        EventEnvelope envelope = objectMapper.readValue(outboxRecord.value(), EventEnvelope.class);
        Assertions.assertEquals("TransactionAnomalyDetected", envelope.eventType());
        Assertions.assertEquals(propagatedTrace, traceparent(outboxRecord));

        awaitAlertIntent(anomalyEvent.getId());
        Assertions.assertEquals(1, countAlertIntents(anomalyEvent.getId()));
        resendAndAwaitConsumerCommit(outboxRecord);
        Assertions.assertEquals(1, countAlertIntents(anomalyEvent.getId()));

        ConsumerRecord<String, String> alertRecord = consumeKafkaRecord(
            "eventdrivenmicroservices-anomaly-alerts", anomalyEvent.getId());
        Assertions.assertEquals(envelope.eventId(), objectMapper.readValue(alertRecord.value(), EventEnvelope.class).eventId());
        Assertions.assertEquals(propagatedTrace, traceparent(alertRecord));
        awaitProcessed(anomalyEvent.getId());

        var ledgerEvent = ledgerEventRepository.findAllByOrderByCreatedAtDesc()
                .filter(event -> "ANOMALY_FLAGGED".equals(event.getTransactionType()))
                .filter(event -> event.getPayload().contains(transactionId))
                .next()
                .block(Duration.ofSeconds(5));
        Assertions.assertNotNull(ledgerEvent);
    }

    @Test
    void cdcReplayAfterKafkaAcknowledgementAndDatabaseFailurePublishesDuplicateButOneAlertIntent()
            throws Exception {
        UUID eventId = UUID.randomUUID();
        String aggregateId = "cdc-replay-" + eventId;
        Instant createdAt = Instant.now();
        String payload = "{\"transactionId\":\"" + eventId + "\",\"amount\":15000}";
        databaseClient.sql("""
                        INSERT INTO outbox_events
                            (id, aggregate_type, aggregate_id, event_type, payload, traceparent,
                             processed, created_at, locked_by, locked_until)
                        VALUES
                            (:id, 'FinancialTransaction', :aggregateId, 'TransactionAnomalyDetected',
                             :payload, :traceparent, false, :createdAt, 'cdc-crash-window-drill', :lockedUntil)
                        """)
                .bind("id", eventId)
                .bind("aggregateId", aggregateId)
                .bind("payload", payload)
                .bind("traceparent", TRACEPARENT)
                .bind("createdAt", createdAt)
                .bind("lockedUntil", createdAt.plus(Duration.ofMinutes(10)))
                .then().block(Duration.ofSeconds(5));

        OutboxCdcBridge bridge = new OutboxCdcBridge(objectMapper, kafkaTemplate, outboxEventRepository,
                new OutboxMetrics(new SimpleMeterRegistry()));
        ConsumerRecord<String, String> cdcRecord = new ConsumerRecord<>(
                "eventdrivenmicroservices_cdc.public.outbox_events",
                0,
                0L,
                "",
                objectMapper.writeValueAsString(Map.of(
                        "op", "c",
                        "after", Map.of(
                                "id", eventId.toString(),
                                "aggregate_type", "FinancialTransaction",
                                "aggregate_id", aggregateId,
                                "event_type", "TransactionAnomalyDetected",
                                "payload", payload,
                                "traceparent", TRACEPARENT,
                                "created_at", createdAt.toString()))));

        try {
            installProcessedFailureTrigger(eventId);
            Assertions.assertThrows(Exception.class, () -> bridge.forward(cdcRecord),
                    "the DB update must fail only after Kafka acknowledges the canonical publish");
            var afterFailedMark = outboxEventRepository.findById(eventId).block(Duration.ofSeconds(5));
            Assertions.assertNotNull(afterFailedMark);
            Assertions.assertFalse(afterFailedMark.isProcessed());

            List<ConsumerRecord<String, String>> firstAttempt = consumeKafkaRecords(
                    "eventdrivenmicroservices-outbox-events", eventId, 1);
            EventEnvelope firstEnvelope = objectMapper.readValue(firstAttempt.get(0).value(), EventEnvelope.class);
            Assertions.assertEquals(eventId, firstEnvelope.eventId());

            removeProcessedFailureTrigger();
            bridge.forward(cdcRecord);

            List<ConsumerRecord<String, String>> replayed = consumeKafkaRecords(
                    "eventdrivenmicroservices-outbox-events", eventId, 2);
            Assertions.assertEquals(firstAttempt.get(0).value(), replayed.get(1).value());
            Assertions.assertNotEquals(firstAttempt.get(0).offset(), replayed.get(1).offset());
            awaitProcessed(eventId);
            awaitConsumerCommit(replayed.get(1));
            awaitAlertIntent(eventId);
            Assertions.assertEquals(1, countAlertIntents(eventId));
        } finally {
            removeProcessedFailureTrigger();
        }
    }

    @Test
    void consumerRedeliversAfterIntentCommitBeforeOffsetCommit() throws Exception {
        UUID eventId = UUID.randomUUID();
        EventEnvelope envelope = new EventEnvelope(
                eventId,
                "ANOMALY_VELOCITY_SPIKE",
                1,
                "FinancialTransaction",
                "consumer-replay-" + eventId,
                TRACEPARENT,
                Instant.now(),
                "{}");
        ProducerRecord<String, String> input = new ProducerRecord<>(
                "eventdrivenmicroservices-outbox-events",
                envelope.aggregateId(),
                objectMapper.writeValueAsString(envelope));
        input.headers().add("traceparent", TRACEPARENT.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        faultInjectingTraceSupport.armFailureAfterIntentCommit();

        try {
            var metadata = kafkaTemplate.send(input).get(30, TimeUnit.SECONDS).getRecordMetadata();
            TopicPartition partition = new TopicPartition(metadata.topic(), metadata.partition());
            Assertions.assertTrue(faultInjectingTraceSupport.awaitIntentCommit(Duration.ofSeconds(15)),
                    "listener must reach the injected failure after persisting its alert intent");
            Assertions.assertEquals(1, countAlertIntents(eventId));
            assertOffsetNotCommittedPast(partition, metadata.offset());

            faultInjectingTraceSupport.allowListenerFailure();
            awaitConsumerCommit(partition, metadata.offset());
            Assertions.assertEquals(1, faultInjectingTraceSupport.injectedFailureCount());
            Assertions.assertEquals(1, countAlertIntents(eventId),
                    "redelivery after durable intent creation must not create a duplicate intent");
        } finally {
            faultInjectingTraceSupport.allowListenerFailure();
            faultInjectingTraceSupport.disarm();
        }
    }

    @Test
    void alertRelayRetriesDuringKafkaOutageAndPublishesAfterRecovery() throws Exception {
        UUID eventId = UUID.randomUUID();
        EventEnvelope envelope = new EventEnvelope(eventId, "ANOMALY_VELOCITY_SPIKE", 1,
                "FinancialTransaction", "alert-outage-" + eventId, TRACEPARENT, Instant.now(), "{}");
        String body = objectMapper.writeValueAsString(envelope);
        var dockerClient = kafka.getDockerClient();
        boolean paused = false;
        try {
            dockerClient.pauseContainerCmd(kafka.getContainerId()).exec();
            paused = true;
            Assertions.assertEquals(1, anomalyAlertOutboxRepository.insertIfAbsent(
                    eventId, envelope.aggregateId(), body, TRACEPARENT, Instant.now()).block(Duration.ofSeconds(5)));

            awaitAlertRelayState(eventId, "retry_count >= 1 AND last_error IS NOT NULL AND published_at IS NULL", 60);

            dockerClient.unpauseContainerCmd(kafka.getContainerId()).exec();
            paused = false;
            awaitAlertRelayState(eventId, "published_at IS NOT NULL AND last_error IS NULL", 60);
            Assertions.assertTrue(consumeKafkaRecords("eventdrivenmicroservices-anomaly-alerts", eventId, 1).size() >= 1);
        } finally {
            if (paused) {
                dockerClient.unpauseContainerCmd(kafka.getContainerId()).exec();
            }
        }
    }

    private void awaitAlertRelayState(UUID eventId, String condition, int timeoutSeconds) throws Exception {
        Instant deadline = Instant.now().plusSeconds(timeoutSeconds);
        while (Instant.now().isBefore(deadline)) {
            Long matches = databaseClient.sql("SELECT COUNT(*) AS n FROM anomaly_alert_outbox WHERE event_id = :id AND "
                            + condition)
                    .bind("id", eventId)
                    .map((row, metadata) -> row.get("n", Long.class))
                    .one()
                    .block(Duration.ofSeconds(5));
            if (matches != null && matches == 1) {
                return;
            }
            java.util.concurrent.locks.LockSupport.parkNanos(Duration.ofMillis(500).toNanos());
        }
        throw new AssertionError("Alert intent " + eventId + " never reached state: " + condition);
    }

    private void installProcessedFailureTrigger(UUID eventId) {
        databaseClient.sql("""
                        CREATE OR REPLACE FUNCTION e2e_fail_processed_mark() RETURNS trigger
                        LANGUAGE plpgsql AS $function$
                        BEGIN
                            IF NEW.id = '%s'::uuid AND NEW.processed AND NOT OLD.processed THEN
                                RAISE EXCEPTION 'injected CDC processed-mark failure';
                            END IF;
                            RETURN NEW;
                        END;
                        $function$
                        """.formatted(eventId))
                .then().block(Duration.ofSeconds(5));
        databaseClient.sql("DROP TRIGGER IF EXISTS e2e_fail_processed_mark ON outbox_events")
                .then().block(Duration.ofSeconds(5));
        databaseClient.sql("""
                        CREATE TRIGGER e2e_fail_processed_mark
                        BEFORE UPDATE ON outbox_events
                        FOR EACH ROW EXECUTE FUNCTION e2e_fail_processed_mark()
                        """)
                .then().block(Duration.ofSeconds(5));
    }

    private void removeProcessedFailureTrigger() {
        databaseClient.sql("DROP TRIGGER IF EXISTS e2e_fail_processed_mark ON outbox_events")
                .then().block(Duration.ofSeconds(5));
        databaseClient.sql("DROP FUNCTION IF EXISTS e2e_fail_processed_mark()")
                .then().block(Duration.ofSeconds(5));
    }

    private ConsumerRecord<String, String> consumeKafkaRecord(String topic, UUID eventId) throws Exception {
        return consumeKafkaRecords(topic, eventId, 1).get(0);
    }

    private List<ConsumerRecord<String, String>> consumeKafkaRecords(String topic, UUID eventId, int count)
            throws Exception {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "e2e-outbox-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(topic));
            List<ConsumerRecord<String, String>> matches = new ArrayList<>();
            Instant deadline = Instant.now().plusSeconds(30);
            while (Instant.now().isBefore(deadline) && matches.size() < count) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                    EventEnvelope envelope = objectMapper.readValue(record.value(), EventEnvelope.class);
                    if (eventId.equals(envelope.eventId())) {
                        matches.add(record);
                        if (matches.size() == count) {
                            return matches;
                        }
                    }
                }
            }
        }

        throw new AssertionError("Expected " + count + " records for event " + eventId
                + " on topic " + topic + " within 30 seconds");
    }

    @Autowired
    private com.eventdrivenmicroservices.platform.application.LedgerService ledgerService;

    @Autowired
    private com.eventdrivenmicroservices.platform.application.DataComplianceService dataComplianceService;

    @Test
    void retentionDropsExpiredPartitionsAndKeepsRecentData() {
        var expiredHour = java.time.OffsetDateTime.now(java.time.ZoneOffset.UTC)
                .minusDays(8).truncatedTo(java.time.temporal.ChronoUnit.HOURS);
        String expiredPartition = "telemetry_events_y" + expiredHour.format(
                java.time.format.DateTimeFormatter.ofPattern("yyyy'm'MM'd'dd'h'HH"));
        String expiredEnd = expiredHour.plusHours(1).toString();
        databaseClient.sql("CREATE TABLE IF NOT EXISTS " + expiredPartition + " PARTITION OF telemetry_events "
                        + "FOR VALUES FROM ('" + expiredHour + "') TO ('" + expiredEnd + "')")
                .then().block(Duration.ofSeconds(5));
        String insert = "INSERT INTO telemetry_events (device_id, timestamp, log_level, raw_payload) "
                + "VALUES ('retention-drill', '%s', 'INFO', '{}')";
        databaseClient.sql(insert.formatted(expiredHour.plusMinutes(15))).then().block(Duration.ofSeconds(5));
        databaseClient.sql("INSERT INTO telemetry_events (device_id, timestamp, log_level, raw_payload) "
                        + "VALUES ('retention-default-drill', '2020-01-15 00:00:00+00', 'INFO', '{}')")
                .then().block(Duration.ofSeconds(5));
        databaseClient.sql(insert.formatted(java.time.OffsetDateTime.now(java.time.ZoneOffset.UTC))).then()
                .block(Duration.ofSeconds(5));

        dataComplianceService.executeDataPurge().block(Duration.ofSeconds(30));

        Long expiredPartitions = databaseClient.sql(
                        "SELECT count(*) AS c FROM pg_class WHERE relname = '" + expiredPartition + "'")
                .map((row, meta) -> row.get("c", Long.class)).one().block(Duration.ofSeconds(5));
        Long remaining = databaseClient.sql(
                        "SELECT count(*) AS c FROM telemetry_events WHERE device_id = 'retention-drill'")
                .map((row, meta) -> row.get("c", Long.class)).one().block(Duration.ofSeconds(5));
        Long expiredOverflowRows = databaseClient.sql(
                        "SELECT count(*) AS c FROM telemetry_events WHERE device_id = 'retention-default-drill'")
                .map((row, meta) -> row.get("c", Long.class)).one().block(Duration.ofSeconds(5));
        var futureHour = java.time.OffsetDateTime.now(java.time.ZoneOffset.UTC)
                .truncatedTo(java.time.temporal.ChronoUnit.HOURS).plusHours(168);
        String futurePartition = "telemetry_events_y" + futureHour.format(
                java.time.format.DateTimeFormatter.ofPattern("yyyy'm'MM'd'dd'h'HH"));
        Long futurePartitions = databaseClient.sql(
                        "SELECT count(*) AS c FROM pg_class WHERE relname = '" + futurePartition + "'")
                .map((row, meta) -> row.get("c", Long.class)).one().block(Duration.ofSeconds(5));

        Assertions.assertEquals(0L, expiredPartitions);
        Assertions.assertEquals(1L, remaining);
        Assertions.assertEquals(0L, expiredOverflowRows);
        Assertions.assertEquals(1L, futurePartitions);
    }

    @Test
    void concurrentLedgerAppendsKeepAVerifiableChainAndRejectMutation() {
        long before = ledgerEventRepository.count().block(Duration.ofSeconds(5));

        var appended = reactor.core.publisher.Flux.range(0, 20)
                .flatMap(i -> ledgerService.appendToLedger("ConcurrentAppend", "{\"i\":" + i + "}"), 20)
                .collectList()
                .block(Duration.ofSeconds(30));

        Assertions.assertEquals(20, appended.size());
        Assertions.assertEquals(before + 20, ledgerEventRepository.count().block(Duration.ofSeconds(5)));
        Assertions.assertEquals(20, appended.stream().map(e -> e.getPreviousHash()).distinct().count());

        var verification = ledgerService.verifyChain().block(Duration.ofSeconds(10));
        Assertions.assertTrue(verification.valid(), () -> "Ledger chain invalid: " + verification);
        Assertions.assertEquals(before + 20, verification.eventsChecked());

        StepVerifier.create(databaseClient.sql("UPDATE ledger_events SET payload = 'tampered'").fetch().rowsUpdated())
                .expectErrorMatches(e -> e.getMessage().contains("append-only"))
                .verify(Duration.ofSeconds(5));
        StepVerifier.create(databaseClient.sql("DELETE FROM ledger_events").fetch().rowsUpdated())
                .expectErrorMatches(e -> e.getMessage().contains("append-only"))
                .verify(Duration.ofSeconds(5));
    }

    private void resendAndAwaitConsumerCommit(ConsumerRecord<String, String> originalRecord) throws Exception {
        ProducerRecord<String, String> duplicate = new ProducerRecord<>(
                originalRecord.topic(), originalRecord.key(), originalRecord.value());
        Header traceparent = originalRecord.headers().lastHeader("traceparent");
        if (traceparent != null) {
            duplicate.headers().add("traceparent", traceparent.value());
        }
        var metadata = kafkaTemplate.send(duplicate).get(30, TimeUnit.SECONDS).getRecordMetadata();
        awaitConsumerCommit(new TopicPartition(metadata.topic(), metadata.partition()), metadata.offset());
    }

    private void awaitConsumerCommit(ConsumerRecord<String, String> record) throws Exception {
        awaitConsumerCommit(new TopicPartition(record.topic(), record.partition()), record.offset());
    }

    private void awaitConsumerCommit(TopicPartition topicPartition, long recordOffset) throws Exception {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (AdminClient adminClient = AdminClient.create(properties)) {
            StepVerifier.create(Mono.fromCallable(() -> {
                        var offsets = adminClient.listConsumerGroupOffsets(
                                        "eventdrivenmicroservices-anomaly-cep-group")
                                .partitionsToOffsetAndMetadata()
                                .get(5, TimeUnit.SECONDS);
                        var committed = offsets.get(topicPartition);
                        return committed != null && committed.offset() > recordOffset;
                    })
                    .filter(Boolean::booleanValue)
                    .repeatWhenEmpty(retries -> retries.delayElements(Duration.ofMillis(100)).take(50)))
                    .expectNext(true)
                    .expectComplete()
                    .verify(Duration.ofSeconds(8));
        }
    }

    private void assertOffsetNotCommittedPast(TopicPartition topicPartition, long recordOffset) throws Exception {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (AdminClient adminClient = AdminClient.create(properties)) {
            var offsets = adminClient.listConsumerGroupOffsets("eventdrivenmicroservices-anomaly-cep-group")
                    .partitionsToOffsetAndMetadata()
                    .get(5, TimeUnit.SECONDS);
            var committed = offsets.get(topicPartition);
            Assertions.assertTrue(committed == null || committed.offset() <= recordOffset,
                    () -> "consumer offset advanced past unacknowledged record " + recordOffset + ": " + committed);
        }
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

    @TestConfiguration(proxyBeanMethods = false)
    static class FaultInjectingTraceConfiguration {
        @Bean
        @Primary
        FaultInjectingTraceSupport faultInjectingTraceSupport(
                io.micrometer.tracing.Tracer tracer,
                io.opentelemetry.api.trace.Tracer otelTracer) {
            return new FaultInjectingTraceSupport(tracer, otelTracer);
        }
    }

    static class FaultInjectingTraceSupport
            extends com.eventdrivenmicroservices.platform.infrastructure.observability.TraceSupport {
        private final AtomicBoolean failAfterIntentCommit = new AtomicBoolean();
        private final AtomicInteger injectedFailures = new AtomicInteger();
        private volatile CountDownLatch intentCommitted = new CountDownLatch(0);
        private volatile CountDownLatch allowFailure = new CountDownLatch(0);

        FaultInjectingTraceSupport(io.micrometer.tracing.Tracer tracer,
                                   io.opentelemetry.api.trace.Tracer otelTracer) {
            super(tracer, otelTracer);
        }

        void armFailureAfterIntentCommit() {
            intentCommitted = new CountDownLatch(1);
            allowFailure = new CountDownLatch(1);
            injectedFailures.set(0);
            failAfterIntentCommit.set(true);
        }

        boolean awaitIntentCommit(Duration timeout) throws InterruptedException {
            return intentCommitted.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }

        void allowListenerFailure() {
            allowFailure.countDown();
        }

        int injectedFailureCount() {
            return injectedFailures.get();
        }

        void disarm() {
            failAfterIntentCommit.set(false);
        }

        @Override
        public void runInSpan(String name, String traceparent, String tagKey, String tagValue,
                              ThrowingRunnable work) throws Exception {
            super.runInSpan(name, traceparent, tagKey, tagValue, () -> {
                work.run();
                if (failAfterIntentCommit.get() && "ANOMALY_VELOCITY_SPIKE".equals(tagValue)) {
                    intentCommitted.countDown();
                    if (!allowFailure.await(15, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Timed out awaiting consumer crash-window test release");
                    }
                    if (failAfterIntentCommit.compareAndSet(true, false)) {
                        injectedFailures.incrementAndGet();
                        throw new IllegalStateException("Injected consumer failure after alert intent commit");
                    }
                }
            });
        }
    }
}
