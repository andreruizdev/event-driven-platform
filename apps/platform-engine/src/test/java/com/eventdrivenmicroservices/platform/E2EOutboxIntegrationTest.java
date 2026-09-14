package com.eventdrivenmicroservices.platform;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
public class E2EOutboxIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:15"))
        .withDatabaseName("eventdrivenmicroservices_test")
        .withUsername("testuser")
        .withPassword("testpass");

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.r2dbc.url", () -> "r2dbc:postgresql://" + postgres.getHost() + ":" + postgres.getFirstMappedPort() + "/eventdrivenmicroservices_test");
        registry.add("spring.r2dbc.username", postgres::getUsername);
        registry.add("spring.r2dbc.password", postgres::getPassword);
        registry.add("spring.flyway.url", postgres::getJdbcUrl);
        registry.add("spring.flyway.user", postgres::getUsername);
        registry.add("spring.flyway.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    private org.springframework.test.web.reactive.server.WebTestClient webTestClient;

    @Autowired
    private com.eventdrivenmicroservices.platform.infrastructure.outbox.OutboxEventRepository outboxEventRepository;

    @Test
    void testE2ETransactionFlow() {
        Assertions.assertTrue(postgres.isRunning());
        Assertions.assertTrue(kafka.isRunning());

        com.eventdrivenmicroservices.platform.model.LoanApplicationRequest request = new com.eventdrivenmicroservices.platform.model.LoanApplicationRequest(
                "app-1001",
                new java.math.BigDecimal("25000.00"),
                36
        );

        String password = System.getenv().getOrDefault("SPRING_SECURITY_USER_PASSWORD", "local-dev-password");

        webTestClient.post()
                .uri("/api/v1/loans")
                .headers(headers -> headers.setBasicAuth("admin", password))
                .bodyValue(request)
                .exchange()
                .expectStatus().isOk();

        // Verify that the Outbox event was atomically persisted to PostgreSQL via R2DBC
        reactor.test.StepVerifier.create(outboxEventRepository.findByProcessedFalse())
                .expectNextMatches(event -> "LoanApplication".equals(event.getAggregateType()))
                .thenCancel()
                .verify(java.time.Duration.ofSeconds(5));

        System.out.println("[E2E] End-to-end REST POST -> R2DBC Outbox transaction flow verified successfully.");
    }
}
