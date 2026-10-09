package com.eventdrivenmicroservices.platform;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;

import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class IntegrationTests {

    @Autowired
    private DatabaseClient databaseClient;

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
        .withDatabaseName("eventdrivenmicroservices_test_db")
        .withUsername("test")
        .withPassword("test");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.r2dbc.url", () -> envOrDefault("SPRING_R2DBC_URL",
                () -> postgres.getJdbcUrl().replace("jdbc", "r2dbc")));
        registry.add("spring.r2dbc.username", () -> envOrDefault("SPRING_R2DBC_USERNAME", postgres::getUsername));
        registry.add("spring.r2dbc.password", () -> envOrDefault("SPRING_R2DBC_PASSWORD", postgres::getPassword));
        registry.add("spring.flyway.url", () -> envOrDefault("SPRING_FLYWAY_URL", postgres::getJdbcUrl));
        registry.add("spring.flyway.user", () -> envOrDefault("SPRING_FLYWAY_USER", postgres::getUsername));
        registry.add("spring.flyway.password", () -> envOrDefault("SPRING_FLYWAY_PASSWORD", postgres::getPassword));
    }

    private static String envOrDefault(String name, Supplier<String> fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback.get() : value;
    }

    @Test
    void contextLoadsAndDatabaseStarts() {
        assertTrue(postgres.isRunning());
        StepVerifier.create(databaseClient.sql("SELECT 1")
                .map((row, metadata) -> row.get(0, Integer.class))
                .one())
            .expectNext(1)
            .verifyComplete();
    }
}
