package com.eventdrivenmicroservices.platform.infrastructure.postgres;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.format.DateTimeFormatter;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
class TelemetryHourlyMigrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(DockerImageName.parse("postgres:15"))
            .withDatabaseName("telemetry_migration_test")
            .withUsername("migration")
            .withPassword("migration");

    @Test
    void upgradesMonthlyPartitionsWithoutLosingRecentOrOutOfRangeRows() throws Exception {
        OffsetDateTime recent = OffsetDateTime.now(ZoneOffset.UTC)
                .minusHours(2).truncatedTo(ChronoUnit.HOURS).plusMinutes(15);
        OffsetDateTime overflow = OffsetDateTime.now(ZoneOffset.UTC).plusYears(2);
        String recentPartition = "telemetry_events_y" + recent.format(
                DateTimeFormatter.ofPattern("yyyy'm'MM'd'dd'h'HH"));
        Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .target(MigrationVersion.fromVersion("5"))
                .load()
                .migrate();

        try (Connection connection = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             Statement statement = connection.createStatement()) {
            statement.execute("INSERT INTO telemetry_events (device_id, timestamp, log_level, raw_payload) VALUES "
                    + "('migration-recent', '" + recent + "', 'INFO', '{}'), "
                    + "('migration-overflow', '" + overflow + "', 'INFO', '{}')");
        }

        Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .load()
                .migrate();

        try (Connection connection = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("""
                     SELECT device_id, tableoid::regclass::text AS partition_name
                     FROM telemetry_events
                     WHERE device_id IN ('migration-recent', 'migration-overflow')
                     ORDER BY device_id
                     """)) {
            assertThat(result.next()).isTrue();
            assertThat(result.getString("device_id")).isEqualTo("migration-overflow");
            assertThat(result.getString("partition_name")).isEqualTo("telemetry_events_default");
            assertThat(result.next()).isTrue();
            assertThat(result.getString("device_id")).isEqualTo("migration-recent");
            assertThat(result.getString("partition_name")).isEqualTo(recentPartition);
            assertThat(result.next()).isFalse();
        }
    }
}
