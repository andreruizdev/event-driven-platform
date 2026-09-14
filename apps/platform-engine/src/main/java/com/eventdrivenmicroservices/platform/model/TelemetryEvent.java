package com.eventdrivenmicroservices.platform.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.mapping.Column;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.OffsetDateTime;
import java.util.UUID;

@Table("telemetry_events")
@Data
@NoArgsConstructor
public class TelemetryEvent {

    @Id
    private UUID id;

    @Column("device_id")
    private String deviceId;

    @Column("timestamp")
    private OffsetDateTime timestamp;

    @Column("log_level")
    private String logLevel;

    @Column("raw_payload")
    private String rawPayload;

    @Column("created_at")
    private OffsetDateTime createdAt = OffsetDateTime.now();

    public TelemetryEvent(String deviceId, OffsetDateTime timestamp, String logLevel, String rawPayload) {
        this.deviceId = deviceId;
        this.timestamp = timestamp;
        this.logLevel = logLevel;
        this.rawPayload = rawPayload;
    }
}
