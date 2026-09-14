package com.eventdrivenmicroservices.platform.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.OffsetDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TelemetryPayload {
    private String deviceId;
    private OffsetDateTime timestamp;
    private String logLevel;
    private String rawPayload;
}
