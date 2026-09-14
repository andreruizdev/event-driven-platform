package com.eventdrivenmicroservices.platform.anomaly;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AnomalyScore {
    private boolean isAnomaly;
    private double score;
    private String anomalyType;
    private String details;
    private Instant timestamp;

    public static AnomalyScore normal() {
        return AnomalyScore.builder()
                .isAnomaly(false)
                .score(0.0)
                .anomalyType("NONE")
                .details("Normal baseline activity")
                .timestamp(Instant.now())
                .build();
    }

    public static AnomalyScore flagged(String anomalyType, double score, String details) {
        return AnomalyScore.builder()
                .isAnomaly(true)
                .score(score)
                .anomalyType(anomalyType)
                .details(details)
                .timestamp(Instant.now())
                .build();
    }
}
