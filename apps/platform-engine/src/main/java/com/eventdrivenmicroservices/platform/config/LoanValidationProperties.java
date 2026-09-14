package com.eventdrivenmicroservices.platform.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;
import java.util.Map;

@Data
@Configuration
@ConfigurationProperties(prefix = "app.validation.loan")
public class LoanValidationProperties {

    private Tier activeTier = Tier.MEDIUM;
    private Map<Tier, TierConfig> tiers;

    public enum Tier {
        LIGHT, MEDIUM, HIGH
    }

    @Data
    public static class TierConfig {
        private BigDecimal maxAmount;
        private Integer maxTermMonths;
    }
}
