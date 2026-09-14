package com.eventdrivenmicroservices.platform.application;

import com.eventdrivenmicroservices.platform.config.LoanValidationProperties;
import com.eventdrivenmicroservices.platform.exception.LoanLimitExceededException;
import com.eventdrivenmicroservices.platform.model.LoanApplicationRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class DynamicLoanValidatorTest {

    private DynamicLoanValidator validator;
    private LoanValidationProperties properties;

    @BeforeEach
    void setUp() {
        properties = new LoanValidationProperties();
        properties.setActiveTier(LoanValidationProperties.Tier.MEDIUM);

        Map<LoanValidationProperties.Tier, LoanValidationProperties.TierConfig> tiers = new HashMap<>();

        LoanValidationProperties.TierConfig mediumConfig = new LoanValidationProperties.TierConfig();
        mediumConfig.setMaxAmount(new BigDecimal("50000.00"));
        mediumConfig.setMaxTermMonths(60);
        tiers.put(LoanValidationProperties.Tier.MEDIUM, mediumConfig);

        properties.setTiers(tiers);
        validator = new DynamicLoanValidator(properties);
    }

    @Test
    void validate_ValidRequest_Passes() {
        LoanApplicationRequest request = new LoanApplicationRequest(
                UUID.randomUUID().toString(),
                new BigDecimal("25000.00"),
                36
        );

        validator.validate(request);
    }

    @Test
    void validate_ExceedsMaxAmount_ThrowsException() {
        LoanApplicationRequest request = new LoanApplicationRequest(
                UUID.randomUUID().toString(),
                new BigDecimal("60000.00"),
                36
        );

        LoanLimitExceededException exception = assertThrows(
                LoanLimitExceededException.class,
                () -> validator.validate(request)
        );

        assertTrue(exception.getMessage().contains("maximum allowed limit of 50000.00"));
    }

    @Test
    void validate_ExceedsMaxTermMonths_ThrowsException() {
        LoanApplicationRequest request = new LoanApplicationRequest(
                UUID.randomUUID().toString(),
                new BigDecimal("20000.00"),
                72
        );

        LoanLimitExceededException exception = assertThrows(
                LoanLimitExceededException.class,
                () -> validator.validate(request)
        );

        assertTrue(exception.getMessage().contains("maximum allowed limit of 60 months"));
    }
}
