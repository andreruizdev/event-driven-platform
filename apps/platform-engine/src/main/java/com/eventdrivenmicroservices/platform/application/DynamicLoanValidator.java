package com.eventdrivenmicroservices.platform.application;

import com.eventdrivenmicroservices.platform.config.LoanValidationProperties;
import com.eventdrivenmicroservices.platform.exception.LoanLimitExceededException;
import com.eventdrivenmicroservices.platform.model.LoanApplicationRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class DynamicLoanValidator {

    private final LoanValidationProperties properties;

    public void validate(LoanApplicationRequest request) {
        LoanValidationProperties.Tier activeTier = properties.getActiveTier();
        LoanValidationProperties.TierConfig config = properties.getTiers() != null ? properties.getTiers().get(activeTier) : null;

        if (config == null) {
            throw new IllegalStateException("Validation tier configuration missing for: " + activeTier);
        }

        if (request.getAmount() != null && request.getAmount().compareTo(config.getMaxAmount()) > 0) {
            throw new LoanLimitExceededException(
                String.format("Requested amount exceeds the maximum allowed limit of %s for tier %s",
                    config.getMaxAmount(), activeTier)
            );
        }

        if (request.getTermMonths() != null && request.getTermMonths() > config.getMaxTermMonths()) {
            throw new LoanLimitExceededException(
                String.format("Requested term exceeds the maximum allowed limit of %d months for tier %s",
                    config.getMaxTermMonths(), activeTier)
            );
        }
    }
}
