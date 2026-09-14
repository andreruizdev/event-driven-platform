package com.eventdrivenmicroservices.platform.model;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Pattern;
import lombok.Data;

import java.math.BigDecimal;

@Data
public class LoanApplicationRequest {
    @NotBlank(message = "Applicant ID is required")
    @Pattern(regexp = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$", message = "Applicant ID must be a valid UUID")
    private String applicantId;

    @NotNull(message = "Amount is required")
    @Positive(message = "Amount must be positive")
    private BigDecimal amount;

    @NotNull(message = "Term months is required")
    @Min(value = 1, message = "Term must be at least 1 month")
    private Integer termMonths;

    public LoanApplicationRequest() {
    }

    public LoanApplicationRequest(String applicantId, BigDecimal amount, Integer termMonths) {
        this.applicantId = applicantId;
        this.amount = amount;
        this.termMonths = termMonths;
    }
}
