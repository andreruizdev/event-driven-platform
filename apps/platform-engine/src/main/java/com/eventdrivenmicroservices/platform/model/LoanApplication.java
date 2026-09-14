package com.eventdrivenmicroservices.platform.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.mapping.Column;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Table("loan_applications")
@Data
@NoArgsConstructor
public class LoanApplication {
    @Id
    private UUID id;
    
    private String applicantId;
    private BigDecimal amount;
    private Integer termMonths;
    private String status;
    private Instant createdAt;
    
    // R2DBC doesn't support @PrePersist, handle in service layer
}
