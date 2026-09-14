package com.eventdrivenmicroservices.platform.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.OffsetDateTime;
import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FinancialTransactionPayload {
    private String transactionId;
    private BigDecimal amount;
    private String currency;
    private String merchantId;
    private String ipAddress;
    private String geolocation;
    private String deviceFingerprint;
    private String correlationId;
    private String settlementStatus;
    private OffsetDateTime timestamp;
}
