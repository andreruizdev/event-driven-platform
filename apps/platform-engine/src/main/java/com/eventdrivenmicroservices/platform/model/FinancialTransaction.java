package com.eventdrivenmicroservices.platform.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.mapping.Column;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.math.BigDecimal;

@Table("financial_transactions")
@Data
@NoArgsConstructor
public class FinancialTransaction {

    @Id
    private UUID id;

    @Column("transaction_id")
    private String transactionId;

    @Column("amount")
    private BigDecimal amount;

    @Column("currency")
    private String currency;

    @Column("merchant_id")
    private String merchantId;

    @Column("ip_address")
    private String ipAddress;

    @Column("geolocation")
    private String geolocation;

    @Column("device_fingerprint")
    private String deviceFingerprint;

    @Column("correlation_id")
    private String correlationId;

    @Column("settlement_status")
    private String settlementStatus;

    @Column("timestamp")
    private OffsetDateTime timestamp;

    @Column("created_at")
    private OffsetDateTime createdAt = OffsetDateTime.now();
}
