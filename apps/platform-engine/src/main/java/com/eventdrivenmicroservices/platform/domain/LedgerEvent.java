package com.eventdrivenmicroservices.platform.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.OffsetDateTime;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("ledger_events")
public class LedgerEvent {
    @Id
    private UUID id;
    private String transactionType;
    private String payload;
    private String currentHash;
    private String previousHash;
    private OffsetDateTime createdAt;
}
