package com.eventdrivenmicroservices.platform.infrastructure.outbox;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.mapping.Column;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.Instant;
import java.util.UUID;

@Table("outbox_events")
@Data
@NoArgsConstructor
public class OutboxEvent {
    @Id
    private UUID id;

    private String aggregateType;
    private String aggregateId;
    private String eventType;
    
    @Column("payload")
    private String payload;
    
    private String traceparent;

    @Column("processed")
    private boolean processed = false;

    @Column("processed_at")
    private Instant processedAt;

    @Column("created_at")
    private Instant createdAt;

    @Column("retry_count")
    private int retryCount = 0;

    @Column("last_error")
    private String lastError;

    @Column("locked_by")
    private String lockedBy;

    @Column("locked_until")
    private Instant lockedUntil;

    // R2DBC doesn't support @PrePersist, handle in service layer
}
