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

    // R2DBC doesn't support @PrePersist, handle in service layer
}
