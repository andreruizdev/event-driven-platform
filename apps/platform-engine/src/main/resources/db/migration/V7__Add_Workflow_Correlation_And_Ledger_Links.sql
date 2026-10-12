ALTER TABLE outbox_events
    ADD COLUMN correlation_id VARCHAR(255);

CREATE INDEX idx_outbox_correlation_created
    ON outbox_events(correlation_id, created_at DESC)
    WHERE correlation_id IS NOT NULL;

ALTER TABLE loan_applications
    ADD COLUMN correlation_id VARCHAR(255);

ALTER TABLE telemetry_events
    ADD COLUMN correlation_id VARCHAR(255);

ALTER TABLE ledger_events
    ADD COLUMN outbox_event_id UUID,
    ADD COLUMN correlation_id VARCHAR(255);

CREATE INDEX idx_ledger_outbox_event
    ON ledger_events(outbox_event_id)
    WHERE outbox_event_id IS NOT NULL;

CREATE INDEX idx_ledger_correlation
    ON ledger_events(correlation_id, created_at DESC)
    WHERE correlation_id IS NOT NULL;
