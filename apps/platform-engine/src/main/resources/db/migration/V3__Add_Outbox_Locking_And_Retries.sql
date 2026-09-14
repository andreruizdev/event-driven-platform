-- V3: Add outbox locking, retry tracking, and failure error fields
ALTER TABLE outbox_events 
    ADD COLUMN IF NOT EXISTS retry_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_error TEXT,
    ADD COLUMN IF NOT EXISTS locked_by VARCHAR(255),
    ADD COLUMN IF NOT EXISTS locked_until TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_outbox_unprocessed_lease 
    ON outbox_events(processed, locked_until, created_at) 
    WHERE processed = FALSE;
