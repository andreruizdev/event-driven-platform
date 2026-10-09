CREATE TABLE anomaly_alert_outbox (
    event_id UUID PRIMARY KEY,
    aggregate_id VARCHAR(255) NOT NULL,
    envelope TEXT NOT NULL,
    traceparent VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    retry_count INT NOT NULL DEFAULT 0,
    last_error TEXT,
    locked_until TIMESTAMPTZ
);

CREATE INDEX idx_anomaly_alert_outbox_pending
    ON anomaly_alert_outbox(locked_until, created_at)
    WHERE published_at IS NULL;