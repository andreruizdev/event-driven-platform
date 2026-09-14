-- V1: Initial Schema for EventDrivenMicroservices Data Matrix
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS telemetry_events (
    id UUID DEFAULT uuid_generate_v4(),
    device_id VARCHAR(255) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    log_level VARCHAR(50) NOT NULL,
    raw_payload TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (id, timestamp)
) PARTITION BY RANGE (timestamp);

-- Initial partitions
CREATE TABLE IF NOT EXISTS telemetry_events_y2026m06 PARTITION OF telemetry_events FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
CREATE TABLE IF NOT EXISTS telemetry_events_y2026m07 PARTITION OF telemetry_events FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');

CREATE INDEX IF NOT EXISTS idx_telemetry_device_timestamp ON telemetry_events(device_id, timestamp DESC);

CREATE TABLE IF NOT EXISTS loan_applications (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    applicant_id VARCHAR(255) NOT NULL,
    amount DECIMAL NOT NULL,
    term_months INT NOT NULL,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    payload TEXT NOT NULL,
    traceparent VARCHAR(255),
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_outbox_unprocessed ON outbox_events(processed) WHERE processed = FALSE;

CREATE TABLE IF NOT EXISTS ledger_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_type VARCHAR(50) NOT NULL,
    payload TEXT NOT NULL,
    current_hash VARCHAR(64) NOT NULL,
    previous_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Ensure chain integrity and fast latest hash lookup at database level
CREATE UNIQUE INDEX IF NOT EXISTS idx_ledger_previous_hash ON ledger_events(previous_hash);
CREATE INDEX IF NOT EXISTS idx_ledger_created_at ON ledger_events(created_at DESC);

CREATE TABLE IF NOT EXISTS financial_transactions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id VARCHAR(255) NOT NULL,
    amount DECIMAL NOT NULL,
    currency VARCHAR(10) NOT NULL,
    merchant_id VARCHAR(255),
    ip_address VARCHAR(50),
    geolocation VARCHAR(255),
    device_fingerprint VARCHAR(255),
    correlation_id VARCHAR(255),
    settlement_status VARCHAR(50) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fin_tx_timestamp ON financial_transactions(timestamp DESC);
