-- V2: Automated Partitioning Lifecycle and Default Partition
-- Fulfills Rule #2 (AGENTS.md): Cold Deletes via Partitioning

-- 1. Forward-looking explicit monthly partitions
CREATE TABLE IF NOT EXISTS telemetry_events_y2026m08 PARTITION OF telemetry_events FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');
CREATE TABLE IF NOT EXISTS telemetry_events_y2026m09 PARTITION OF telemetry_events FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
CREATE TABLE IF NOT EXISTS telemetry_events_y2026m10 PARTITION OF telemetry_events FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');
CREATE TABLE IF NOT EXISTS telemetry_events_y2026m11 PARTITION OF telemetry_events FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
CREATE TABLE IF NOT EXISTS telemetry_events_y2026m12 PARTITION OF telemetry_events FOR VALUES FROM ('2026-12-01') TO ('2027-01-01');
CREATE TABLE IF NOT EXISTS telemetry_events_y2027m01 PARTITION OF telemetry_events FOR VALUES FROM ('2027-01-01') TO ('2027-02-01');
CREATE TABLE IF NOT EXISTS telemetry_events_y2027m02 PARTITION OF telemetry_events FOR VALUES FROM ('2027-02-01') TO ('2027-03-01');
CREATE TABLE IF NOT EXISTS telemetry_events_y2027m03 PARTITION OF telemetry_events FOR VALUES FROM ('2027-03-01') TO ('2027-04-01');
CREATE TABLE IF NOT EXISTS telemetry_events_y2027m04 PARTITION OF telemetry_events FOR VALUES FROM ('2027-04-01') TO ('2027-05-01');
CREATE TABLE IF NOT EXISTS telemetry_events_y2027m05 PARTITION OF telemetry_events FOR VALUES FROM ('2027-05-01') TO ('2027-06-01');
CREATE TABLE IF NOT EXISTS telemetry_events_y2027m06 PARTITION OF telemetry_events FOR VALUES FROM ('2027-06-01') TO ('2027-07-01');

-- 2. Default partition to safely absorb any telemetry outside dedicated monthly ranges without rejection
CREATE TABLE IF NOT EXISTS telemetry_events_default PARTITION OF telemetry_events DEFAULT;
