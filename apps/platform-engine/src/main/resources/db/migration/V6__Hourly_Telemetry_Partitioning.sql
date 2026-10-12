-- Convert the monthly telemetry parent to UTC-hour partitions while preserving every existing row.
-- The application lifecycle drops fully expired hour partitions; only out-of-range overflow stays
-- in the default partition.
DO $$
DECLARE
    current_hour TIMESTAMP WITHOUT TIME ZONE := date_trunc('hour', now() AT TIME ZONE 'UTC');
    first_hour TIMESTAMP WITHOUT TIME ZONE;
    last_hour TIMESTAMP WITHOUT TIME ZONE;
    partition_hour TIMESTAMP WITHOUT TIME ZONE;
    partition_name TEXT;
BEGIN
    LOCK TABLE telemetry_events IN ACCESS EXCLUSIVE MODE;

    SELECT date_trunc('hour', min(timestamp) AT TIME ZONE 'UTC')
    INTO first_hour
    FROM telemetry_events;

    IF first_hour IS NULL THEN
        first_hour := current_hour - INTERVAL '7 days';
    END IF;

    last_hour := current_hour + INTERVAL '168 hours';

    CREATE TABLE telemetry_events_hourly (
        id UUID DEFAULT uuid_generate_v4(),
        device_id VARCHAR(255) NOT NULL,
        timestamp TIMESTAMPTZ NOT NULL,
        log_level VARCHAR(50) NOT NULL,
        raw_payload TEXT NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (id, timestamp)
    ) PARTITION BY RANGE (timestamp);

    CREATE INDEX telemetry_events_hourly_device_timestamp_idx
        ON telemetry_events_hourly(device_id, timestamp DESC);

    partition_hour := first_hour;
    WHILE partition_hour <= last_hour LOOP
        partition_name := 'telemetry_events_y' ||
                to_char(partition_hour, 'YYYY"m"MM"d"DD"h"HH24');
        EXECUTE format(
                'CREATE TABLE %I PARTITION OF telemetry_events_hourly FOR VALUES FROM (%L) TO (%L)',
                partition_name,
                partition_hour AT TIME ZONE 'UTC',
                (partition_hour + INTERVAL '1 hour') AT TIME ZONE 'UTC');
        partition_hour := partition_hour + INTERVAL '1 hour';
    END LOOP;

    CREATE TABLE telemetry_events_hourly_default
        PARTITION OF telemetry_events_hourly DEFAULT;
    CREATE INDEX telemetry_events_hourly_default_timestamp_idx
        ON telemetry_events_hourly_default(timestamp);

    INSERT INTO telemetry_events_hourly
        (id, device_id, timestamp, log_level, raw_payload, created_at)
    SELECT id, device_id, timestamp, log_level, raw_payload, created_at
    FROM telemetry_events;

    DROP TABLE telemetry_events;
    ALTER TABLE telemetry_events_hourly RENAME TO telemetry_events;
    ALTER TABLE telemetry_events_hourly_default RENAME TO telemetry_events_default;
    ALTER INDEX telemetry_events_hourly_pkey RENAME TO telemetry_events_pkey;
    ALTER INDEX telemetry_events_hourly_device_timestamp_idx RENAME TO idx_telemetry_device_timestamp;
    ALTER INDEX telemetry_events_hourly_default_timestamp_idx RENAME TO telemetry_events_default_timestamp_idx;
END
$$;
