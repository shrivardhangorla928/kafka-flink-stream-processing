-- AWARE alert store.
--
-- Written in the SQL subset that PostgreSQL 16 and H2 2.x (MODE=PostgreSQL) both accept, so the
-- repository tests run these exact migrations in-memory instead of testing a Hibernate-generated
-- schema that nobody deploys. Consequences of that choice, all deliberate:
--   * no "timestamptz" shorthand - the ANSI "timestamp with time zone" spelling is used instead;
--   * no partial / expression indexes;
--   * no ON CONFLICT - idempotency is enforced by the primary key plus a read-before-write in
--     AlertPersistenceService, which keeps the upsert portable and lets the service count
--     duplicates rather than swallowing them.

CREATE TABLE IF NOT EXISTS alerts (
    -- Deterministic UUID from AlertIds.deterministicId, stored as text: the Flink sink is
    -- at-least-once, so this key is the only thing standing between a checkpoint replay and a
    -- duplicated flood warning.
    alert_id         VARCHAR(64)              NOT NULL,

    station_id       VARCHAR(64)              NOT NULL,
    station_name     VARCHAR(255),
    district_id      VARCHAR(64),

    sensor_type      VARCHAR(32)              NOT NULL,
    severity         VARCHAR(16)              NOT NULL,
    -- Severity.rank() denormalised so the "WARNING and above" filter is an indexable integer
    -- comparison instead of a CASE expression the planner cannot use an index for.
    severity_rank    INTEGER                  NOT NULL,

    metric           VARCHAR(64),
    observed_value   DOUBLE PRECISION         NOT NULL,
    threshold_value  DOUBLE PRECISION         NOT NULL,
    unit             VARCHAR(16),

    window_start     TIMESTAMP WITH TIME ZONE,
    window_end       TIMESTAMP WITH TIME ZONE,

    message          VARCHAR(1024),

    latitude         DOUBLE PRECISION         NOT NULL,
    longitude        DOUBLE PRECISION         NOT NULL,

    -- Produced by Flink; the API's sort key and the range filter.
    generated_at     TIMESTAMP WITH TIME ZONE NOT NULL,
    -- Written by this service; generated_at -> received_at is the sink-side lag.
    received_at      TIMESTAMP WITH TIME ZONE NOT NULL,

    acknowledged     BOOLEAN                  NOT NULL DEFAULT FALSE,
    acknowledged_at  TIMESTAMP WITH TIME ZONE,
    acknowledged_by  VARCHAR(128),

    CONSTRAINT pk_alerts PRIMARY KEY (alert_id)
);

-- Indexes are chosen from the query API's access paths, not one per column. Every listing
-- endpoint sorts by generated_at DESC, so generated_at is the trailing column of each composite
-- index: that lets the same index satisfy the filter and the ordering in one range scan.
--
-- Deliberately NOT indexed: sensor_type (3 values) and acknowledged (2 values). Their
-- selectivity is far too low to lead an index; they are evaluated as filters on rows already
-- narrowed by one of the indexes below.

-- Unfiltered and time-range-only listings, and the /summary aggregation.
CREATE INDEX IF NOT EXISTS idx_alerts_generated_at
    ON alerts (generated_at DESC);

-- District dashboards: the most common filter in the real platform.
CREATE INDEX IF NOT EXISTS idx_alerts_district_generated_at
    ON alerts (district_id, generated_at DESC);

-- Per-station drill-down from the map.
CREATE INDEX IF NOT EXISTS idx_alerts_station_generated_at
    ON alerts (station_id, generated_at DESC);

-- "severity >= WARNING" listings; the rank ordering makes this a plain range scan.
CREATE INDEX IF NOT EXISTS idx_alerts_severity_rank_generated_at
    ON alerts (severity_rank, generated_at DESC);


-- Windowed aggregates, kept for the observability feed and the per-station timeline endpoint.
-- (station_id, sensor_type, window_start) is the natural key: Flink re-emits a window verbatim
-- after a restart, so a replay must land on the same row rather than accumulating history.
CREATE TABLE IF NOT EXISTS station_window_aggregates (
    station_id       VARCHAR(64)              NOT NULL,
    sensor_type      VARCHAR(32)              NOT NULL,
    window_start     TIMESTAMP WITH TIME ZONE NOT NULL,

    window_end       TIMESTAMP WITH TIME ZONE NOT NULL,
    station_name     VARCHAR(255),
    district_id      VARCHAR(64),
    unit             VARCHAR(16),

    reading_count    BIGINT                   NOT NULL,
    sum_value        DOUBLE PRECISION         NOT NULL,
    min_value        DOUBLE PRECISION         NOT NULL,
    max_value        DOUBLE PRECISION         NOT NULL,
    avg_value        DOUBLE PRECISION         NOT NULL,
    aggregated_value DOUBLE PRECISION         NOT NULL,

    latitude         DOUBLE PRECISION         NOT NULL,
    longitude        DOUBLE PRECISION         NOT NULL,

    computed_at      TIMESTAMP WITH TIME ZONE,
    received_at      TIMESTAMP WITH TIME ZONE NOT NULL,

    CONSTRAINT pk_station_window_aggregates PRIMARY KEY (station_id, sensor_type, window_start)
);

-- The timeline endpoint reads the most recent windows for one station, optionally narrowed to
-- one sensor type; the primary key already leads with station_id but orders by sensor_type
-- before window_start, so it cannot serve "latest N for this station" on its own.
CREATE INDEX IF NOT EXISTS idx_aggregates_station_window_start
    ON station_window_aggregates (station_id, window_start DESC);
