-- Staging table for raw flight events polled from AeroDataBox.
--
-- dedup_key exists because flight numbers are not globally unique across
-- airlines, and scheduled_departure is NULL for arrival-direction rows (so a
-- plain UNIQUE on scheduled_departure + flight_number silently misses
-- duplicates, since Postgres treats NULL <> NULL). COALESCE-ing departure/
-- arrival time together with airline and route gives a stable key regardless
-- of which side of the AeroDataBox response the row came from.
CREATE TABLE IF NOT EXISTS staging.flight_raw (
    id SERIAL PRIMARY KEY,
    flight_number VARCHAR(20),
    airline VARCHAR(100),
    origin_iata VARCHAR(10),
    destination_iata VARCHAR(10),
    scheduled_departure TIMESTAMPTZ,
    scheduled_arrival TIMESTAMPTZ,
    status VARCHAR(30),
    queried_airport VARCHAR(10) NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    inserted_at TIMESTAMPTZ DEFAULT now(),
    dedup_key TEXT GENERATED ALWAYS AS (
        COALESCE(scheduled_departure::text, scheduled_arrival::text) || '|' ||
        COALESCE(airline, '') || '|' ||
        COALESCE(origin_iata, '') || '|' ||
        COALESCE(destination_iata, '')
    ) STORED,
    CONSTRAINT flight_raw_dedup_key_uq UNIQUE (dedup_key)
);

-- Tracks AeroDataBox request/unit consumption per day so we notice we're
-- approaching the free-tier cap before we get a 429.
CREATE TABLE IF NOT EXISTS staging.api_usage_tracker (
    id SERIAL PRIMARY KEY,
    provider VARCHAR(50) NOT NULL,
    endpoint VARCHAR(100) NOT NULL,
    usage_date DATE NOT NULL,
    request_count INT NOT NULL DEFAULT 0,
    unit_count INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT now(),
    CONSTRAINT api_usage_tracker_uq UNIQUE (provider, endpoint, usage_date)
);
