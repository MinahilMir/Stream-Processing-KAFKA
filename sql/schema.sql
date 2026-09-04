CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.weather_raw (
    id SERIAL PRIMARY KEY,
    city VARCHAR(50) NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    temperature DOUBLE PRECISION,
    windspeed DOUBLE PRECISION,
    winddirection DOUBLE PRECISION,
    weathercode INT,
    event_timestamp TIMESTAMPTZ NOT NULL,
    inserted_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS staging.weather_aggregates (
    id SERIAL PRIMARY KEY,
    city VARCHAR(50) NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    avg_temperature DOUBLE PRECISION,
    max_windspeed DOUBLE PRECISION,
    dominant_weathercode INT,
    inserted_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging.weather_alerts (
    id SERIAL PRIMARY KEY,
    city VARCHAR(50) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    alert_message TEXT,
    triggered_value DOUBLE PRECISION,
    threshold_value DOUBLE PRECISION,
    event_timestamp TIMESTAMPTZ NOT NULL,
    inserted_at TIMESTAMPTZ DEFAULT now()
);