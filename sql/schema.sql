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