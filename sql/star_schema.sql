CREATE SCHEMA IF NOT EXISTS warehouse;

-- Dimension: City
CREATE TABLE IF NOT EXISTS warehouse.dim_city (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(50) UNIQUE NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);

-- Dimension: Date
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_id SERIAL PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- Fact: Hourly weather
CREATE TABLE IF NOT EXISTS warehouse.fact_weather_hourly (
    fact_id SERIAL PRIMARY KEY,
    city_id INT NOT NULL REFERENCES warehouse.dim_city(city_id),
    date_id INT NOT NULL REFERENCES warehouse.dim_date(date_id),
    hour INT NOT NULL,
    avg_temperature DOUBLE PRECISION,
    avg_windspeed DOUBLE PRECISION,
    max_windspeed DOUBLE PRECISION,
    dominant_weathercode INT,
    reading_count INT,
    UNIQUE (city_id, date_id, hour)
);

-- Fact: Daily weather
CREATE TABLE IF NOT EXISTS warehouse.fact_weather_daily (
    fact_id SERIAL PRIMARY KEY,
    city_id INT NOT NULL REFERENCES warehouse.dim_city(city_id),
    date_id INT NOT NULL REFERENCES warehouse.dim_date(date_id),
    min_temperature DOUBLE PRECISION,
    max_temperature DOUBLE PRECISION,
    avg_temperature DOUBLE PRECISION,
    avg_windspeed DOUBLE PRECISION,
    UNIQUE (city_id, date_id)
);