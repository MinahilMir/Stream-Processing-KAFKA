-- View 1: Hourly summary per city (for trend charts)
CREATE OR REPLACE VIEW staging.v_hourly_weather_summary AS
SELECT
    city,
    date_trunc('hour', event_timestamp) AS hour,
    ROUND(AVG(temperature)::numeric, 2) AS avg_temperature,
    ROUND(AVG(windspeed)::numeric, 2) AS avg_windspeed,
    MAX(windspeed) AS max_windspeed,
    MODE() WITHIN GROUP (ORDER BY weathercode) AS dominant_weathercode,
    COUNT(*) AS reading_count
FROM staging.weather_raw
GROUP BY city, date_trunc('hour', event_timestamp)
ORDER BY city, hour;

-- View 2: Latest reading per city (for a "current conditions" card)
CREATE OR REPLACE VIEW staging.v_latest_weather_by_city AS
SELECT DISTINCT ON (city)
    city,
    temperature,
    windspeed,
    winddirection,
    weathercode,
    event_timestamp
FROM staging.weather_raw
ORDER BY city, event_timestamp DESC;

-- View 3: Daily min/max/avg per city (for daily climate summary)
CREATE OR REPLACE VIEW staging.v_daily_climate_summary AS
SELECT
    city,
    date_trunc('day', event_timestamp)::date AS day,
    ROUND(MIN(temperature)::numeric, 2) AS min_temperature,
    ROUND(MAX(temperature)::numeric, 2) AS max_temperature,
    ROUND(AVG(temperature)::numeric, 2) AS avg_temperature,
    ROUND(AVG(windspeed)::numeric, 2) AS avg_windspeed
FROM staging.weather_raw
GROUP BY city, date_trunc('day', event_timestamp)
ORDER BY city, day;

-- View 4: City comparison / benchmarking (for a comparison chart)
CREATE OR REPLACE VIEW staging.v_city_benchmarks AS
SELECT
    city,
    ROUND(AVG(temperature)::numeric, 2) AS overall_avg_temperature,
    ROUND(AVG(windspeed)::numeric, 2) AS overall_avg_windspeed,
    COUNT(*) AS total_readings
FROM staging.weather_raw
GROUP BY city
ORDER BY overall_avg_temperature DESC;

-- View 5: Latest known status per tracked flight (for Flight Risk Advisor's status board)
CREATE OR REPLACE VIEW staging.v_latest_flight_status AS
SELECT DISTINCT ON (flight_number, origin_iata, destination_iata, COALESCE(scheduled_departure, scheduled_arrival))
    flight_number,
    airline,
    origin_iata,
    destination_iata,
    scheduled_departure,
    scheduled_arrival,
    status,
    fetched_at
FROM staging.flight_raw
ORDER BY flight_number, origin_iata, destination_iata, COALESCE(scheduled_departure, scheduled_arrival), fetched_at DESC;

-- View 6: Route-level summary (for a route risk/reliability chart)
CREATE OR REPLACE VIEW staging.v_flight_route_summary AS
SELECT
    origin_iata,
    destination_iata,
    COUNT(*) AS total_flights,
    COUNT(*) FILTER (WHERE status = 'Canceled') AS cancelled_flights,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE status = 'Canceled') / NULLIF(COUNT(*), 0), 2
    ) AS cancellation_rate_pct
FROM staging.flight_raw
GROUP BY origin_iata, destination_iata
ORDER BY origin_iata, destination_iata;

-- View 7: AeroDataBox API usage against the free-tier cap (for a pipeline-health card)
CREATE OR REPLACE VIEW staging.v_api_usage_summary AS
SELECT
    provider,
    endpoint,
    usage_date,
    request_count,
    unit_count
FROM staging.api_usage_tracker
ORDER BY usage_date DESC;