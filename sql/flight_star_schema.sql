-- Extends warehouse.sql's star schema with a flights side, reusing
-- warehouse.dim_date from star_schema.sql.

CREATE TABLE IF NOT EXISTS warehouse.dim_airport (
    airport_id SERIAL PRIMARY KEY,
    iata_code VARCHAR(10) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.fact_flight_daily (
    fact_id SERIAL PRIMARY KEY,
    origin_airport_id INT NOT NULL REFERENCES warehouse.dim_airport(airport_id),
    destination_airport_id INT NOT NULL REFERENCES warehouse.dim_airport(airport_id),
    date_id INT NOT NULL REFERENCES warehouse.dim_date(date_id),
    airline VARCHAR(100) NOT NULL DEFAULT '',
    scheduled_flights INT NOT NULL DEFAULT 0,
    cancelled_flights INT NOT NULL DEFAULT 0,
    UNIQUE (origin_airport_id, destination_airport_id, date_id, airline)
);
