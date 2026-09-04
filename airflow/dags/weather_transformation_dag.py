from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.timetables.interval import CronDataIntervalTimetable

CONN_ID = "weather_postgres"

with DAG(
    dag_id="weather_transformation_pipeline",
    start_date=datetime(2026, 7, 1),
    schedule=CronDataIntervalTimetable("0 * * * *", timezone="UTC"),
    catchup=False,
    tags=["weather", "warehouse"],
) as dag:

    # Step 1: Populate dim_city (idempotent upsert)
    populate_dim_city = SQLExecuteQueryOperator(
        task_id="populate_dim_city",
        conn_id=CONN_ID,
        sql="""
            INSERT INTO warehouse.dim_city (city_name, latitude, longitude)
            SELECT DISTINCT city, latitude, longitude
            FROM staging.weather_raw
            ON CONFLICT (city_name) DO NOTHING;
        """,
    )

    # Step 2: Populate dim_date (idempotent upsert)
    populate_dim_date = SQLExecuteQueryOperator(
        task_id="populate_dim_date",
        conn_id=CONN_ID,
        sql="""
            INSERT INTO warehouse.dim_date (full_date, year, month, day, day_of_week, is_weekend)
            SELECT DISTINCT
                event_timestamp::date,
                EXTRACT(YEAR FROM event_timestamp)::int,
                EXTRACT(MONTH FROM event_timestamp)::int,
                EXTRACT(DAY FROM event_timestamp)::int,
                EXTRACT(DOW FROM event_timestamp)::int,
                EXTRACT(DOW FROM event_timestamp)::int IN (0, 6)
            FROM staging.weather_raw
            ON CONFLICT (full_date) DO NOTHING;
        """,
    )

    # Step 3: Populate fact_weather_hourly (idempotent upsert)
    populate_fact_hourly = SQLExecuteQueryOperator(
        task_id="populate_fact_weather_hourly",
        conn_id=CONN_ID,
        sql="""
            INSERT INTO warehouse.fact_weather_hourly
                (city_id, date_id, hour, avg_temperature, avg_windspeed, max_windspeed, dominant_weathercode, reading_count)
            SELECT
                dc.city_id,
                dd.date_id,
                EXTRACT(HOUR FROM wr.event_timestamp)::int AS hour,
                ROUND(AVG(wr.temperature)::numeric, 2),
                ROUND(AVG(wr.windspeed)::numeric, 2),
                MAX(wr.windspeed),
                MODE() WITHIN GROUP (ORDER BY wr.weathercode),
                COUNT(*)
            FROM staging.weather_raw wr
            JOIN warehouse.dim_city dc ON dc.city_name = wr.city
            JOIN warehouse.dim_date dd ON dd.full_date = wr.event_timestamp::date
            GROUP BY dc.city_id, dd.date_id, EXTRACT(HOUR FROM wr.event_timestamp)
            ON CONFLICT (city_id, date_id, hour) DO UPDATE SET
                avg_temperature = EXCLUDED.avg_temperature,
                avg_windspeed = EXCLUDED.avg_windspeed,
                max_windspeed = EXCLUDED.max_windspeed,
                dominant_weathercode = EXCLUDED.dominant_weathercode,
                reading_count = EXCLUDED.reading_count;
        """,
    )

    # Step 4: Populate fact_weather_daily (idempotent upsert)
    populate_fact_daily = SQLExecuteQueryOperator(
        task_id="populate_fact_weather_daily",
        conn_id=CONN_ID,
        sql="""
            INSERT INTO warehouse.fact_weather_daily
                (city_id, date_id, min_temperature, max_temperature, avg_temperature, avg_windspeed)
            SELECT
                dc.city_id,
                dd.date_id,
                ROUND(MIN(wr.temperature)::numeric, 2),
                ROUND(MAX(wr.temperature)::numeric, 2),
                ROUND(AVG(wr.temperature)::numeric, 2),
                ROUND(AVG(wr.windspeed)::numeric, 2)
            FROM staging.weather_raw wr
            JOIN warehouse.dim_city dc ON dc.city_name = wr.city
            JOIN warehouse.dim_date dd ON dd.full_date = wr.event_timestamp::date
            GROUP BY dc.city_id, dd.date_id
            ON CONFLICT (city_id, date_id) DO UPDATE SET
                min_temperature = EXCLUDED.min_temperature,
                max_temperature = EXCLUDED.max_temperature,
                avg_temperature = EXCLUDED.avg_temperature,
                avg_windspeed = EXCLUDED.avg_windspeed;
        """,
    )

    populate_dim_city >> populate_dim_date >> populate_fact_hourly >> populate_fact_daily