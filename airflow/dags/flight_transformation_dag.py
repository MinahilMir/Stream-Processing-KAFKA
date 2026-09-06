from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.timetables.interval import CronDataIntervalTimetable

CONN_ID = "weather_postgres"

with DAG(
    dag_id="flight_transformation_pipeline",
    start_date=datetime(2026, 8, 1),
    schedule=CronDataIntervalTimetable("0 * * * *", timezone="UTC"),
    catchup=False,
    tags=["flight", "warehouse"],
) as dag:

    # Step 1: Populate dim_airport (idempotent upsert)
    populate_dim_airport = SQLExecuteQueryOperator(
        task_id="populate_dim_airport",
        conn_id=CONN_ID,
        sql="""
            INSERT INTO warehouse.dim_airport (iata_code)
            SELECT DISTINCT origin_iata FROM staging.flight_raw WHERE origin_iata IS NOT NULL
            UNION
            SELECT DISTINCT destination_iata FROM staging.flight_raw WHERE destination_iata IS NOT NULL
            ON CONFLICT (iata_code) DO NOTHING;
        """,
    )

    # Step 2: Populate dim_date from flight events (shared table with the
    # weather DAG; ON CONFLICT DO NOTHING makes running both DAGs safe).
    populate_dim_date = SQLExecuteQueryOperator(
        task_id="populate_dim_date",
        conn_id=CONN_ID,
        sql="""
            INSERT INTO warehouse.dim_date (full_date, year, month, day, day_of_week, is_weekend)
            SELECT DISTINCT
                d.event_date,
                EXTRACT(YEAR FROM d.event_date)::int,
                EXTRACT(MONTH FROM d.event_date)::int,
                EXTRACT(DAY FROM d.event_date)::int,
                EXTRACT(DOW FROM d.event_date)::int,
                EXTRACT(DOW FROM d.event_date)::int IN (0, 6)
            FROM (
                SELECT COALESCE(scheduled_departure, scheduled_arrival, fetched_at)::date AS event_date
                FROM staging.flight_raw
            ) d
            ON CONFLICT (full_date) DO NOTHING;
        """,
    )

    # Step 3: Populate fact_flight_daily (idempotent upsert)
    populate_fact_flight_daily = SQLExecuteQueryOperator(
        task_id="populate_fact_flight_daily",
        conn_id=CONN_ID,
        sql="""
            INSERT INTO warehouse.fact_flight_daily
                (origin_airport_id, destination_airport_id, date_id, airline, scheduled_flights, cancelled_flights)
            SELECT
                da_origin.airport_id,
                da_dest.airport_id,
                dd.date_id,
                COALESCE(fr.airline, ''),
                COUNT(*) FILTER (WHERE fr.status IS DISTINCT FROM 'Canceled'),
                COUNT(*) FILTER (WHERE fr.status = 'Canceled')
            FROM staging.flight_raw fr
            JOIN warehouse.dim_airport da_origin ON da_origin.iata_code = fr.origin_iata
            JOIN warehouse.dim_airport da_dest ON da_dest.iata_code = fr.destination_iata
            JOIN warehouse.dim_date dd ON dd.full_date = COALESCE(fr.scheduled_departure, fr.scheduled_arrival, fr.fetched_at)::date
            GROUP BY da_origin.airport_id, da_dest.airport_id, dd.date_id, fr.airline
            ON CONFLICT (origin_airport_id, destination_airport_id, date_id, airline) DO UPDATE SET
                scheduled_flights = EXCLUDED.scheduled_flights,
                cancelled_flights = EXCLUDED.cancelled_flights;
        """,
    )

    populate_dim_airport >> populate_dim_date >> populate_fact_flight_daily
