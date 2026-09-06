import json

import psycopg2
from kafka import KafkaConsumer

from common import DB_CONFIG, KAFKA_BROKER, KAFKA_TOPIC_FLIGHT_RAW


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def upsert_record(conn, record):
    # Dedup relies on staging.flight_raw's generated `dedup_key` column
    # (COALESCE(scheduled_departure, scheduled_arrival) + airline + route).
    # Flight numbers alone aren't globally unique across airlines, and
    # scheduled_departure is NULL for arrival-direction rows, so neither
    # column works as a uniqueness key on its own.
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO staging.flight_raw
                (flight_number, airline, origin_iata, destination_iata,
                 scheduled_departure, scheduled_arrival, status, queried_airport, fetched_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (dedup_key) DO UPDATE SET
                status = EXCLUDED.status,
                fetched_at = EXCLUDED.fetched_at
            """,
            (
                record["flight_number"],
                record["airline"],
                record["origin_iata"],
                record["destination_iata"],
                record["scheduled_departure"],
                record["scheduled_arrival"],
                record["status"],
                record["queried_airport"],
                record["fetched_at"],
            ),
        )
    conn.commit()


def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC_FLIGHT_RAW,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="flight-consumer-group",
    )

    conn = get_db_connection()
    print(f"Consumer started. Listening on topic '{KAFKA_TOPIC_FLIGHT_RAW}'...")

    for message in consumer:
        record = message.value
        try:
            upsert_record(conn, record)
            print(f"Upserted: {record['flight_number']} {record['origin_iata']}->{record['destination_iata']}")
        except Exception as e:
            print(f"Error upserting record: {e}")
            conn.rollback()


if __name__ == "__main__":
    main()
