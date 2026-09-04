import json
import psycopg2
from kafka import KafkaConsumer

from common import KAFKA_BROKER, KAFKA_TOPIC_RAW, DB_CONFIG


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def insert_record(conn, record):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO staging.weather_raw
                (city, latitude, longitude, temperature, windspeed, winddirection, weathercode, event_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                record["city"],
                record["latitude"],
                record["longitude"],
                record["temperature"],
                record["windspeed"],
                record["winddirection"],
                record["weathercode"],
                record["timestamp"],
            ),
        )
    conn.commit()


def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC_RAW,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="weather-consumer-group",
    )

    conn = get_db_connection()
    print(f"Consumer started. Listening on topic '{KAFKA_TOPIC_RAW}'...")

    for message in consumer:
        record = message.value
        try:
            insert_record(conn, record)
            print(f"Inserted: {record['city']} @ {record['timestamp']}")
        except Exception as e:
            print(f"Error inserting record: {e}")
            conn.rollback()


if __name__ == "__main__":
    main()