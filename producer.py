import json
import time
import requests
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer

from common import (
    KAFKA_BROKER,
    KAFKA_TOPIC_RAW,
    CITIES,
    OPEN_METEO_FORECAST_URL,
    OPEN_METEO_ARCHIVE_URL,
    POLL_INTERVAL_SECONDS,
)


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def fetch_live_weather(city, lat, lon):
    """Fetch current live weather for a city."""
    url = f"{OPEN_METEO_FORECAST_URL}?latitude={lat}&longitude={lon}&current_weather=true"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()["current_weather"]

    return {
        "city": city,
        "latitude": lat,
        "longitude": lon,
        "temperature": data["temperature"],
        "windspeed": data["windspeed"],
        "winddirection": data["winddirection"],
        "weathercode": data["weathercode"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def fetch_historical_backfill(city, lat, lon, days_back=7):
    """Fetch historical hourly weather for backfill purposes."""
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=days_back)

    url = (
        f"{OPEN_METEO_ARCHIVE_URL}?latitude={lat}&longitude={lon}"
        f"&start_date={start_date}&end_date={end_date}"
        f"&hourly=temperature_2m,windspeed_10m,winddirection_10m,weathercode"
    )
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    data = response.json()["hourly"]

    records = []
    for i, ts in enumerate(data["time"]):
        records.append({
            "city": city,
            "latitude": lat,
            "longitude": lon,
            "temperature": data["temperature_2m"][i],
            "windspeed": data["windspeed_10m"][i],
            "winddirection": data["winddirection_10m"][i],
            "weathercode": data["weathercode"][i],
            "timestamp": ts + ":00Z" if len(ts) == 16 else ts,
        })
    return records


def run_backfill(producer, days_back=7):
    """One-time historical backfill for all cities."""
    print(f"Starting historical backfill ({days_back} days)...")
    for city, (lat, lon) in CITIES.items():
        try:
            records = fetch_historical_backfill(city, lat, lon, days_back)
            for record in records:
                producer.send(KAFKA_TOPIC_RAW, value=record)
            producer.flush()
            print(f"Backfilled {len(records)} records for {city}.")
        except Exception as e:
            print(f"Error backfilling {city}: {e}")
    print("Backfill complete.\n")


def run_live_polling(producer):
    """Continuous live polling loop."""
    print(f"Live polling started. Publishing to '{KAFKA_TOPIC_RAW}' every {POLL_INTERVAL_SECONDS}s.")
    while True:
        for city, (lat, lon) in CITIES.items():
            try:
                record = fetch_live_weather(city, lat, lon)
                producer.send(KAFKA_TOPIC_RAW, value=record)
                print(f"Sent: {record}")
            except Exception as e:
                print(f"Error fetching/sending data for {city}: {e}")
        producer.flush()
        time.sleep(POLL_INTERVAL_SECONDS)


def main():
    producer = create_producer()

    # Run backfill once at startup, then switch to live polling
    run_backfill(producer, days_back=7)
    run_live_polling(producer)


if __name__ == "__main__":
    main()