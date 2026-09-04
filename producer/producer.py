import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime, timezone
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from schemas.weather_schema import WeatherRaw

# Cities to track (lat/lon)
CITIES = {
    "Lahore": (31.55, 74.35),
    "Karachi": (24.86, 67.01),
    "Islamabad": (33.68, 73.05),
}

KAFKA_TOPIC = "weather-raw"
KAFKA_BROKER = "localhost:9092"
POLL_INTERVAL_SECONDS = 60  # fetch every 60 seconds


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )


def fetch_weather(city, lat, lon):
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()["current_weather"]

    record = WeatherRaw(
        city=city,
        latitude=lat,
        longitude=lon,
        temperature=data["temperature"],
        windspeed=data["windspeed"],
        winddirection=data["winddirection"],
        weathercode=data["weathercode"],
        timestamp=datetime.now(timezone.utc)
    )
    return record.model_dump(mode="json")


def main():
    producer = create_producer()
    print(f"Producer started. Publishing to topic '{KAFKA_TOPIC}' every {POLL_INTERVAL_SECONDS}s.")

    while True:
        for city, (lat, lon) in CITIES.items():
            try:
                record = fetch_weather(city, lat, lon)
                producer.send(KAFKA_TOPIC, value=record)
                print(f"Sent: {record}")
            except Exception as e:
                print(f"Error fetching/sending data for {city}: {e}")
        producer.flush()
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()