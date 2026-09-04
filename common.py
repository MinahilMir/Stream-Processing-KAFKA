import os

# --- Kafka Configuration ---
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC_RAW = "weather-raw"

# --- PostgreSQL Configuration ---
DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 5434,
    "dbname": "weather_db",
    "user": "weather_user",
    "password": "weather_pass",
}

# --- Cities tracked ---
CITIES = {
    "Lahore": (31.55, 74.35),
    "Karachi": (24.86, 67.01),
    "Islamabad": (33.68, 73.05),
}

# --- Open-Meteo Endpoints ---
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

POLL_INTERVAL_SECONDS = 60