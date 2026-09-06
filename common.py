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
    "password": "dev_local_password_not_real",
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

# --- Kafka Configuration (Flights) ---
KAFKA_TOPIC_FLIGHT_RAW = "flight-raw"

# --- AeroDataBox Configuration ---
# Free-tier RapidAPI key: https://rapidapi.com/aedbx-aedbx/api/aerodatabox
AERODATABOX_API_KEY = os.environ.get("AERODATABOX_API_KEY", "")
AERODATABOX_HOST = "aerodatabox.p.rapidapi.com"
AERODATABOX_BASE_URL = f"https://{AERODATABOX_HOST}"

# --- Airports tracked (IATA codes) ---
TRACKED_AIRPORTS = ["LHE", "KHI", "ISB"]

# Only these routes are kept; AeroDataBox's by-airport endpoint returns every
# route touching the airport, so anything not listed here is dropped before
# it reaches Kafka.
FLIGHT_ROUTES = [
    ("LHE", "KHI"), ("KHI", "LHE"),
    ("LHE", "ISB"), ("ISB", "LHE"),
    ("KHI", "ISB"), ("ISB", "KHI"),
    ("LHE", "DXB"), ("DXB", "LHE"),
]
TRACKED_ROUTE_PAIRS = set(FLIGHT_ROUTES)

FLIGHT_POLL_INTERVAL_SECONDS = 900  # AeroDataBox free tier has tight request/unit caps
FLIGHT_WINDOW_HOURS = 12  # AeroDataBox's max time span per by-airport request