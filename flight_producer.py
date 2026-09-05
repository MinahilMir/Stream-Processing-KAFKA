import json
import time
from datetime import datetime, timedelta, timezone

import psycopg2
import requests
from kafka import KafkaProducer

from common import (
    AERODATABOX_API_KEY,
    AERODATABOX_BASE_URL,
    AERODATABOX_HOST,
    DB_CONFIG,
    FLIGHT_POLL_INTERVAL_SECONDS,
    FLIGHT_WINDOW_HOURS,
    KAFKA_BROKER,
    KAFKA_TOPIC_FLIGHT_RAW,
    TRACKED_AIRPORTS,
    TRACKED_ROUTE_PAIRS,
)


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def record_api_usage(conn, endpoint, unit_count):
    """AeroDataBox bills by request AND by 'unit' (rows returned) against a
    daily/monthly cap. Track both here instead of finding out via a 429."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO staging.api_usage_tracker (provider, endpoint, usage_date, request_count, unit_count)
            VALUES ('aerodatabox', %s, CURRENT_DATE, 1, %s)
            ON CONFLICT (provider, endpoint, usage_date) DO UPDATE SET
                request_count = staging.api_usage_tracker.request_count + 1,
                unit_count = staging.api_usage_tracker.unit_count + EXCLUDED.unit_count,
                updated_at = now()
            """,
            (endpoint, unit_count),
        )
    conn.commit()


def _extract_flight_record(flight, queried_airport):
    """AeroDataBox returns the same flight object shape in both the
    'departures' and 'arrivals' buckets of a by-airport response; which side
    (departure/arrival) has full timing data depends on which bucket it came
    from. Pull whatever is present on each side rather than assuming both."""
    departure = flight.get("departure") or {}
    arrival = flight.get("arrival") or {}

    origin_iata = (departure.get("airport") or {}).get("iata")
    destination_iata = (arrival.get("airport") or {}).get("iata")
    if not origin_iata or not destination_iata:
        return None

    return {
        "flight_number": flight.get("number"),
        "airline": (flight.get("airline") or {}).get("name"),
        "origin_iata": origin_iata,
        "destination_iata": destination_iata,
        "scheduled_departure": (departure.get("scheduledTime") or {}).get("utc"),
        "scheduled_arrival": (arrival.get("scheduledTime") or {}).get("utc"),
        "status": flight.get("status"),
        "queried_airport": queried_airport,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def is_tracked_route(origin_iata, destination_iata):
    return (origin_iata, destination_iata) in TRACKED_ROUTE_PAIRS


def fetch_airport_flights(conn, airport_iata):
    """Fetch scheduled arrivals+departures for one airport over a rolling
    window, then drop everything except the curated TRACKED_ROUTE_PAIRS."""
    now = datetime.now(timezone.utc)
    start = now.strftime("%Y-%m-%dT%H:%M")
    end = (now + timedelta(hours=FLIGHT_WINDOW_HOURS)).strftime("%Y-%m-%dT%H:%M")

    endpoint = f"/flights/airports/iata/{airport_iata}/{start}/{end}"
    url = f"{AERODATABOX_BASE_URL}{endpoint}"
    headers = {
        "X-RapidAPI-Key": AERODATABOX_API_KEY,
        "X-RapidAPI-Host": AERODATABOX_HOST,
    }
    params = {"withLeg": "true", "direction": "Both", "withCancelled": "false"}

    response = requests.get(url, headers=headers, params=params, timeout=15)
    response.raise_for_status()
    data = response.json()

    all_flights = data.get("departures", []) + data.get("arrivals", [])
    record_api_usage(conn, endpoint="flights_by_airport", unit_count=len(all_flights))

    records = []
    for flight in all_flights:
        record = _extract_flight_record(flight, airport_iata)
        if record is None:
            continue
        if not is_tracked_route(record["origin_iata"], record["destination_iata"]):
            continue
        records.append(record)
    return records


def run_polling(producer, conn):
    print(
        f"Flight polling started. Publishing to '{KAFKA_TOPIC_FLIGHT_RAW}' "
        f"every {FLIGHT_POLL_INTERVAL_SECONDS}s for airports {TRACKED_AIRPORTS}."
    )
    while True:
        for airport in TRACKED_AIRPORTS:
            try:
                records = fetch_airport_flights(conn, airport)
                for record in records:
                    producer.send(KAFKA_TOPIC_FLIGHT_RAW, value=record)
                producer.flush()
                print(f"Sent {len(records)} tracked-route flights for {airport}.")
            except Exception as e:
                print(f"Error fetching/sending flights for {airport}: {e}")
        time.sleep(FLIGHT_POLL_INTERVAL_SECONDS)


def main():
    if not AERODATABOX_API_KEY:
        raise RuntimeError(
            "AERODATABOX_API_KEY is not set. Get a free-tier key from "
            "https://rapidapi.com/aedbx-aedbx/api/aerodatabox and export it."
        )
    producer = create_producer()
    conn = psycopg2.connect(**DB_CONFIG)
    run_polling(producer, conn)


if __name__ == "__main__":
    main()
