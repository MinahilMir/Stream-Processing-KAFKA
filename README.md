\# Stream-Processing-KAFKA



Real-time weather (and later flight) data pipeline using Apache Kafka, PostgreSQL, Apache Airflow, and Power BI.



\## Status

In development — Week 4 of internship roadmap. Adds a second, independent
streaming domain (flight tracking via AeroDataBox) alongside the existing
weather pipeline, sharing the same Kafka/PostgreSQL/Airflow architecture.



\## Week 4 — Flight Pipeline Setup

1\. Get a free-tier AeroDataBox API key from RapidAPI:
   https://rapidapi.com/aedbx-aedbx/api/aerodatabox

2\. Export it before running the producer:

   ```
   set AERODATABOX_API_KEY=your_key_here        (Windows cmd)
   $env:AERODATABOX_API_KEY = "your_key_here"    (PowerShell)
   export AERODATABOX_API_KEY=your_key_here      (bash)
   ```

3\. Apply the new SQL (in order) against the running Postgres instance:

   ```
   sql/flight_schema.sql
   sql/flight_star_schema.sql
   sql/insights.sql   (re-run — adds the new flight/API-usage views)
   ```

4\. Run the flight pipeline (same Kafka/Postgres stack as weather):

   ```
   python flight_producer.py
   python flight_consumer.py
   ```

5\. Copy `airflow/dags/flight_transformation_dag.py` into the Airflow stack's
   `dags/` folder (already covered if you mount `airflow/dags` as in
   `airflow/docker-compose.yml`) — it registers as `flight_transformation_pipeline`
   alongside `weather_transformation_pipeline`.

6\. Tableau dashboard: this repo does not include a `Flight_Risk_Advisor.twb`
   file — Tableau workbooks are authored interactively in Tableau Desktop, not
   generated as code. Point a new Tableau workbook at the warehouse
   (`warehouse.fact_flight_daily`, `warehouse.dim_airport`) and the staging
   views `staging.v_latest_flight_status`, `staging.v_flight_route_summary`,
   and `staging.v_api_usage_summary` to build it.

Tracked airports and routes are configured in `common.py`
(`TRACKED_AIRPORTS`, `FLIGHT_ROUTES`) — routes not listed there are dropped
before they reach Kafka, since AeroDataBox's by-airport endpoint returns
every route touching a queried airport, not just the ones you care about.

