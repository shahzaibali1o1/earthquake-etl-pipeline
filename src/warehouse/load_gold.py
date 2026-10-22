import os
import sys

import snowflake.connector
from pyspark.sql import SparkSession
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import Error as SnowflakeError

WAREHOUSE = "earthquake_wh"
DATABASE = "earthquake_db"
SCHEMA = "gold"
SILVER_PATH = "data/silver/"

MERGE_DIM_DATE_SQL = """
MERGE INTO dim_date AS target
USING (SELECT %s AS date_key, %s AS full_date, %s AS year, %s AS month, %s AS day, %s AS day_of_week) AS source
ON target.date_key = source.date_key
WHEN NOT MATCHED THEN
    INSERT (date_key, full_date, year, month, day, day_of_week)
    VALUES (source.date_key, source.full_date, source.year, source.month, source.day, source.day_of_week)
"""

MERGE_DIM_LOCATION_SQL = """
MERGE INTO dim_location AS target
USING (SELECT %s AS place, %s AS latitude, %s AS longitude, %s AS region) AS source
ON target.place = source.place
WHEN NOT MATCHED THEN
    INSERT (place, latitude, longitude, region)
    VALUES (source.place, source.latitude, source.longitude, source.region)
"""

CREATE_STAGE_SQL = """
CREATE OR REPLACE TEMPORARY TABLE stage_fact_earthquake (
    usgs_id VARCHAR,
    event_date DATE,
    place VARCHAR,
    magnitude FLOAT,
    depth_km FLOAT,
    tsunami INTEGER,
    event_time TIMESTAMP,
    updated_time TIMESTAMP
)
"""

INSERT_STAGE_SQL = """
INSERT INTO stage_fact_earthquake
    (usgs_id, event_date, place, magnitude, depth_km, tsunami, event_time, updated_time)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

MERGE_FACT_SQL = """
MERGE INTO fact_earthquake AS target
USING (
    SELECT
        s.usgs_id,
        d.date_key,
        l.location_key,
        b.magnitude_band_key,
        s.magnitude,
        s.depth_km,
        s.tsunami,
        s.event_time,
        s.updated_time
    FROM stage_fact_earthquake s
    JOIN dim_date d ON d.full_date = s.event_date
    JOIN dim_location l ON l.place = s.place
    JOIN dim_magnitude_band b ON s.magnitude BETWEEN b.min_magnitude AND b.max_magnitude
) AS source
ON target.usgs_id = source.usgs_id
WHEN MATCHED THEN UPDATE SET
    target.date_key = source.date_key,
    target.location_key = source.location_key,
    target.magnitude_band_key = source.magnitude_band_key,
    target.magnitude = source.magnitude,
    target.depth_km = source.depth_km,
    target.tsunami = source.tsunami,
    target.event_time = source.event_time,
    target.updated_time = source.updated_time
WHEN NOT MATCHED THEN
    INSERT (usgs_id, date_key, location_key, magnitude_band_key, magnitude, depth_km, tsunami, event_time, updated_time)
    VALUES (source.usgs_id, source.date_key, source.location_key, source.magnitude_band_key,
            source.magnitude, source.depth_km, source.tsunami, source.event_time, source.updated_time)
"""


def get_connection() -> SnowflakeConnection:
    return snowflake.connector.connect(
        account=os.environ["SF_ACCOUNT"],
        user=os.environ["SF_USER"],
        password=os.environ["SF_PASSWORD"],
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
    )


def read_silver(path: str) -> list:
    spark = SparkSession.builder.appName("load_gold").getOrCreate()
    try:
        return spark.read.parquet(path).collect()
    finally:
        spark.stop()


def parse_region(place: str) -> str | None:
    if "," not in place:
        return None
    return place.rsplit(",", 1)[-1].strip()


def load_dim_date(cur, rows: list) -> None:
    seen = {}
    for r in rows:
        d = r["event_date"]
        if d in seen:
            continue
        date_key = int(d.strftime("%Y%m%d"))
        seen[d] = (date_key, d, d.year, d.month, d.day, d.strftime("%A"))

    cur.executemany(MERGE_DIM_DATE_SQL, list(seen.values()))


def load_dim_location(cur, rows: list) -> None:
    seen = {}
    for r in rows:
        place = r["place"]
        if place in seen:
            continue
        seen[place] = (place, r["latitude"], r["longitude"], parse_region(place))

    cur.executemany(MERGE_DIM_LOCATION_SQL, list(seen.values()))


def load_fact_earthquake(cur, rows: list) -> None:
    cur.execute(CREATE_STAGE_SQL)

    stage_rows = [
        (
            r["event_id"],
            r["event_date"],
            r["place"],
            r["magnitude"],
            r["depth_km"],
            r["tsunami"],
            r["event_time"],
            r["updated_time"],
        )
        for r in rows
    ]
    cur.executemany(INSERT_STAGE_SQL, stage_rows)
    cur.execute(MERGE_FACT_SQL)


def main() -> int:
    rows = read_silver(SILVER_PATH)

    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            load_dim_date(cur, rows)
            load_dim_location(cur, rows)
            load_fact_earthquake(cur, rows)
        finally:
            cur.close()
    except SnowflakeError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    finally:
        conn.close()

    print(f"Loaded {len(rows)} events into {DATABASE}.{SCHEMA}.fact_earthquake")
    return 0


if __name__ == "__main__":
    sys.exit(main())
