import os
import sys

import snowflake.connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import Error as SnowflakeError

WAREHOUSE = "earthquake_wh"
DATABASE = "earthquake_db"
SCHEMA = "gold"

CREATE_DIM_DATE_SQL = """
CREATE OR REPLACE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    day_of_week VARCHAR
)
"""

CREATE_DIM_LOCATION_SQL = """
CREATE OR REPLACE TABLE dim_location (
    location_key INTEGER AUTOINCREMENT PRIMARY KEY,
    place VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    region VARCHAR
)
"""

CREATE_DIM_MAGNITUDE_BAND_SQL = """
CREATE OR REPLACE TABLE dim_magnitude_band (
    magnitude_band_key INTEGER PRIMARY KEY,
    band_name VARCHAR,
    min_magnitude FLOAT,
    max_magnitude FLOAT
)
"""

CREATE_FACT_EARTHQUAKE_SQL = """
CREATE OR REPLACE TABLE fact_earthquake (
    earthquake_key INTEGER AUTOINCREMENT PRIMARY KEY,
    usgs_id VARCHAR,
    date_key INTEGER REFERENCES dim_date(date_key),
    location_key INTEGER REFERENCES dim_location(location_key),
    magnitude_band_key INTEGER REFERENCES dim_magnitude_band(magnitude_band_key),
    magnitude FLOAT,
    depth_km FLOAT,
    tsunami INTEGER,
    event_time TIMESTAMP,
    updated_time TIMESTAMP
)
"""

MAGNITUDE_BANDS = [
    (1, "Micro", 0.0, 2.99),
    (2, "Minor", 3.0, 3.99),
    (3, "Light", 4.0, 4.99),
    (4, "Moderate", 5.0, 5.99),
    (5, "Strong", 6.0, 6.99),
    (6, "Major", 7.0, 7.99),
    (7, "Great", 8.0, 10.0),
]

INSERT_MAGNITUDE_BAND_SQL = """
INSERT INTO dim_magnitude_band (magnitude_band_key, band_name, min_magnitude, max_magnitude)
VALUES (%s, %s, %s, %s)
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


def create_tables(conn: SnowflakeConnection) -> None:
    cur = conn.cursor()
    try:
        cur.execute(CREATE_DIM_DATE_SQL)
        cur.execute(CREATE_DIM_LOCATION_SQL)
        cur.execute(CREATE_DIM_MAGNITUDE_BAND_SQL)
        cur.execute(CREATE_FACT_EARTHQUAKE_SQL)

        cur.executemany(INSERT_MAGNITUDE_BAND_SQL, MAGNITUDE_BANDS)
    finally:
        cur.close()


def main() -> int:
    conn = get_connection()
    try:
        create_tables(conn)
    except SnowflakeError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    finally:
        conn.close()

    print(
        f"Created {DATABASE}.{SCHEMA} tables: dim_date, dim_location, "
        "dim_magnitude_band (7 rows loaded), fact_earthquake"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
