import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta

from src.ingest.extract import fetch_earthquakes
from src.ingest.upload_s3 import upload_to_s3
from src.transform.bronze_to_silver import transform_bronze_to_silver
from src.transform.upload_silver_s3 import upload_directory_to_s3
from src.warehouse.load_gold import main as load_gold_main

SILVER_PATH = "data/silver/"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the earthquake pipeline end-to-end.")
    parser.add_argument("--start", required=True, help="Start date, e.g. 2026-01-01")
    parser.add_argument("--end", required=True, help="End date, e.g. 2026-01-02")
    return parser.parse_args()


def daily_windows(start_date: str, end_date: str) -> list[tuple[str, str]]:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    windows = []
    day = start
    while day < end:
        next_day = day + timedelta(days=1)
        windows.append((day.isoformat(), next_day.isoformat()))
        day = next_day

    return windows


def run(start_date: str, end_date: str) -> None:
    bucket = os.environ["S3_BUCKET"]
    bronze_path = f"data/bronze/raw_{start_date}.json"

    print(f"[1/5] Extracting earthquakes ({start_date} to {end_date})...")
    data = fetch_earthquakes(start_date, end_date)
    os.makedirs("data/bronze", exist_ok=True)
    with open(bronze_path, "w") as f:
        json.dump(data, f)
    print(f"  Found {len(data['features'])} earthquakes")

    print("[2/5] Uploading Bronze to S3...")
    bronze_key = f"bronze/{start_date}/raw_{start_date}.json"
    upload_to_s3(bronze_path, bucket, bronze_key)

    print("[3/5] Transforming Bronze to Silver...")
    transform_bronze_to_silver(bronze_path, SILVER_PATH)

    print("[4/5] Uploading Silver to S3...")
    upload_directory_to_s3(SILVER_PATH, bucket, "silver")

    print("[5/5] Loading Gold tables in Snowflake...")
    if load_gold_main() != 0:
        raise RuntimeError("Gold load failed")


def main() -> int:
    args = parse_args()
    start_time = time.time()

    windows = daily_windows(args.start, args.end)
    total_days = len(windows)
    failed_days = []

    for i, (day_start, day_end) in enumerate(windows, start=1):
        print(f"Day {i}/{total_days}: {day_start} to {day_end}")
        try:
            run(day_start, day_end)
        except Exception as e:
            print(f"  Day {i}/{total_days} failed: {e}", file=sys.stderr)
            failed_days.append(day_start)

    duration = time.time() - start_time
    succeeded = total_days - len(failed_days)

    print()
    print("Backfill summary:")
    print(f"  Succeeded: {succeeded}/{total_days}")
    print(f"  Failed: {len(failed_days)}/{total_days}")
    if failed_days:
        print(f"  Failed days: {', '.join(failed_days)}")
    print(f"  Total time: {duration:.1f}s")

    return 1 if failed_days else 0


if __name__ == "__main__":
    sys.exit(main())
