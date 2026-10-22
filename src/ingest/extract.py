import json
import os
import time

import requests

API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
PAGE_SIZE = 500
MAX_RETRIES = 3


def _request_page(params: dict) -> dict:
    for attempt in range(MAX_RETRIES + 1):
        try:
            response = requests.get(API_URL, params=params, timeout=30)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            if attempt == MAX_RETRIES:
                raise RuntimeError(
                    f"Could not reach USGS API after {MAX_RETRIES} retries: {exc}"
                ) from exc
            time.sleep(2 ** attempt)
            continue

        if response.status_code >= 500:
            if attempt == MAX_RETRIES:
                raise RuntimeError(
                    f"USGS API returned server error {response.status_code} after "
                    f"{MAX_RETRIES} retries: {response.text[:300]}"
                )
            time.sleep(2 ** attempt)
            continue

        if response.status_code >= 400:
            raise RuntimeError(
                f"USGS API rejected the request ({response.status_code}): {response.text[:300]}"
            )

        return response.json()


def fetch_earthquakes(start_date: str, end_date: str, min_magnitude: float = 2.5) -> dict:
    combined = None
    all_features = []
    offset = 1

    while True:
        page = _request_page({
            "format": "geojson",
            "starttime": start_date,
            "endtime": end_date,
            "minmagnitude": min_magnitude,
            "limit": PAGE_SIZE,
            "offset": offset,
        })

        features = page["features"]
        all_features.extend(features)

        if combined is None:
            combined = page

        # The API drops metadata.count once limit/offset are set, so a short
        # page (fewer features than PAGE_SIZE) is the signal we've reached the end.
        if len(features) < PAGE_SIZE:
            break

        offset += PAGE_SIZE

    combined["features"] = all_features
    combined["metadata"]["count"] = len(all_features)
    return combined


if __name__ == "__main__":
    start_date = "2026-01-01"
    end_date = "2026-01-02"

    try:
        data = fetch_earthquakes(start_date, end_date)
    except RuntimeError as exc:
        print(f"Failed to fetch earthquake data: {exc}")
        raise SystemExit(1)

    os.makedirs("data/bronze", exist_ok=True)
    output_path = f"data/bronze/raw_{start_date}.json"
    with open(output_path, "w") as f:
        json.dump(data, f)

    print(f"Found {len(data['features'])} earthquakes")
