import pytest

from src.ingest.extract import fetch_earthquakes


def test_fetch_returns_geojson():
    result = fetch_earthquakes("2026-01-01", "2026-01-02")

    assert isinstance(result, dict)
    assert isinstance(result["features"], list)
    assert "metadata" in result
    assert len(result["features"]) > 0


def test_bad_date_raises():
    with pytest.raises(RuntimeError):
        fetch_earthquakes("not-a-date", "2026-01-02")


def test_features_have_required_fields():
    result = fetch_earthquakes("2026-01-01", "2026-01-02")
    feature = result["features"][0]

    assert feature["id"]
    assert feature["properties"]["mag"] is not None
    assert feature["properties"]["place"]
    assert len(feature["geometry"]["coordinates"]) == 3
