from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tripml.features import FeatureBuildError, FeatureSourceError, build_gold_features
from tripml.ingestion import CONTRACT_VERSION, SILVER_SCHEMA, YearMonth
from tripml.settings import IngestionSettings, PlatformSettings, StreamingSettings

BUILT_AT = datetime(2024, 2, 1, tzinfo=UTC)


def test_named_gold_windows_cannot_silently_change_semantics(tmp_path: Path) -> None:
    settings = PlatformSettings(
        ingestion=IngestionSettings(data_root=tmp_path),
        streaming=StreamingSettings(short_window_seconds=600),
    )
    with pytest.raises(FeatureBuildError, match="900/3600"):
        build_gold_features("2024-01", settings=settings)


def _trip(
    trip_id: str,
    pickup: datetime,
    duration_seconds: int,
    *,
    pickup_zone_id: int = 1,
    dropoff_zone_id: int = 9,
    distance_miles: float = 5.0,
    source_row_number: int = 0,
) -> dict[str, object]:
    return {
        "trip_id": trip_id,
        "pickup_datetime": pickup,
        "dropoff_datetime": pickup + timedelta(seconds=duration_seconds),
        "pickup_zone_id": pickup_zone_id,
        "dropoff_zone_id": dropoff_zone_id,
        "trip_distance_miles": distance_miles,
        "passenger_count": 1,
        "fare_amount": 10.0,
        "actual_duration_seconds": duration_seconds,
        "source_row_number": source_row_number,
        "contract_version": CONTRACT_VERSION,
    }


def _write_silver(data_root: Path, month: YearMonth, rows: list[dict[str, object]]) -> None:
    destination = data_root / f"silver/yellow/month={month}/trips.parquet"
    destination.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=SILVER_SCHEMA), destination)


def _settings(data_root: Path) -> PlatformSettings:
    return PlatformSettings(
        ingestion=IngestionSettings(data_root=data_root),
        streaming=StreamingSettings(short_window_seconds=900, long_window_seconds=3600),
    )


def test_gold_features_have_known_answers_without_target_leakage(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    _write_silver(
        data_root,
        YearMonth(2023, 12),
        [_trip("previous", datetime(2023, 12, 31, 23, 40), 600)],
    )
    _write_silver(
        data_root,
        YearMonth(2024, 1),
        [
            _trip("boundary", datetime(2024, 1, 1, 0, 10), 600, source_row_number=0),
            _trip("history-a", datetime(2024, 1, 1, 11, 40), 600, source_row_number=1),
            _trip(
                "history-b",
                datetime(2024, 1, 1, 11, 50),
                300,
                distance_miles=2.5,
                source_row_number=2,
            ),
            _trip("not-complete", datetime(2024, 1, 1, 11, 58), 600, source_row_number=3),
            _trip("target", datetime(2024, 1, 1, 12), 600, source_row_number=4),
        ],
    )

    report = build_gold_features("2024-01", settings=_settings(data_root), now=lambda: BUILT_AT)

    table = pq.ParquetFile(report.output_path).read()
    rows = {row["trip_id"]: row for row in table.to_pylist()}
    target = rows["target"]
    boundary = rows["boundary"]
    same_timestamp_pickup = rows["history-b"]

    assert report.row_count == 5
    assert len(report.output_sha256) == 64
    assert report.output_size_bytes == Path(report.output_path).stat().st_size
    assert report.input_rows == 6
    assert len(report.inputs) == 2
    assert target["pu_zone_trips_15m"] == 2
    assert target["pu_zone_mean_duration_60m"] == pytest.approx(450.0)
    assert target["pu_zone_mean_speed_15m"] == pytest.approx(30.0)
    assert target["do_zone_trips_60m"] == 2
    assert target["pu_zone_features_15m_as_of"] == datetime(2024, 1, 1, 11, 55)
    assert same_timestamp_pickup["pu_zone_trips_15m"] == 0
    assert boundary["pu_zone_trips_15m"] == 0
    assert boundary["pu_zone_trips_60m"] == 1
    assert boundary["pu_zone_features_60m_as_of"] == datetime(2023, 12, 31, 23, 50)
    manifest = json.loads(Path(report.manifest_path).read_text(encoding="utf-8"))
    assert manifest["built_at"] == "2024-02-01T00:00:00Z"
    assert manifest["output_sha256"] == report.output_sha256


def test_gold_feature_build_requires_target_silver_partition(tmp_path: Path) -> None:
    with pytest.raises(FeatureSourceError, match="accepted silver partition"):
        build_gold_features("2024-01", settings=_settings(tmp_path))
