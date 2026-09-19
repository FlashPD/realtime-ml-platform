from __future__ import annotations

import io
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Self
from urllib.request import Request

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tripml.ingestion import (
    BronzeIntegrityError,
    PartitionPaths,
    PartitionStatus,
    SourceSchemaError,
    YearMonth,
    download_bronze,
    source_url,
    validate_partition,
)

NOW = datetime(2024, 5, 1, tzinfo=UTC)
JANUARY = YearMonth(2024, 1)
SOURCE_SCHEMA = pa.schema(
    [
        ("tpep_pickup_datetime", pa.timestamp("us")),
        ("tpep_dropoff_datetime", pa.timestamp("us")),
        ("passenger_count", pa.float64()),
        ("trip_distance", pa.float64()),
        ("PULocationID", pa.int64()),
        ("DOLocationID", pa.int64()),
        ("fare_amount", pa.float64()),
    ]
)


class MemoryResponse(io.BytesIO):
    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


def _row(**overrides: object) -> dict[str, object]:
    pickup = datetime(2024, 1, 15, 12)
    row: dict[str, object] = {
        "tpep_pickup_datetime": pickup,
        "tpep_dropoff_datetime": pickup + timedelta(minutes=10),
        "passenger_count": 1.0,
        "trip_distance": 2.5,
        "PULocationID": 161,
        "DOLocationID": 236,
        "fare_amount": 18.0,
    }
    row.update(overrides)
    return row


def _write_source(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=SOURCE_SCHEMA)
    pq.write_table(table, path)


def _parquet_bytes() -> bytes:
    buffer = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist([_row()], schema=SOURCE_SCHEMA), buffer)
    return buffer.getvalue().to_pybytes()


def test_year_month_parses_and_builds_source_paths(tmp_path: Path) -> None:
    month = YearMonth.parse("2024-12")
    paths = PartitionPaths.from_root(tmp_path, month)

    assert str(month) == "2024-12"
    assert month.end == datetime(2025, 1, 1)
    assert paths.bronze_file.name == "yellow_tripdata_2024-12.parquet"
    assert paths.silver_file == tmp_path / "silver/yellow/month=2024-12/trips.parquet"
    assert source_url(month, "https://example.test/") == (
        "https://example.test/yellow_tripdata_2024-12.parquet"
    )


@pytest.mark.parametrize("value", ["2024-1", "24-01", "2024/01", "2024-13", "2008-12"])
def test_year_month_rejects_invalid_values(value: str) -> None:
    with pytest.raises(ValueError, match=r"month|year"):
        YearMonth.parse(value)


def test_download_bronze_is_atomic_verified_and_idempotent(tmp_path: Path) -> None:
    paths = PartitionPaths.from_root(tmp_path, JANUARY)
    calls: list[tuple[str, float]] = []

    def opener(request: Request, timeout: float) -> MemoryResponse:
        calls.append((request.full_url, timeout))
        return MemoryResponse(_parquet_bytes())

    manifest = download_bronze(
        JANUARY,
        paths,
        base_url="https://example.test/trips",
        timeout_seconds=5,
        opener=opener,
        now=lambda: NOW,
    )
    reused = download_bronze(
        JANUARY,
        paths,
        base_url="https://example.test/trips",
        timeout_seconds=5,
        opener=opener,
        now=lambda: NOW + timedelta(days=1),
    )

    assert len(calls) == 1
    assert manifest == reused
    assert manifest.retrieved_at == NOW
    assert manifest.size_bytes == paths.bronze_file.stat().st_size
    assert paths.bronze_manifest.exists()


def test_download_rejects_non_parquet_and_corrupt_existing_object(tmp_path: Path) -> None:
    paths = PartitionPaths.from_root(tmp_path, JANUARY)

    with pytest.raises(BronzeIntegrityError, match="not a valid Parquet"):
        download_bronze(
            JANUARY,
            paths,
            base_url="https://example.test",
            timeout_seconds=5,
            opener=lambda _request, _timeout: MemoryResponse(b"not parquet"),
        )
    assert not paths.bronze_file.exists()

    download_bronze(
        JANUARY,
        paths,
        base_url="https://example.test",
        timeout_seconds=5,
        opener=lambda _request, _timeout: MemoryResponse(_parquet_bytes()),
    )
    paths.bronze_file.write_bytes(paths.bronze_file.read_bytes() + b"corruption")
    with pytest.raises(BronzeIntegrityError, match="does not match"):
        download_bronze(
            JANUARY,
            paths,
            base_url="https://example.test",
            timeout_seconds=5,
            opener=lambda _request, _timeout: MemoryResponse(_parquet_bytes()),
        )


def test_valid_partition_publishes_normalized_silver_and_invalid_rows(tmp_path: Path) -> None:
    paths = PartitionPaths.from_root(tmp_path, JANUARY)
    source = tmp_path / "source.parquet"
    paths.quarantine_source_file.parent.mkdir(parents=True)
    paths.quarantine_source_file.write_bytes(b"stale rejected source")
    _write_source(
        source,
        [
            _row(),
            _row(PULocationID=0),
            _row(
                tpep_pickup_datetime=datetime(2024, 1, 16, 9),
                tpep_dropoff_datetime=datetime(2024, 1, 16, 9, 10),
            ),
        ],
    )

    report = validate_partition(
        source,
        JANUARY,
        paths,
        max_violation_rate=0.5,
        batch_size=2,
        now=lambda: NOW,
    )

    silver = pq.read_table(paths.silver_file)
    invalid = pq.read_table(paths.invalid_rows_file)
    assert report.status is PartitionStatus.ACCEPTED
    assert (report.total_rows, report.valid_rows, report.invalid_rows) == (3, 2, 1)
    assert silver.column_names == [field.name for field in silver.schema]
    assert silver["trip_id"].to_pylist() == ["yellow:2024-01:0", "yellow:2024-01:2"]
    assert silver["actual_duration_seconds"].to_pylist() == [600, 600]
    assert invalid["source_row_number"].to_pylist() == [1]
    assert invalid["violates_known_pickup_zone"].to_pylist() == [True]
    assert not paths.quarantine_source_file.exists()
    assert paths.quality_report.exists()


def test_every_quality_rule_is_reported_independently(tmp_path: Path) -> None:
    paths = PartitionPaths.from_root(tmp_path, JANUARY)
    source = tmp_path / "rules.parquet"
    pickup = datetime(2024, 1, 15, 12)
    rows = [
        _row(),
        _row(passenger_count=None),
        _row(tpep_dropoff_datetime=pickup),
        _row(tpep_dropoff_datetime=pickup + timedelta(seconds=30)),
        _row(trip_distance=101.0),
        _row(passenger_count=10.0),
        _row(passenger_count=1.5),
        _row(PULocationID=0),
        _row(DOLocationID=266),
        _row(fare_amount=-0.01),
        _row(
            tpep_pickup_datetime=datetime(2024, 2, 1),
            tpep_dropoff_datetime=datetime(2024, 2, 1, 0, 10),
        ),
    ]
    _write_source(source, rows)

    report = validate_partition(
        source,
        JANUARY,
        paths,
        max_violation_rate=1,
        batch_size=20,
    )
    checks = {check.rule: check.violations for check in report.checks}

    assert report.status is PartitionStatus.ACCEPTED
    assert report.invalid_rows == len(rows) - 1
    assert checks == {
        "any_contract_violation": 10,
        "required_fields": 1,
        "pickup_before_dropoff": 1,
        "duration_range": 2,
        "distance_range": 1,
        "passenger_count_range": 2,
        "known_pickup_zone": 1,
        "known_dropoff_zone": 1,
        "non_negative_fare": 1,
        "timestamps_in_partition": 1,
    }


def test_unknown_passenger_policy_preserves_nulls_and_other_quality_rules(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    _write_source(
        source,
        [
            _row(passenger_count=None),
            _row(passenger_count=0),
            _row(passenger_count=2),
            _row(passenger_count=None, fare_amount=-1),
            _row(passenger_count=None, trip_distance=None),
            _row(passenger_count=1.5),
            _row(passenger_count=-1),
            _row(passenger_count=10),
            _row(passenger_count=float("nan")),
            _row(passenger_count=float("inf")),
        ],
    )
    paths = PartitionPaths.from_root(tmp_path / "nullable", JANUARY)
    report = validate_partition(
        source,
        JANUARY,
        paths,
        max_violation_rate=0.7,
        batch_size=2,
        passenger_count_policy="allow_unknown",
    )
    assert report.status is PartitionStatus.ACCEPTED
    assert report.contract_version == "1.1"
    assert report.missing_passenger_rows == 3
    assert report.valid_missing_passenger_rows == 1
    assert report.valid_rows == 3
    silver = pq.ParquetFile(paths.silver_file).read()
    assert silver["passenger_count"].to_pylist() == [None, 0, 2]
    assert silver["contract_version"].to_pylist() == ["1.1"] * 3
    assert {check.contract_version for check in report.checks} == {"1.1"}
    strict = validate_partition(
        source,
        JANUARY,
        PartitionPaths.from_root(tmp_path / "strict", JANUARY),
        max_violation_rate=0.7,
        batch_size=3,
    )
    assert strict.status is PartitionStatus.QUARANTINED
    assert strict.missing_passenger_rows == 3
    assert strict.valid_missing_passenger_rows == 0


def test_unknown_passenger_policy_keeps_partition_gate_at_ten_percent(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    _write_source(source, [_row(passenger_count=None)] * 8 + [_row(PULocationID=0)] * 2)
    report = validate_partition(
        source,
        JANUARY,
        PartitionPaths.from_root(tmp_path / "nullable", JANUARY),
        max_violation_rate=0.1,
        batch_size=2,
        passenger_count_policy="allow_unknown",
    )
    assert report.status is PartitionStatus.QUARANTINED
    assert report.violation_rate == 0.2


def test_rejected_partition_is_quarantined_without_replacing_silver(tmp_path: Path) -> None:
    paths = PartitionPaths.from_root(tmp_path, JANUARY)
    source = tmp_path / "bad.parquet"
    _write_source(source, [_row(), _row(PULocationID=0)])
    paths.silver_file.parent.mkdir(parents=True, exist_ok=True)
    paths.silver_file.write_bytes(b"previous accepted partition")

    report = validate_partition(
        source,
        JANUARY,
        paths,
        max_violation_rate=0.1,
        batch_size=1,
    )

    assert report.status is PartitionStatus.QUARANTINED
    assert report.silver_path is None
    assert paths.silver_file.read_bytes() == b"previous accepted partition"
    assert paths.quarantine_source_file.read_bytes() == source.read_bytes()
    assert pq.read_table(paths.invalid_rows_file).num_rows == 1


def test_empty_partition_is_quarantined(tmp_path: Path) -> None:
    paths = PartitionPaths.from_root(tmp_path, JANUARY)
    source = tmp_path / "empty.parquet"
    pq.write_table(pa.Table.from_pylist([], schema=SOURCE_SCHEMA), source)
    paths.invalid_rows_file.parent.mkdir(parents=True)
    paths.invalid_rows_file.write_bytes(b"stale invalid rows")

    report = validate_partition(
        source,
        JANUARY,
        paths,
        max_violation_rate=0.1,
        batch_size=10,
    )

    assert report.status is PartitionStatus.QUARANTINED
    assert report.total_rows == 0
    assert paths.quarantine_source_file.exists()
    assert not paths.invalid_rows_file.exists()


def test_missing_source_columns_fail_before_outputs_are_created(tmp_path: Path) -> None:
    paths = PartitionPaths.from_root(tmp_path, JANUARY)
    source = tmp_path / "missing.parquet"
    pq.write_table(pa.table({"PULocationID": [161]}), source)

    with pytest.raises(SourceSchemaError, match="fare_amount"):
        validate_partition(
            source,
            JANUARY,
            paths,
            max_violation_rate=0.1,
            batch_size=10,
        )
    assert not paths.quality_report.exists()
