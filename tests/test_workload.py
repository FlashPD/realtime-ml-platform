from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tripml.benchmark import load_requests
from tripml.cli import main
from tripml.ingestion import PartitionPaths, YearMonth
from tripml.settings import IngestionSettings, PlatformSettings
from tripml.workflows.ingestion import run_ingestion
from tripml.workload import build_workload


def _input(
    root: Path, pickups: list[datetime] | None = None, month: str = "2024-01"
) -> tuple[PlatformSettings, PartitionPaths, Path]:
    pickups = pickups or [datetime(2024, 1, 1) + timedelta(hours=n) for n in range(200)]
    count = len(pickups)
    source = root / "source.parquet"
    pq.write_table(
        pa.table(
            {
                "tpep_pickup_datetime": pickups,
                "tpep_dropoff_datetime": [value + timedelta(minutes=10) for value in pickups],
                "passenger_count": [1 + n % 3 for n in range(count)],
                "trip_distance": [0.5 + n % 15 for n in range(count)],
                "PULocationID": [1 + n % 10 for n in range(count)],
                "DOLocationID": [11 + n % 7 for n in range(count)],
                "fare_amount": [18.0] * count,
            }
        ),
        source,
    )
    config = root / "config.yaml"
    config.write_text(f"ingestion:\n  data_root: {root / 'data'}\n", encoding="utf-8")
    run_ingestion(month, config_path=config, source=source)
    settings = PlatformSettings(ingestion=IngestionSettings(data_root=root / "data", batch_size=7))
    return settings, PartitionPaths.from_root(root / "data", YearMonth.parse(month)), config


def test_sample_is_reproducible_bounded_and_benchmark_compatible(tmp_path: Path) -> None:
    settings, paths, _ = _input(tmp_path)
    first, second, third = (tmp_path / name for name in ("first", "second", "third"))
    report = build_workload("2024-01", settings=settings, output=first, rows=40)
    # Changing Arrow batch boundaries must not change selection or order.
    settings = settings.model_copy(
        update={"ingestion": settings.ingestion.model_copy(update={"batch_size": 19})}
    )
    build_workload("2024-01", settings=settings, output=second, rows=40)
    build_workload("2024-01", settings=settings, output=third, rows=40, seed=43)
    assert (first / "requests.jsonl").read_bytes() == (second / "requests.jsonl").read_bytes()
    assert (first / "requests.jsonl").read_bytes() != (third / "requests.jsonl").read_bytes()
    requests = load_requests(first / "requests.jsonl")
    assert len({request.trip_id for request in requests}) == 40
    assert all(request.pickup_time.utcoffset() == timedelta(hours=-5) for request in requests)
    assert report["source"]["scanned_rows"] == report["source"]["eligible_rows"] == 200
    assert (
        report["source"]["silver_sha256"]
        == hashlib.sha256(paths.silver_file.read_bytes()).hexdigest()
    )
    assert (
        report["artifacts_sha256"]["requests.jsonl"]
        == hashlib.sha256((first / "requests.jsonl").read_bytes()).hexdigest()
    )
    assert (first / "quality-report.json").read_bytes() == paths.quality_report.read_bytes()
    assert json.loads((first / "manifest.json").read_text()) == report
    for dimension, bins in report["population_profile"].items():
        assert sum(bins.values()) == 200
        assert sum(report["sample_profile"][dimension].values()) == 40
        assert 0 <= report["total_variation_distance"][dimension] <= 1
    assert "actual_duration_seconds" not in (first / "requests.jsonl").read_text()


@pytest.mark.parametrize(
    ("month", "pickups", "offset"),
    [
        ("2024-03", [datetime(2024, 3, 10, 2, 30), datetime(2024, 3, 10, 3, 30)], -4),
        ("2024-11", [datetime(2024, 11, 3, 1, 30), datetime(2024, 11, 3, 2, 30)], -5),
    ],
)
def test_dst_uncertainty_is_counted_and_excluded(
    tmp_path: Path, month: str, pickups: list[datetime], offset: int
) -> None:
    settings, _, _ = _input(tmp_path, pickups, month)
    output = tmp_path / "workload"
    report = build_workload(month, settings=settings, output=output, rows=1)
    assert report["source"]["excluded_ambiguous_or_nonexistent_local_times"] == 1
    assert report["source"]["eligible_rows"] == 1
    assert load_requests(output / "requests.jsonl")[0].pickup_time.utcoffset() == timedelta(
        hours=offset
    )
    assert set(report["total_variation_distance"].values()) == {0.0}


@pytest.mark.parametrize("rows", [0, 100_001, 201])
def test_invalid_sample_size_does_not_publish(tmp_path: Path, rows: int) -> None:
    settings, _, _ = _input(tmp_path)
    output = tmp_path / "workload"
    with pytest.raises(ValueError, match=r"rows|eligible"):
        build_workload("2024-01", settings=settings, output=output, rows=rows)
    assert not output.exists()


def test_existing_evidence_is_never_overwritten(tmp_path: Path) -> None:
    settings, _, _ = _input(tmp_path)
    output = tmp_path / "workload"
    output.mkdir()
    (output / "sentinel").write_text("keep", encoding="utf-8")
    with pytest.raises(FileExistsError):
        build_workload("2024-01", settings=settings, output=output, rows=1)
    assert (output / "sentinel").read_text() == "keep"


@pytest.mark.parametrize(
    "corruption",
    ["count", "month", "path", "missing", "version", "timestamp", "timezone", "null", "distance"],
)
def test_invalid_source_fails_before_publication(tmp_path: Path, corruption: str) -> None:
    settings, paths, _ = _input(tmp_path)
    if corruption in {"count", "missing", "version", "timestamp", "timezone", "null", "distance"}:
        table = pq.ParquetFile(paths.silver_file).read()
        if corruption == "count":
            table = table.slice(0, 10)
        elif corruption == "missing":
            table = table.drop(["passenger_count"])
        else:
            column, value = {
                "version": ("contract_version", "2.0"),
                "timestamp": ("pickup_datetime", datetime(2024, 2, 1)),
                "timezone": ("pickup_datetime", datetime(2024, 1, 1, tzinfo=UTC)),
                "null": ("pickup_datetime", None),
                "distance": ("trip_distance_miles", float("nan")),
            }[corruption]
            table = table.set_column(
                table.schema.get_field_index(column), column, pa.array([value] * 200)
            )
        pq.write_table(table, paths.silver_file)
    else:
        report = json.loads(paths.quality_report.read_text())
        report["month" if corruption == "month" else "silver_path"] = "wrong"
        paths.quality_report.write_text(json.dumps(report), encoding="utf-8")
    output = tmp_path / "workload"
    with pytest.raises(ValueError, match=r"silver|contract|timestamp|finite"):
        build_workload("2024-01", settings=settings, output=output, rows=1)
    assert not output.exists()


def test_source_change_during_sampling_fails(tmp_path: Path) -> None:
    settings, _, _ = _input(tmp_path)
    with (
        patch("tripml.workload._sha256", side_effect=["a" * 64, "b" * 64]),
        pytest.raises(ValueError, match="changed during sampling"),
    ):
        build_workload("2024-01", settings=settings, output=tmp_path / "workload", rows=1)
    assert not (tmp_path / "workload").exists()


def test_cli_generates_workload(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _, _, config = _input(tmp_path)
    output = tmp_path / "workload"
    assert (
        main(
            [
                "benchmark-workload",
                "--month",
                "2024-01",
                "--config",
                str(config),
                "--output",
                str(output),
                "--rows",
                "20",
                "--seed",
                "7",
            ]
        )
        == 0
    )
    report = json.loads(capsys.readouterr().out)
    assert report["sampling"] == {"algorithm": "reservoir-v1", "seed": 7, "rows": 20}
    assert len(load_requests(output / "requests.jsonl")) == 20
