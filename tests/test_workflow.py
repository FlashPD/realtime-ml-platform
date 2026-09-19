from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tripml.ingestion import PartitionStatus, SourceSchemaError
from tripml.lineage import PostgresLineageRepository
from tripml.workflows.ingestion import IngestionConfigurationError, run_ingestion

RUN_ID = UUID("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee")


def _write_valid_source(path: Path) -> None:
    pickup = datetime(2024, 1, 15, 12)
    pq.write_table(
        pa.table(
            {
                "tpep_pickup_datetime": [pickup],
                "tpep_dropoff_datetime": [pickup + timedelta(minutes=10)],
                "passenger_count": [1.0],
                "trip_distance": [2.5],
                "PULocationID": [161],
                "DOLocationID": [236],
                "fare_amount": [18.0],
            }
        ),
        path,
    )


def _config(tmp_path: Path) -> Path:
    config = tmp_path / "config.yaml"
    config.write_text(
        f"ingestion:\n  data_root: {tmp_path / 'data'}\n  batch_size: 10\n",
        encoding="utf-8",
    )
    return config


def test_orchestrated_ingestion_requires_durable_lineage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("TRIPML_LINEAGE__DATABASE_URL", raising=False)

    with pytest.raises(IngestionConfigurationError, match="DATABASE_URL"):
        run_ingestion("2024-01", config_path=_config(tmp_path), require_lineage=True)


def test_local_workflow_can_run_without_lineage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("TRIPML_LINEAGE__DATABASE_URL", raising=False)
    source = tmp_path / "source.parquet"
    _write_valid_source(source)

    execution = run_ingestion("2024-01", config_path=_config(tmp_path), source=source)

    assert execution.report.status is PartitionStatus.ACCEPTED
    assert execution.lineage_run_id is None
    assert execution.exit_code == 0
    assert execution.as_document()["lineage_run_id"] is None


def test_validation_failure_is_recorded_in_lineage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FakeLineage:
        def __init__(self) -> None:
            self.failed: tuple[UUID, BaseException] | None = None

        def migrate(self) -> None:
            return None

        def start_run(self, **_kwargs: object) -> UUID:
            return RUN_ID

        def complete_run(self, _run_id: UUID, _report: object) -> None:
            raise AssertionError("failed validation cannot complete lineage")

        def fail_run(self, run_id: UUID, error: BaseException) -> None:
            self.failed = (run_id, error)

    fake_lineage = FakeLineage()

    def fake_from_dsn(
        _cls: type[PostgresLineageRepository], _dsn: str, _timeout: int
    ) -> FakeLineage:
        return fake_lineage

    monkeypatch.setenv("TRIPML_LINEAGE__DATABASE_URL", "postgresql://secret@postgresql/tripml")
    monkeypatch.setattr(
        PostgresLineageRepository,
        "from_dsn",
        classmethod(fake_from_dsn),
    )
    source = tmp_path / "bad.parquet"
    pq.write_table(pa.table({"PULocationID": [161]}), source)

    with pytest.raises(SourceSchemaError):
        run_ingestion("2024-01", config_path=_config(tmp_path), source=source)

    assert fake_lineage.failed is not None
    assert fake_lineage.failed[0] == RUN_ID
    assert isinstance(fake_lineage.failed[1], SourceSchemaError)
