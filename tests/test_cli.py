import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tripml import tracking as tracking_module
from tripml import training as training_module
from tripml.cli import main
from tripml.contracts import CONTRACTS
from tripml.features import FeatureBuildReport
from tripml.ingestion import PartitionQualityReport
from tripml.lineage import PostgresLineageRepository


def test_serve_passes_configuration_and_bind_options(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    config = tmp_path / "config.yaml"
    config.write_text("serving:\n  p95_latency_objective_ms: 75\n", encoding="utf-8")
    with patch("tripml.serving.create_app") as factory, patch("uvicorn.run") as run:
        assert (
            main(
                [
                    "serve",
                    "--bundle",
                    str(bundle),
                    "--config",
                    str(config),
                    "--host",
                    "127.0.0.1",
                    "--port",
                    "8001",
                ]
            )
            == 0
        )
    assert factory.call_args.args[0].serving.p95_latency_objective_ms == 75
    assert factory.call_args.kwargs == {"bundle": bundle}
    run.assert_called_once_with(factory.return_value, host="127.0.0.1", port=8001)


def test_config_validate_prints_validated_settings(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["config", "validate"])

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert output["valid"] is True
    assert len(output["fingerprint"]) == 64


def test_contract_export_writes_all_schemas(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    destination = tmp_path / "contracts"

    exit_code = main(["contracts", "export", "--output", str(destination)])

    assert exit_code == 0
    assert {path.stem for path in destination.glob("*.json")} == set(CONTRACTS)
    assert f"Exported {len(CONTRACTS)} contracts" in capsys.readouterr().out


def test_ingest_local_partition_reports_acceptance(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lineage_run_id = UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")

    class FakeLineage:
        def __init__(self) -> None:
            self.migrated = False
            self.completed: tuple[UUID, PartitionQualityReport] | None = None

        def migrate(self) -> None:
            self.migrated = True

        def start_run(self, **_kwargs: object) -> UUID:
            return lineage_run_id

        def complete_run(self, run_id: UUID, report: PartitionQualityReport) -> None:
            self.completed = (run_id, report)

        def fail_run(self, _run_id: UUID, _error: BaseException) -> None:
            raise AssertionError("successful ingestion must not record failure")

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
    pickup = datetime(2024, 1, 15, 12)
    source = tmp_path / "source.parquet"
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
        source,
    )
    config = tmp_path / "config.yaml"
    config.write_text(
        f"ingestion:\n  data_root: {tmp_path / 'data'}\n  batch_size: 10\n",
        encoding="utf-8",
    )

    exit_code = main(
        ["ingest", "--month", "2024-01", "--config", str(config), "--source", str(source)]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["status"] == "accepted"
    assert output["valid_rows"] == 1
    assert output["lineage_run_id"] == str(lineage_run_id)
    assert fake_lineage.migrated is True
    assert fake_lineage.completed is not None


def test_feature_build_prints_machine_readable_report(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    report = FeatureBuildReport(
        month="2024-01",
        model_version="gold-features-v1",
        output_path="data/gold/yellow/month=2024-01/training_features.parquet",
        manifest_path="data/gold/yellow/month=2024-01/manifest.json",
        row_count=42,
        output_sha256="b" * 64,
        output_size_bytes=100,
        input_rows=42,
        inputs=(),
        short_window_seconds=900,
        long_window_seconds=3600,
        config_fingerprint="a" * 64,
        built_at=datetime(2024, 2, 1).astimezone(),
    )
    monkeypatch.setattr("tripml.cli.build_gold_features", lambda *_args, **_kwargs: report)

    exit_code = main(["features", "build", "--month", "2024-01"])
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["row_count"] == 42


def test_untracked_train_prints_machine_readable_report(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    class FakeReport:
        def model_dump(self, *, mode: str) -> dict[str, object]:
            assert mode == "json"
            return {"run_id": "0123456789abcdef", "outcome": "promote"}

    monkeypatch.setattr(training_module, "train_models", lambda *_args, **_kwargs: FakeReport())

    exit_code = main(["train", "--no-track"])
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["run_id"] == "0123456789abcdef"


def test_tracked_train_prints_registry_result(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    class FakeWorkflow:
        def model_dump(self, *, mode: str) -> dict[str, object]:
            assert mode == "json"
            return {
                "training": {"run_id": "0123456789abcdef"},
                "tracking": {"production_alias_version": "3"},
            }

    monkeypatch.setattr(
        tracking_module, "run_training_workflow", lambda *_args, **_kwargs: FakeWorkflow()
    )

    exit_code = main(["train"])
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["tracking"]["production_alias_version"] == "3"
