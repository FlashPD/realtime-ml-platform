import json
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tripml.cli import main
from tripml.contracts import CONTRACTS


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
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
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
