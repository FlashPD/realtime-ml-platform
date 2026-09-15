import json
from pathlib import Path

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
