"""Evidence corruption must be detected before the release validator writes to MLflow."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType
from unittest.mock import Mock

import pytest

from tripml.contracts import ETARequest, PromotionOutcome
from tripml.settings import PlatformSettings, TrackingSettings
from tripml.training import TrainingRunReport


@pytest.fixture
def validator() -> ModuleType:
    path = Path(__file__).parents[1] / "scripts/validate-release-model.py"
    spec = importlib.util.spec_from_file_location("release_validation", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def evidence(
    tmp_path: Path, nullable_trained_report: TrainingRunReport, validator: ModuleType
) -> tuple[TrainingRunReport, PlatformSettings, Path]:
    settings = PlatformSettings(tracking=TrackingSettings(candidate_role="static"))
    source = tmp_path / "gold.parquet"
    source.write_bytes(b"gold-checksum-fixture")
    inputs = (
        nullable_trained_report.inputs[-1].model_copy(
            update={
                "path": str(source),
                "sha256": validator.digest(source),
            }
        ),
    )
    report = nullable_trained_report.model_copy(
        update={
            "inputs": inputs,
            "config_fingerprint": settings.fingerprint,
        }
    )
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            {
                "month": report.holdout_month,
                "output_sha256": inputs[0].sha256,
                "inputs": [
                    {"sha256": "silver-fixture", "path": "silver/month=2024-02/trips.parquet"},
                    {"sha256": "context-fixture", "path": "silver/month=2024-01/trips.parquet"},
                ],
            }
        )
    )
    workload = tmp_path / "workload"
    workload.mkdir()
    request = ETARequest.model_validate(
        {
            "schema_version": "1.1",
            "trip_id": "release-check",
            "pickup_zone_id": 161,
            "dropoff_zone_id": 236,
            "pickup_time": "2024-02-01T12:00:00-05:00",
            "trip_distance_miles": 3.2,
            "passenger_count": None,
        }
    )
    requests = workload / "requests.jsonl"
    requests.write_text(request.model_dump_json() + "\n")
    (workload / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "month": report.holdout_month,
                "source": {"silver_sha256": "silver-fixture"},
                "artifacts_sha256": {"requests.jsonl": validator.digest(requests)},
            }
        )
    )
    return report, settings, workload


def test_verifies_heldout_lineage_and_canonical_requests(
    evidence: tuple[TrainingRunReport, PlatformSettings, Path],
    validator: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report, settings, workload = evidence
    # Native-bundle integrity is tested separately in training/tracking tests.
    monkeypatch.setattr(validator, "_load_verified_report", lambda *_: report)
    verified = validator.verify_inputs(report, settings, workload)
    assert verified[report.inputs[0].path] == report.inputs[0].sha256
    assert str(workload / "requests.jsonl") in verified


@pytest.mark.parametrize(
    "damage", ["gold", "requests", "month", "silver", "context", "config", "gates"]
)
def test_bad_evidence_never_reaches_registry(
    damage: str,
    evidence: tuple[TrainingRunReport, PlatformSettings, Path],
    validator: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    report, settings, workload = evidence
    if damage == "gold":
        Path(report.inputs[0].path).write_bytes(b"changed")
    elif damage == "requests":
        path = workload / "requests.jsonl"
        path.write_text(path.read_text().replace('"passenger_count":null', '"passenger_count":0'))
    elif damage in {"month", "silver", "context"}:
        path = workload / "manifest.json"
        manifest = json.loads(path.read_text())
        if damage == "month":
            manifest["month"] = "2024-01"
        elif damage == "context":
            manifest["source"]["silver_sha256"] = "context-fixture"
        else:
            manifest["source"]["silver_sha256"] = "other-population"
        path.write_text(json.dumps(manifest))
    elif damage == "config":
        settings = settings.model_copy(
            update={
                "promotion_gate": settings.promotion_gate.model_copy(
                    update={"max_inference_p95_ms": 99}
                )
            }
        )
    else:
        report = report.model_copy(
            update={
                "promotion_decision": report.promotion_decision.model_copy(
                    update={
                        "outcome": PromotionOutcome.REJECT,
                    }
                )
            }
        )
    monkeypatch.setattr(validator, "_load_verified_report", lambda *_: report)
    registry = Mock(side_effect=AssertionError("registry must not be reached"))
    monkeypatch.setattr(validator, "create_client", registry)
    with pytest.raises(ValueError, match=r"checksum|holdout|gold input|configuration|gates"):
        validator.validate(report, settings, workload, tmp_path)
    registry.assert_not_called()
