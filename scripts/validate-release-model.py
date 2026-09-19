"""Publish an eligible static batch model and capture local registry-to-HTTP evidence.

Uses an existing training report; never retrains or changes gates. Requires the original
bundle and gold inputs, a matching configuration, and a checksummed held-out workload.
This is an in-process smoke test with publication disabled, not a load or cluster test.
Every attempt requires a fresh output directory, including failed attempts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import platform
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any

import lightgbm as lgb
import numpy as np
from fastapi.testclient import TestClient
from pydantic import SecretStr

from tripml.benchmark import load_requests
from tripml.contracts import Prediction, PromotionOutcome
from tripml.serving import create_app, static_features
from tripml.settings import PlatformSettings, load_settings
from tripml.tracking import create_client, publish_training_report
from tripml.training import STATIC_FEATURES, TrainingRunReport, _load_verified_report


def digest(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def verify_inputs(
    report: TrainingRunReport, settings: PlatformSettings, workload: Path
) -> dict[str, str]:
    """Fail before registry writes if the evidence or held-out workload has changed."""
    require(
        report.config_fingerprint == settings.fingerprint, "configuration differs from training"
    )
    require(report.promotion_role == "static", "this validation requires static promotion")
    require(settings.publication.bootstrap_servers is None, "smoke requires publication disabled")
    require(report.promotion_decision.outcome is PromotionOutcome.PROMOTE, "model gates rejected")
    require(
        bool(report.promotion_decision.gate_results)
        and all(gate.passed for gate in report.promotion_decision.gate_results),
        "every promotion gate must pass",
    )
    bundle = Path(report.artifact_directory)
    require(_load_verified_report(bundle / "manifest.json", bundle) == report, "bundle differs")
    verified = {}
    for item in report.inputs:
        require(digest(Path(item.path)) == item.sha256, f"gold checksum mismatch: {item.month}")
        verified[item.path] = item.sha256
    holdout = next(item for item in report.inputs if item.month == report.holdout_month)
    gold_manifest_path = Path(holdout.path).parent / "manifest.json"
    gold = json.loads(gold_manifest_path.read_bytes())
    manifest_path = workload / "manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    require(manifest["schema_version"] == "1.0", "unsupported workload manifest")
    require(manifest["month"] == report.holdout_month, "workload is not from the holdout month")
    require(gold["month"] == report.holdout_month, "gold manifest has the wrong holdout month")
    require(gold["output_sha256"] == holdout.sha256, "gold manifest differs from training")
    require(
        any(
            item["sha256"] == manifest["source"]["silver_sha256"]
            and Path(item["path"]).parent.name == f"month={report.holdout_month}"
            for item in gold["inputs"]
        ),
        "workload silver is not the target-month gold input",
    )
    requests_path = workload / "requests.jsonl"
    requests = load_requests(requests_path)
    canonical = "".join(request.model_dump_json() + "\n" for request in requests).encode()
    require(
        hashlib.sha256(canonical).hexdigest() == manifest["artifacts_sha256"]["requests.jsonl"],
        "workload requests checksum mismatch",
    )
    for path in (gold_manifest_path, manifest_path, requests_path):
        verified[str(path)] = digest(path)
    return verified


def validate(
    report: TrainingRunReport, settings: PlatformSettings, workload: Path, output: Path
) -> dict[str, Any]:
    verified = verify_inputs(report, settings, workload)
    requests = load_requests(workload / "requests.jsonl")
    selected = {}
    for request in requests:
        group = (
            "unknown"
            if request.passenger_count is None
            else ("zero" if request.passenger_count == 0 else "known_positive")
        )
        selected.setdefault(group, request)
    require(set(selected) == {"unknown", "zero", "known_positive"}, "workload lacks a count cohort")
    native = lgb.Booster(model_file=report.static_model_path)
    client = create_client(settings)
    publication = publish_training_report(report, settings, client=client)
    # Write receipts immediately so a later smoke failure does not hide a registry mutation.
    (output / "publication.json").write_text(publication.model_dump_json(indent=2) + "\n")
    registry = client.get_model_version_by_alias(
        settings.tracking.registered_model_name, settings.tracking.production_alias
    )
    require(str(registry.version) == publication.registered_version, "alias is not this bundle")
    retry = publish_training_report(report, settings, client=client)
    require(not retry.alias_updated, "publication retry unexpectedly changed alias")
    require(retry.registered_version == publication.registered_version, "retry created a version")
    serving = settings.model_copy(
        update={
            "serving": settings.serving.model_copy(
                update={"redis_url": SecretStr("redis://127.0.0.1:1")}
            )
        }
    )
    smokes = {}
    with TestClient(create_app(serving)) as http:
        ready_response = http.get("/readyz")
        require(ready_response.status_code == 200, "API did not become ready")
        readiness = ready_response.json()
        require(readiness["mode"] == "static_primary", "wrong serving mode")
        require(readiness["registry_version"] == str(registry.version), "wrong registry version")
        require(readiness["feature_model_version"] == "gold-features-v2", "wrong feature contract")
        for group, request in selected.items():
            response = http.post("/v1/eta", json=request.model_dump(mode="json"))
            require(response.status_code == 200, f"{group} request failed")
            prediction = Prediction.model_validate(response.json())
            features = static_features(request)
            matrix = np.array([[features[name] for name in STATIC_FEATURES]], dtype=np.float64)
            expected = float(native.predict(matrix, num_threads=1)[0])
            require(
                math.isclose(prediction.estimated_duration_seconds, expected, rel_tol=1e-12),
                f"{group} native/API prediction mismatch",
            )
            require(prediction.model_version == f"{report.run_id}-static", "wrong model bundle")
            require(prediction.schema_version == "1.1", "wrong prediction schema")
            require(prediction.features_used == features, "API changed the input features")
            require(
                prediction.feature_fallback and not prediction.feature_timestamps, "wrong features"
            )
            require(
                response.headers["X-TripML-Publication"] == "disabled", "wrong publication mode"
            )
            smokes[group] = {
                "request": request.model_dump(mode="json"),
                "prediction": prediction.model_dump(mode="json"),
                "native_prediction_seconds": expected,
                "http_status": response.status_code,
            }
        invalid = selected["unknown"].model_dump(mode="json") | {"schema_version": "1.0"}
        rejected = http.post("/v1/eta", json=invalid)
        require(rejected.status_code == 422, "legacy contract accepted unknown passenger count")
        metrics = http.get("/metrics").text
        require(
            'tripml_feature_lookup_total{outcome="disabled"} 3.0' in metrics, "Redis not bypassed"
        )
    (output / "metrics.txt").write_text(metrics)
    return {
        "validated_at": datetime.now(UTC).isoformat(),
        "scope": "Local MLflow publication and in-process API smoke; no cluster or load claims",
        "configuration": settings.model_dump(mode="json"),
        "training": report.model_dump(mode="json"),
        "publication": publication.model_dump(mode="json"),
        "publication_retry": retry.model_dump(mode="json"),
        "readiness": readiness,
        "application_smokes": smokes,
        "legacy_null_http_status": rejected.status_code,
        "configured_redis": "redis://127.0.0.1:1",
        "publication_mode": "disabled",
        "verified_input_checksums": verified,
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "packages": {
                name: version(name) for name in ("lightgbm", "mlflow", "numpy", "pyarrow")
            },
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--training-report", type=Path, required=True)
    parser.add_argument("--workload", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    try:
        settings = load_settings(args.config)
        report = TrainingRunReport.model_validate_json(args.training_report.read_bytes())
        result = validate(report, settings, args.workload, args.output)
        result["evidence_checksums"] = {
            str(path): digest(path) for path in (Path(__file__), args.config, args.training_report)
        }
        (args.output / "validation.json").write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n"
        )
    except Exception as error:
        (args.output / "failure.json").write_text(
            json.dumps(
                {
                    "failed_at": datetime.now(UTC).isoformat(),
                    "error": str(error),
                    "error_type": type(error).__name__,
                },
                indent=2,
            )
            + "\n"
        )
        raise
    print(json.dumps({"run_id": report.run_id, "readiness": result["readiness"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
