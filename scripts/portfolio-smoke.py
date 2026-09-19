"""Verify the relocated approved model against native inference for all April count cohorts."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import platform
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path

import lightgbm as lgb
import numpy as np
from fastapi.testclient import TestClient

from tripml.benchmark import load_requests
from tripml.contracts import Prediction
from tripml.serving import create_app, static_features
from tripml.settings import load_settings
from tripml.training import STATIC_FEATURES, TrainingRunReport


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = args.release_root
    bundle = root / "artifacts/training/passenger-v1_1/98ef1dd9ef4447f7"
    workload = root / "artifacts/workloads/april-passenger-v1_1"
    report = TrainingRunReport.model_validate_json((bundle / "manifest.json").read_bytes())
    require(report.run_id == "98ef1dd9ef4447f7", "unexpected model identity")
    require(report.promotion_role == "static", "unexpected model role")
    require(
        bool(report.promotion_decision.gate_results)
        and all(gate.passed for gate in report.promotion_decision.gate_results),
        "original model gates did not pass",
    )
    manifest = json.loads((workload / "manifest.json").read_bytes())
    requests = load_requests(workload / "requests.jsonl")
    canonical = "".join(request.model_dump_json() + "\n" for request in requests).encode()
    require(
        hashlib.sha256(canonical).hexdigest() == manifest["artifacts_sha256"]["requests.jsonl"],
        "April workload checksum mismatch",
    )
    cohorts = {}
    for request in requests:
        cohort = (
            "unknown"
            if request.passenger_count is None
            else "zero"
            if request.passenger_count == 0
            else "known_positive"
        )
        cohorts.setdefault(cohort, request)
    require(set(cohorts) == {"unknown", "zero", "known_positive"}, "missing cohort")
    settings = load_settings(Path("examples/training/batch-release.yaml"))
    require(settings.publication.bootstrap_servers is None, "demo requires publication disabled")
    native = lgb.Booster(model_file=str(bundle / "static-model.txt"))
    predictions = {}
    with TestClient(create_app(settings, bundle=bundle)) as http:
        readiness = http.get("/readyz").json()
        require(readiness["mode"] == "static_primary", "unexpected serving mode")
        require(readiness["registry_version"] is None, "local demo must not claim registry loading")
        for cohort, request in cohorts.items():
            response = http.post("/v1/eta", json=request.model_dump(mode="json"))
            require(response.status_code == 200, f"{cohort} HTTP failure")
            require(response.headers["X-TripML-Publication"] == "disabled", "publication enabled")
            prediction = Prediction.model_validate(response.json())
            features = static_features(request)
            matrix = np.array([[features[name] for name in STATIC_FEATURES]], dtype=np.float64)
            expected = float(native.predict(matrix, num_threads=1)[0])
            require(
                math.isclose(expected, prediction.estimated_duration_seconds, rel_tol=1e-12),
                f"{cohort} native/API mismatch",
            )
            require(prediction.model_version == "98ef1dd9ef4447f7-static", "wrong model")
            predictions[cohort] = {
                "request": request.model_dump(mode="json"),
                "prediction": prediction.model_dump(mode="json"),
                "native_seconds": expected,
            }
        legacy = cohorts["unknown"].model_dump(mode="json") | {"schema_version": "1.0"}
        require(http.post("/v1/eta", json=legacy).status_code == 422, "legacy null accepted")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as output:
        json.dump(
            {
                "passed": True,
                "captured_at": datetime.now(UTC).isoformat(),
                "scope": "Relocated bundle and in-process HTTP; no training/cluster/load rerun",
                "python": platform.python_version(),
                "platform": platform.platform(),
                "packages": {name: version(name) for name in ("lightgbm", "numpy", "fastapi")},
                "readiness": readiness,
                "predictions": predictions,
                "legacy_null_http_status": 422,
            },
            output,
            indent=2,
        )
        output.write("\n")
    print(f"Verified three April cohorts and legacy null rejection; receipt: {args.output}")


if __name__ == "__main__":
    main()
