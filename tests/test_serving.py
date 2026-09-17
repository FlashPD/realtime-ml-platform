from __future__ import annotations

import hashlib
import json
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import lightgbm as lgb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient
from mlflow import MlflowClient

from tripml.contracts import ETARequest, Prediction, PromotionOutcome
from tripml.serving import (
    ServingError,
    ServingModel,
    create_app,
    load_local_model,
    load_production_model,
    static_features,
)
from tripml.settings import (
    IngestionSettings,
    ModelSettings,
    PlatformSettings,
    PromotionGateSettings,
    TrackingSettings,
    TrainingSettings,
)
from tripml.tracking import publish_training_report
from tripml.training import STATIC_FEATURES, TrainingRunReport, train_models

PAYLOAD = {
    "trip_id": "trip-123",
    "pickup_zone_id": 161,
    "dropoff_zone_id": 236,
    "pickup_time": "2024-04-15T12:00:00-04:00",
    "trip_distance_miles": 3.2,
    "passenger_count": 2,
}


@pytest.fixture(scope="module")
def trained_report(tmp_path_factory: pytest.TempPathFactory) -> TrainingRunReport:
    root = tmp_path_factory.mktemp("serving-training")
    settings = PlatformSettings(
        ingestion=IngestionSettings(data_root=root / "data"),
        training=TrainingSettings(
            train_months=("2024-01",),
            holdout_month="2024-02",
            artifact_root=root / "artifacts",
            min_training_rows=100,
            min_holdout_rows=50,
            model=ModelSettings(num_leaves=15, learning_rate=0.1, n_estimators=40),
        ),
        promotion_gate=PromotionGateSettings(
            max_bucket_calibration_error_pct=20, max_inference_p95_ms=1000
        ),
    )
    for month, count in (("2024-01", 200), ("2024-02", 100)):
        signal = np.arange(count) % 10
        target = 300 + signal * 50
        path = root / f"data/gold/yellow/month={month}/training_features.parquet"
        path.parent.mkdir(parents=True)
        pq.write_table(
            pa.table(
                {
                    "pickup_zone_id": [161] * count,
                    "dropoff_zone_id": [236] * count,
                    "pickup_hour_of_week": [36] * count,
                    "trip_distance_miles": [3.2] * count,
                    "passenger_count": [2] * count,
                    "pu_zone_trips_15m": signal,
                    "pu_zone_mean_speed_15m": 10 + signal,
                    "pu_zone_mean_duration_60m": target,
                    "do_zone_trips_60m": signal * 2,
                    "actual_duration_seconds": target,
                    "feature_model_version": ["gold-features-v1"] * count,
                }
            ),
            path,
        )
    report = train_models(settings)
    assert report.promotion_decision.outcome is PromotionOutcome.PROMOTE
    return report


@pytest.fixture
def bundle(tmp_path: Path, trained_report: TrainingRunReport) -> Path:
    return Path(shutil.copytree(trained_report.artifact_directory, tmp_path / "relocated"))


@pytest.mark.parametrize(
    ("pickup", "expected"),
    [
        ("2024-04-15T12:00:00-04:00", 36),
        ("2024-04-15T16:00:00Z", 36),
        ("2024-04-15T02:00:00Z", 22),  # Sunday in New York, Monday in UTC.
        ("2024-03-10T06:30:00Z", 1),  # Before the spring DST jump.
        ("2024-03-10T07:30:00Z", 3),
        ("2024-11-03T05:30:00Z", 1),  # Both autumn folds map to local hour one.
        ("2024-11-03T06:30:00Z", 1),
    ],
)
def test_static_time_features_match_gold_convention(pickup: str, expected: int) -> None:
    features = static_features(ETARequest.model_validate(PAYLOAD | {"pickup_time": pickup}))
    assert tuple(features) == STATIC_FEATURES
    assert features["pickup_hour_of_week"] == expected


def test_local_prediction_matches_native_model_after_bundle_relocation(bundle: Path) -> None:
    model = load_local_model(bundle)
    request = ETARequest.model_validate(PAYLOAD)
    expected = lgb.Booster(model_file=str(bundle / "static-model.txt")).predict(
        np.array([[161, 236, 36, 3.2, 2]]), num_threads=1
    )[0]
    prediction = model.predict(request)
    assert prediction.estimated_duration_seconds == pytest.approx(expected)
    assert prediction.feature_fallback is True
    assert prediction.feature_timestamps == {}
    assert prediction.model_version.endswith("-static")
    assert model.registry_version is None


def test_api_lifecycle_predictions_and_metrics(bundle: Path) -> None:
    app = create_app(bundle=bundle)
    with TestClient(app) as client:
        assert client.get("/healthz").json() == {"status": "alive"}
        ready = client.get("/readyz").json()
        assert ready["mode"] == "static_fallback"
        assert ready["registry_version"] is None
        response = client.post("/v1/eta", json=PAYLOAD)
        assert response.status_code == 200
        prediction = Prediction.model_validate(response.json())
        assert prediction.model_version == ready["model_version"]
        assert prediction.trip_id == PAYLOAD["trip_id"]
        assert prediction.features_used["pickup_hour_of_week"] == 36
        assert prediction.feature_fallback is True
        metrics = client.get("/metrics")
        assert "text/plain" in metrics.headers["content-type"]
        assert 'tripml_prediction_requests_total{status="200"} 1.0' in metrics.text
        assert "tripml_feature_fallback_total 1.0" in metrics.text
        assert "tripml_prediction_request_duration_seconds_count 1.0" in metrics.text
        schema = client.get("/openapi.json").json()
        assert "/v1/eta" in schema["paths"]
    assert client.get("/readyz").status_code == 503
    assert client.post("/v1/eta", json=PAYLOAD).status_code == 503
    assert client.get("/healthz").status_code == 200


@pytest.mark.parametrize(
    "invalid",
    [
        {"pickup_time": "2024-04-15T12:00:00"},
        {"pickup_zone_id": 0},
        {"dropoff_zone_id": 266},
        {"passenger_count": 10},
        {"trip_distance_miles": -1},
        {"trip_distance_miles": "NaN"},
        {"trip_distance_miles": "Infinity"},
        {"trip_id": ""},
        {"unexpected": True},
    ],
)
def test_invalid_requests_are_rejected_and_counted(
    bundle: Path, invalid: dict[str, object]
) -> None:
    with TestClient(create_app(bundle=bundle)) as client:
        response = client.post("/v1/eta", json=PAYLOAD | invalid)
        assert response.status_code == 422
        metrics = client.get("/metrics").text
        assert 'tripml_prediction_requests_total{status="422"} 1.0' in metrics
        assert "tripml_feature_fallback_total 0.0" in metrics


def test_corrupt_model_aborts_startup(bundle: Path) -> None:
    (bundle / "static-model.txt").write_text("tampered", encoding="utf-8")
    with (
        pytest.raises(ServingError, match="checksum mismatch"),
        TestClient(create_app(bundle=bundle)),
    ):
        pytest.fail("a corrupt model must never become ready")


def test_numeric_overflow_returns_a_serializable_validation_error(bundle: Path) -> None:
    payload = json.dumps(PAYLOAD).replace(
        '"trip_distance_miles": 3.2', '"trip_distance_miles": 1e999'
    )
    with TestClient(create_app(bundle=bundle)) as client:
        response = client.post(
            "/v1/eta", content=payload, headers={"Content-Type": "application/json"}
        )
        assert response.status_code == 422
        assert response.json()["detail"][0]["loc"] == ["body", "trip_distance_miles"]
        assert "input" not in response.json()["detail"][0]


@pytest.mark.parametrize("native_order", [False, True])
def test_incompatible_feature_schema_is_rejected(bundle: Path, native_order: bool) -> None:
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if native_order:
        model_path = bundle / "static-model.txt"
        model_path.write_text(model_path.read_text().replace("pickup_zone_id", "wrong_zone_id"))
        manifest["static_model_sha256"] = hashlib.sha256(model_path.read_bytes()).hexdigest()
    else:
        manifest["static_features"] = ["wrong_zone_id"]
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ServingError, match="feature"):
        load_local_model(bundle)


@pytest.mark.parametrize("estimate", [float("nan"), float("inf"), 0.0, -1.0])
def test_invalid_model_outputs_are_unavailable_and_not_counted_as_fallbacks(
    estimate: float,
) -> None:
    booster = MagicMock(spec=lgb.Booster)
    booster.predict.return_value = [estimate]
    model = ServingModel(booster, "invalid-model-static", None)
    with TestClient(create_app(model_loader=lambda: model)) as client:
        response = client.post("/v1/eta", json=PAYLOAD)
        assert response.status_code == 503
        assert response.json() == {"detail": "prediction is unavailable"}
        metrics = client.get("/metrics").text
        assert 'tripml_prediction_requests_total{status="503"} 1.0' in metrics
        assert "tripml_feature_fallback_total 0.0" in metrics


def test_concurrent_native_inference_is_consistent_and_has_unique_ids(bundle: Path) -> None:
    model = load_local_model(bundle)
    request = ETARequest.model_validate(PAYLOAD)
    with ThreadPoolExecutor(max_workers=4) as pool:
        predictions = list(pool.map(model.predict, [request] * 20))
    assert len({item.estimated_duration_seconds for item in predictions}) == 1
    assert len({item.prediction_id for item in predictions}) == 20


def test_real_mlflow_publication_to_http_prediction(
    tmp_path: Path, trained_report: TrainingRunReport
) -> None:
    settings = PlatformSettings(
        tracking=TrackingSettings(
            tracking_uri=f"sqlite:///{tmp_path / 'mlflow.db'}",
            local_artifact_root=tmp_path / "mlartifacts",
        )
    )
    publication = publish_training_report(trained_report, settings)
    with TestClient(create_app(settings)) as client:
        ready = client.get("/readyz").json()
        assert ready["registry_version"] == publication.registered_version == "1"
        response = client.post("/v1/eta", json=PAYLOAD)
        assert response.status_code == 200
        assert response.json()["model_version"] == f"{trained_report.run_id}-static"


def _registry_client(report: TrainingRunReport) -> MagicMock:
    client = MagicMock(spec=MlflowClient)
    client.get_model_version_by_alias.return_value = SimpleNamespace(
        version="7",
        run_id="streaming-run",
        tags={
            "tripml.bundle_run_id": report.run_id,
            "tripml.artifact_sha256": report.streaming_model_sha256,
        },
    )
    client.get_run.return_value = SimpleNamespace(
        info=SimpleNamespace(status="FINISHED", run_id="streaming-run", experiment_id="1"),
        data=SimpleNamespace(
            tags={
                "tripml.model_role": "streaming_candidate",
                "tripml.bundle_run_id": report.run_id,
                "tripml.artifact_sha256": report.streaming_model_sha256,
            }
        ),
    )
    client.search_runs.return_value = [
        SimpleNamespace(
            info=SimpleNamespace(status="FINISHED", run_id="static-run"),
            data=SimpleNamespace(tags={"tripml.artifact_sha256": report.static_model_sha256}),
        )
    ]
    client.download_artifacts.side_effect = lambda _run, path, _dest: str(
        Path(report.artifact_directory) / Path(path).name
    )
    return client


@pytest.mark.parametrize(
    ("failure", "message"),
    [
        ("missing_run", "no source run"),
        ("unfinished", "finished streaming-model"),
        ("metadata", "metadata disagrees"),
        ("missing_sibling", "exactly one"),
        ("ambiguous_sibling", "exactly one"),
        ("unfinished_sibling", "static sibling is incomplete"),
        ("sibling_checksum", "static sibling is incomplete"),
    ],
)
def test_registry_inconsistency_fails_closed(
    trained_report: TrainingRunReport, failure: str, message: str
) -> None:
    client = _registry_client(trained_report)
    if failure == "missing_run":
        client.get_model_version_by_alias.return_value.run_id = None
    elif failure == "unfinished":
        client.get_run.return_value.info.status = "RUNNING"
    elif failure == "metadata":
        client.get_model_version_by_alias.return_value.tags["tripml.artifact_sha256"] = "a" * 64
    elif failure == "missing_sibling":
        client.search_runs.return_value = []
    elif failure == "ambiguous_sibling":
        client.search_runs.return_value *= 2
    elif failure == "unfinished_sibling":
        client.search_runs.return_value[0].info.status = "FAILED"
    else:
        client.search_runs.return_value[0].data.tags["tripml.artifact_sha256"] = "a" * 64
    with pytest.raises(ServingError, match=message):
        load_production_model(PlatformSettings(), client=client)


def test_registry_alias_is_resolved_once_and_model_survives_registry_outage(
    trained_report: TrainingRunReport,
) -> None:
    client = _registry_client(trained_report)
    model = load_production_model(PlatformSettings(), client=client)
    client.get_model_version_by_alias.side_effect = RuntimeError("registry offline")
    assert model.registry_version == "7"
    assert model.predict(ETARequest.model_validate(PAYLOAD)).estimated_duration_seconds > 0
    client.get_model_version_by_alias.assert_called_once()
