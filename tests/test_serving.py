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
import pytest
from fastapi.testclient import TestClient
from mlflow import MlflowClient
from pydantic import SecretStr
from redis import Redis
from redis.exceptions import TimeoutError as RedisTimeoutError

from tripml.contracts import ETARequest, OnlineZoneWindowFeatures, Prediction
from tripml.online_features import RedisFeatureStore
from tripml.publication import KafkaPredictionPublisher, PublicationError, PublicationOutcome
from tripml.serving import (
    ServingError,
    ServingModel,
    create_app,
    load_local_model,
    load_production_model,
    static_features,
)
from tripml.settings import (
    PlatformSettings,
    PublicationSettings,
    ServingSettings,
    StreamingSettings,
    TrackingSettings,
)
from tripml.tracking import publish_training_report
from tripml.training import STATIC_FEATURES, TrainingRunReport

PAYLOAD = {
    "trip_id": "trip-123",
    "pickup_zone_id": 161,
    "dropoff_zone_id": 236,
    "pickup_time": "2024-04-15T12:00:00-04:00",
    "trip_distance_miles": 3.2,
    "passenger_count": 2,
}


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
        assert response.headers["X-TripML-Publication"] == "disabled"
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


def test_http_success_publishes_the_exact_prediction_once(bundle: Path) -> None:
    publisher = MagicMock(spec=KafkaPredictionPublisher)
    with TestClient(create_app(bundle=bundle, publisher=publisher)) as client:
        assert client.get("/readyz").json()["publication_mode"] == "acknowledged"
        response = client.post("/v1/eta", json=PAYLOAD)
        assert response.status_code == 200
        assert response.headers["X-TripML-Publication"] == "acknowledged"
        publisher.publish.assert_called_once_with(Prediction.model_validate(response.json()))
        assert (
            'tripml_prediction_publication_total{outcome="acknowledged"} 1.0'
            in client.get("/metrics").text
        )
        client.post("/v1/eta", json={})
        publisher.publish.assert_called_once()
    publisher.close.assert_not_called()  # The injected publisher remains caller-owned.


@pytest.mark.parametrize(
    "outcome",
    [
        PublicationOutcome.QUEUE_FULL,
        PublicationOutcome.FAILED,
        PublicationOutcome.TIMEOUT,
        PublicationOutcome.CLOSED,
    ],
)
def test_unconfirmed_publication_returns_503_and_correlation_id(
    bundle: Path,
    outcome: PublicationOutcome,
) -> None:
    publisher = MagicMock(spec=KafkaPredictionPublisher)
    publisher.publish.side_effect = PublicationError(outcome)
    with TestClient(create_app(bundle=bundle, publisher=publisher)) as client:
        response = client.post("/v1/eta", json=PAYLOAD)
        assert response.status_code == 503
        assert response.headers["X-TripML-Publication"] == "unconfirmed"
        details = response.json()["detail"]
        assert details["prediction_id"] == str(publisher.publish.call_args.args[0].prediction_id)
        assert details["outcome"] == outcome.value
        assert details["delivery_unknown"] == (
            outcome in {PublicationOutcome.FAILED, PublicationOutcome.TIMEOUT}
        )
        metrics = client.get("/metrics").text
        assert f'tripml_prediction_publication_total{{outcome="{outcome.value}"}} 1.0' in metrics
        assert "tripml_feature_fallback_total 0.0" in metrics


def test_owned_publisher_is_closed_and_redis_cleanup_survives_flush_failure(
    bundle: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publisher = MagicMock(spec=KafkaPredictionPublisher)
    publisher.close.side_effect = RuntimeError("flush failed")
    store = MagicMock(spec=RedisFeatureStore)
    monkeypatch.setattr(KafkaPredictionPublisher, "from_settings", lambda _settings: publisher)
    monkeypatch.setattr(RedisFeatureStore, "from_settings", lambda _settings: store)
    settings = PlatformSettings(
        publication=PublicationSettings(bootstrap_servers="broker:9092"),
        serving=ServingSettings(redis_url=SecretStr("redis://localhost")),
    )
    with (
        pytest.raises(RuntimeError, match="flush failed"),
        TestClient(create_app(settings, bundle=bundle)) as client,
    ):
        assert client.get("/readyz").json()["publication_mode"] == "acknowledged"
    publisher.close.assert_called_once()
    store.close.assert_called_once()


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


def test_static_registry_to_http_uses_only_approved_static_model(
    tmp_path: Path,
    static_trained_report: TrainingRunReport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = PlatformSettings(
        tracking=TrackingSettings(
            candidate_role="static",
            registered_model_name="tripml-trip-duration-static",
            tracking_uri=f"sqlite:///{tmp_path / 'mlflow.db'}",
            local_artifact_root=tmp_path / "mlartifacts",
        ),
        serving=ServingSettings(redis_url=SecretStr("redis://localhost:1")),
    )
    publication = publish_training_report(static_trained_report, settings)
    redis_factory = MagicMock(side_effect=AssertionError("static release must not construct Redis"))
    monkeypatch.setattr(RedisFeatureStore, "from_settings", redis_factory)
    publisher = MagicMock(spec=KafkaPredictionPublisher)
    with TestClient(create_app(settings, publisher=publisher)) as client:
        ready = client.get("/readyz").json()
        assert ready["mode"] == "static_primary"
        assert ready["registry_version"] == publication.registered_version == "1"
        assert ready["streaming_model_version"] is None
        response = client.post("/v1/eta", json=PAYLOAD)
        assert response.status_code == 200
        prediction = Prediction.model_validate(response.json())
        assert prediction.model_version == f"{static_trained_report.run_id}-static"
        assert tuple(prediction.features_used) == STATIC_FEATURES
        assert prediction.feature_timestamps == {}
        assert prediction.feature_fallback is True  # Version-1 contract: static features used.
        publisher.publish.assert_called_once_with(prediction)
        assert response.headers["X-TripML-Publication"] == "acknowledged"
    redis_factory.assert_not_called()


def test_static_local_bundle_needs_no_streaming_artifact_or_redis(
    tmp_path: Path,
    static_trained_report: TrainingRunReport,
) -> None:
    bundle = Path(shutil.copytree(static_trained_report.artifact_directory, tmp_path / "bundle"))
    (bundle / "streaming-model.txt").unlink()
    store = MagicMock(spec=RedisFeatureStore)
    with TestClient(create_app(bundle=bundle, feature_store=store)) as client:
        assert client.get("/readyz").json()["mode"] == "static_primary"
        assert client.post("/v1/eta", json=PAYLOAD).status_code == 200
    store.lookup.assert_not_called()


@pytest.mark.parametrize("failure", ["role", "checksum", "rejected", "candidate", "gate"])
def test_static_registry_inconsistency_fails_closed(
    tmp_path: Path,
    static_trained_report: TrainingRunReport,
    failure: str,
) -> None:
    report = static_trained_report
    client = _registry_client(report)
    version = client.get_model_version_by_alias.return_value
    version.tags["tripml.artifact_sha256"] = report.static_model_sha256
    run = client.get_run.return_value
    run.data.tags["tripml.model_role"] = "static_candidate"
    run.data.tags["tripml.artifact_sha256"] = report.static_model_sha256
    manifest = json.loads(Path(report.artifact_directory, "manifest.json").read_text())
    if failure == "role":
        run.data.tags["tripml.model_role"] = "streaming_candidate"
    elif failure == "checksum":
        version.tags["tripml.artifact_sha256"] = report.streaming_model_sha256
    elif failure == "rejected":
        manifest["promotion_decision"]["outcome"] = "reject"
        manifest["promotion_decision"]["gate_results"][0]["passed"] = False
    elif failure == "candidate":
        manifest["promotion_decision"]["candidate_version"] = f"{report.run_id}-streaming"
    else:
        manifest["promotion_decision"]["gate_results"][0]["passed"] = False
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    client.download_artifacts.side_effect = lambda *_args: str(path)
    with pytest.raises(ServingError, match="promotion evidence"):
        load_production_model(PlatformSettings(), client=client)
    client.search_runs.assert_not_called()


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
        ("unfinished", "finished candidate-model"),
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


def test_http_streaming_inference_matches_native_model_and_falls_back_on_outage(
    bundle: Path,
    online_snapshots: tuple[OnlineZoneWindowFeatures, ...],
) -> None:
    redis = MagicMock(spec=Redis)
    redis.mget.return_value = [item.model_dump_json() for item in online_snapshots]
    store = RedisFeatureStore(redis, ServingSettings())
    expected = lgb.Booster(model_file=str(bundle / "streaming-model.txt")).predict(
        np.array([[161, 236, 36, 3.2, 2, 5, 12, 600, 7]]), num_threads=1
    )[0]
    with TestClient(create_app(bundle=bundle, feature_store=store)) as client:
        ready = client.get("/readyz").json()
        assert ready["mode"] == "online_with_fallback"
        response = client.post("/v1/eta", json=PAYLOAD)
        assert response.status_code == 200
        prediction = Prediction.model_validate(response.json())
        assert prediction.estimated_duration_seconds == pytest.approx(expected)
        assert prediction.model_version == ready["streaming_model_version"]
        assert prediction.feature_fallback is False
        assert len(prediction.features_used) == 9
        assert len(prediction.feature_timestamps) == 4
        assert set(prediction.feature_timestamps.values()) == {online_snapshots[0].window_end}
        redis.mget.side_effect = RedisTimeoutError("unavailable")
        fallback = client.post("/v1/eta", json=PAYLOAD).json()
        assert fallback["model_version"] == ready["model_version"]
        assert fallback["feature_fallback"] is True
        assert fallback["feature_timestamps"] == {}
        assert client.get("/readyz").status_code == 200
        metrics = client.get("/metrics").text
        assert 'tripml_feature_lookup_total{outcome="fresh"} 1.0' in metrics
        assert 'tripml_feature_lookup_total{outcome="unavailable"} 1.0' in metrics
        assert "tripml_feature_fallback_total 1.0" in metrics
        assert "tripml_feature_age_seconds_count 1.0" in metrics


def test_configured_redis_pool_is_closed_on_shutdown(
    bundle: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = MagicMock(spec=RedisFeatureStore)
    monkeypatch.setattr(RedisFeatureStore, "from_settings", lambda _settings: store)
    settings = PlatformSettings(serving=ServingSettings(redis_url=SecretStr("redis://localhost")))
    with TestClient(create_app(settings, bundle=bundle)) as client:
        assert client.get("/readyz").json()["mode"] == "online_with_fallback"
    store.close.assert_called_once()


def test_nonstandard_windows_cannot_use_online_feature_schema(bundle: Path) -> None:
    settings = PlatformSettings(
        serving=ServingSettings(redis_url=SecretStr("redis://localhost")),
        streaming=StreamingSettings(short_window_seconds=600),
    )
    with (
        pytest.raises(ServingError, match="900/3600"),
        TestClient(create_app(settings, bundle=bundle)),
    ):
        pytest.fail("incompatible windows must not start")


def test_corrupt_streaming_model_prevents_startup_even_when_redis_is_disabled(bundle: Path) -> None:
    (bundle / "streaming-model.txt").write_text("corrupt")
    with pytest.raises(ServingError, match="checksum"):
        load_local_model(bundle)


def test_streaming_inference_requires_loaded_streaming_model() -> None:
    from tripml.online_features import OnlineFeatures

    model = ServingModel(MagicMock(spec=lgb.Booster), "static-only", None)
    with pytest.raises(ServingError, match="streaming model is not loaded"):
        model.predict(ETARequest.model_validate(PAYLOAD), OnlineFeatures({}, {}, 0))


@pytest.mark.parametrize("native_order", [False, True])
def test_streaming_model_rejects_incompatible_gold_version_or_native_order(
    bundle: Path,
    native_order: bool,
) -> None:
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if native_order:
        model_path = bundle / "streaming-model.txt"
        model_path.write_text(model_path.read_text().replace("pu_zone_trips_15m", "wrong_window"))
        manifest["streaming_model_sha256"] = hashlib.sha256(model_path.read_bytes()).hexdigest()
    else:
        manifest["inputs"][0]["feature_model_version"] = "unsupported"
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ServingError, match="feature"):
        load_local_model(bundle)
