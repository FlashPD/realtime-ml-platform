"""Verified model serving with event-time Redis features and static degradation."""

from __future__ import annotations

import hashlib
import logging
import math
import tempfile
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from zoneinfo import ZoneInfo

import lightgbm as lgb
import numpy as np
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from mlflow import MlflowClient
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Histogram,
    generate_latest,
)
from starlette.middleware.base import RequestResponseEndpoint

from tripml.contracts import ETARequest, FeatureValue, Prediction, PromotionOutcome
from tripml.online_features import FeatureLookup, LookupOutcome, OnlineFeatures, RedisFeatureStore
from tripml.settings import PlatformSettings
from tripml.tracking import create_client
from tripml.training import STATIC_FEATURES, STREAMING_FEATURES, TrainingRunReport

logger = logging.getLogger(__name__)
NEW_YORK = ZoneInfo("America/New_York")


class ServingError(RuntimeError):
    """A model cannot safely be loaded or used for prediction."""


def static_features(request: ETARequest) -> dict[str, FeatureValue]:
    """Match gold's local wall-clock, Sunday-zero hour-of-week convention."""

    pickup = request.pickup_time.astimezone(NEW_YORK)
    return {
        "pickup_zone_id": request.pickup_zone_id,
        "dropoff_zone_id": request.dropoff_zone_id,
        "pickup_hour_of_week": ((pickup.weekday() + 1) % 7) * 24 + pickup.hour,
        "trip_distance_miles": request.trip_distance_miles,
        "passenger_count": request.passenger_count,
    }


def _verified_bytes(path: Path, expected: str) -> bytes:
    content = path.read_bytes()
    if hashlib.sha256(content).hexdigest() != expected:
        raise ServingError(f"model checksum mismatch: {path.name}")
    return content


@dataclass(frozen=True)
class ServingModel:
    """One startup snapshot containing the streaming model and its static sibling."""

    booster: lgb.Booster
    model_version: str
    registry_version: str | None
    streaming_booster: lgb.Booster | None = None
    streaming_version: str | None = None

    def predict(self, request: ETARequest, online: OnlineFeatures | None = None) -> Prediction:
        features = static_features(request)
        booster = self.booster
        version = self.model_version
        feature_names: tuple[str, ...] = STATIC_FEATURES
        if online is not None:
            if self.streaming_booster is None or self.streaming_version is None:
                raise ServingError("streaming model is not loaded")
            features.update(online.values)
            booster = self.streaming_booster
            version = self.streaming_version
            feature_names += STREAMING_FEATURES
        matrix = np.array([[features[name] for name in feature_names]], dtype=np.float64)
        estimate = float(booster.predict(matrix, num_threads=1)[0])
        if not math.isfinite(estimate) or estimate <= 0:
            raise ServingError("model returned an invalid duration")
        return Prediction(
            trip_id=request.trip_id,
            model_version=version,
            features_used=features,
            feature_timestamps=online.timestamps if online is not None else {},
            feature_fallback=online is None,
            estimated_duration_seconds=estimate,
            served_at=datetime.now(UTC),
        )


def _load_models(
    report: TrainingRunReport, path: Path, streaming_path: Path, *, registry_version: str | None
) -> ServingModel:
    if report.static_features != STATIC_FEATURES or report.streaming_features != STREAMING_FEATURES:
        raise ServingError("unsupported training feature schema")
    content = _verified_bytes(path, report.static_model_sha256)
    booster = lgb.Booster(model_str=content.decode("utf-8"))
    if tuple(booster.feature_name()) != STATIC_FEATURES:
        raise ServingError("native model feature order does not match the training contract")
    streaming_content = _verified_bytes(streaming_path, report.streaming_model_sha256)
    streaming_booster = lgb.Booster(model_str=streaming_content.decode("utf-8"))
    if tuple(streaming_booster.feature_name()) != STATIC_FEATURES + STREAMING_FEATURES:
        raise ServingError("native streaming feature order does not match the training contract")
    if any(item.feature_model_version != "gold-features-v1" for item in report.inputs):
        raise ServingError("unsupported gold feature model version")
    model = ServingModel(
        booster,
        f"{report.run_id}-static",
        registry_version,
        streaming_booster,
        f"{report.run_id}-streaming",
    )
    # Warm the same prediction path before becoming ready, including output validation.
    probe = ETARequest(
        trip_id="startup-probe",
        pickup_zone_id=1,
        dropoff_zone_id=2,
        pickup_time=datetime(2024, 1, 1, tzinfo=UTC),
        trip_distance_miles=3,
        passenger_count=1,
    )
    model.predict(probe)
    model.predict(
        probe,
        OnlineFeatures(
            values={
                "pu_zone_trips_15m": 1,
                "pu_zone_mean_speed_15m": 10,
                "pu_zone_mean_duration_60m": 600,
                "do_zone_trips_60m": 1,
            },
            timestamps=dict.fromkeys(STREAMING_FEATURES, probe.pickup_time),
            age_seconds=0,
        ),
    )
    return model


def load_local_model(bundle: Path) -> ServingModel:
    """Explicit development mode; no registry promotion is claimed for a local bundle."""

    report = TrainingRunReport.model_validate_json((bundle / "manifest.json").read_bytes())
    # Resolve fixed filenames inside the supplied bundle, never embedded training-machine paths.
    return _load_models(
        report, bundle / "static-model.txt", bundle / "streaming-model.txt", registry_version=None
    )


def load_production_model(
    settings: PlatformSettings, *, client: MlflowClient | None = None
) -> ServingModel:
    """Resolve the alias once and verify the static sibling against its promoted bundle."""

    active = client or create_client(settings)
    version = active.get_model_version_by_alias(
        settings.tracking.registered_model_name, settings.tracking.production_alias
    )
    if not version.run_id:
        raise ServingError("production version has no source run")
    run = active.get_run(version.run_id)
    if (
        run.info.status != "FINISHED"
        or run.data.tags.get("tripml.model_role") != "streaming_candidate"
    ):
        raise ServingError("production source is not a finished streaming-model run")

    with tempfile.TemporaryDirectory(prefix="tripml-serving-") as temporary:
        manifest = Path(
            active.download_artifacts(run.info.run_id, "evidence/manifest.json", temporary)
        )
        report = TrainingRunReport.model_validate_json(manifest.read_bytes())
        if (
            report.promotion_decision.outcome is not PromotionOutcome.PROMOTE
            or report.promotion_decision.candidate_version != f"{report.run_id}-streaming"
            or version.tags.get("tripml.bundle_run_id") != report.run_id
            or run.data.tags.get("tripml.bundle_run_id") != report.run_id
            or version.tags.get("tripml.artifact_sha256") != report.streaming_model_sha256
            or run.data.tags.get("tripml.artifact_sha256") != report.streaming_model_sha256
        ):
            raise ServingError("production registry metadata disagrees with promotion evidence")
        streaming_path = Path(
            active.download_artifacts(run.info.run_id, "model/streaming-model.txt", temporary)
        )
        siblings = active.search_runs(
            [run.info.experiment_id],
            filter_string=(
                f"tags.`tripml.bundle_run_id` = '{report.run_id}' AND "
                "tags.`tripml.model_role` = 'static_candidate'"
            ),
            max_results=2,
        )
        if len(siblings) != 1:
            raise ServingError("production bundle must have exactly one static sibling")
        sibling = siblings[0]
        if (
            sibling.info.status != "FINISHED"
            or sibling.data.tags.get("tripml.artifact_sha256") != report.static_model_sha256
        ):
            raise ServingError("static sibling is incomplete or disagrees with the manifest")
        path = Path(
            active.download_artifacts(sibling.info.run_id, "model/static-model.txt", temporary)
        )
        return _load_models(report, path, streaming_path, registry_version=str(version.version))


def create_app(
    settings: PlatformSettings | None = None,
    *,
    bundle: Path | None = None,
    model_loader: Callable[[], ServingModel] | None = None,
    feature_store: RedisFeatureStore | None = None,
) -> FastAPI:
    """Build an isolated app; loading failure aborts startup and never reports readiness."""

    active_settings = settings or PlatformSettings()
    model: ServingModel | None = None
    store = feature_store
    registry = CollectorRegistry()
    requests = Counter(
        "tripml_prediction_requests", "Prediction HTTP responses", ["status"], registry=registry
    )
    fallbacks = Counter(
        "tripml_feature_fallback", "Predictions served with the static fallback", registry=registry
    )
    lookups = Counter(
        "tripml_feature_lookup", "Online feature lookups by outcome", ["outcome"], registry=registry
    )
    feature_age = Histogram(
        "tripml_feature_age_seconds",
        "Accepted snapshot age relative to pickup event time",
        buckets=(0, 1, 5, 15, 30, 60, 120, 300, 600),
        registry=registry,
    )
    latency = Histogram(
        "tripml_prediction_request_duration_seconds",
        "Prediction HTTP duration including validation and serialization",
        buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 1, 5),
        registry=registry,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        nonlocal model, store
        del app
        if model_loader is not None:
            model = model_loader()
        elif bundle is not None:
            model = load_local_model(bundle)
        else:
            model = load_production_model(active_settings)
        try:
            if store is None and active_settings.serving.redis_url is not None:
                if (
                    active_settings.streaming.short_window_seconds != 900
                    or active_settings.streaming.long_window_seconds != 3600
                ):
                    raise ServingError("online features require 900/3600-second windows")
                store = RedisFeatureStore.from_settings(active_settings.serving)
            yield
        finally:
            model = None
            if store is not None and feature_store is None:
                store.close()
                store = None

    app = FastAPI(title="TripML Prediction API", version="0.1.0", lifespan=lifespan)

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request: Request, error: RequestValidationError) -> JSONResponse:
        del request
        # Do not echo raw values: JSON numeric overflow can produce infinity, which
        # cannot itself be serialized in FastAPI's default validation response.
        return JSONResponse(
            status_code=422,
            content={
                "detail": [
                    {key: issue[key] for key in ("loc", "msg", "type")} for issue in error.errors()
                ]
            },
        )

    @app.middleware("http")
    async def measure_request(request: Request, call_next: RequestResponseEndpoint) -> Response:
        if request.url.path != "/v1/eta" or request.method != "POST":
            return await call_next(request)
        started = perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        finally:
            requests.labels(status=str(status)).inc()
            latency.observe(perf_counter() - started)

    @app.get("/healthz")
    def health() -> dict[str, str]:
        return {"status": "alive"}

    @app.get("/readyz")
    def ready() -> dict[str, str | None]:
        if model is None:
            raise HTTPException(status_code=503, detail="model is not ready")
        return {
            "status": "ready",
            "mode": "online_with_fallback" if store is not None else "static_fallback",
            "model_version": model.model_version,
            "streaming_model_version": model.streaming_version,
            "registry_version": model.registry_version,
        }

    @app.get("/metrics", include_in_schema=False)
    def metrics() -> Response:
        return Response(generate_latest(registry), headers={"Content-Type": CONTENT_TYPE_LATEST})

    @app.post("/v1/eta", response_model=Prediction)
    def predict(request: ETARequest) -> Prediction:
        if model is None:
            raise HTTPException(status_code=503, detail="model is not ready")
        try:
            lookup = (
                store.lookup(request)
                if store is not None
                else FeatureLookup(LookupOutcome.DISABLED)
            )
            lookups.labels(outcome=lookup.outcome.value).inc()
            if lookup.features is not None:
                feature_age.observe(lookup.features.age_seconds)
            prediction = model.predict(request, lookup.features)
        except Exception as error:
            logger.exception("prediction failed")
            raise HTTPException(status_code=503, detail="prediction is unavailable") from error
        if prediction.feature_fallback:
            fallbacks.inc()
        return prediction

    return app
