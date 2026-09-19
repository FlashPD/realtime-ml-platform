from __future__ import annotations

import hashlib
import importlib.util
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType

import pytest

from tripml.benchmark import BenchmarkSettings, RequestSample, summarize
from tripml.contracts import Prediction


@pytest.fixture
def driver() -> ModuleType:
    path = Path(__file__).parents[1] / "scripts/release-operations-driver.py"
    spec = importlib.util.spec_from_file_location("release_operations_driver", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("damage", [None, "timeout", "acknowledged", "success", "overflow"])
def test_outage_requires_bounded_explicit_unconfirmed_failures(
    driver: ModuleType, damage: str | None
) -> None:
    settings = BenchmarkSettings(requests=2, warmup=0)
    samples = [
        RequestSample(
            sequence=index,
            trip_id=str(index),
            scheduled_offset_seconds=0,
            dispatch_lag_ms=0,
            latency_ms=1000,
            http_latency_ms=1000,
            status_code=503,
            publication="unconfirmed",
            outcome="http_error",
        )
        for index in range(2)
    ]
    if damage == "timeout":
        samples[0].outcome = "timeout"
    elif damage == "acknowledged":
        samples[0].publication = "acknowledged"
    elif damage == "success":
        samples[0].outcome = "success"
        samples[0].status_code = 200
    elif damage == "overflow":
        samples[0].outcome = "load_generator_overflow"
    report = summarize(samples, settings, 2)
    checks = driver.phase_checks(report, outage=True)
    assert all(checks.values()) is (damage is None)


@pytest.mark.parametrize("damage", [None, "model", "latency", "warmup"])
def test_healthy_phase_retains_model_latency_and_warmup_gates(
    driver: ModuleType, damage: str | None
) -> None:
    settings = BenchmarkSettings(requests=1, warmup=1)
    sample = RequestSample(
        sequence=0,
        trip_id="test",
        scheduled_offset_seconds=0,
        dispatch_lag_ms=0,
        latency_ms=10,
        http_latency_ms=10,
        status_code=200,
        publication="acknowledged",
        model_version=driver.MODEL,
        feature_fallback=True,
        outcome="success",
    )
    if damage == "model":
        sample.model_version = "unapproved"
    elif damage == "latency":
        sample.latency_ms = 51
    report = summarize([sample], settings, 1)
    report["settings"] = settings.model_dump()
    report["warmup_outcomes"] = {"timeout" if damage == "warmup" else "success": 1}
    assert all(driver.phase_checks(report, outage=False).values()) is (damage is None)


def test_broker_readback_requires_full_prediction_hash(driver: ModuleType) -> None:
    prediction = Prediction(
        schema_version="1.1",
        trip_id="delivery-test",
        model_version=driver.MODEL,
        features_used={"passenger_count": None},
        feature_timestamps={},
        feature_fallback=True,
        estimated_duration_seconds=600,
        served_at=datetime.now(UTC),
    )
    sample = RequestSample(
        sequence=0,
        trip_id=prediction.trip_id,
        scheduled_offset_seconds=0,
        dispatch_lag_ms=0,
        outcome="success",
        model_version=prediction.model_version,
        feature_fallback=True,
        prediction_id=str(prediction.prediction_id),
        prediction_sha256=hashlib.sha256(prediction.model_dump_json().encode()).hexdigest(),
    )
    assert driver.delivery_matches(prediction, sample)
    assert not driver.delivery_matches(
        prediction.model_copy(update={"estimated_duration_seconds": 601}), sample
    )
