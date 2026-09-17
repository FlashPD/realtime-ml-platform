from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tripml.contracts import ModelMetrics, PromotionOutcome
from tripml.settings import (
    IngestionSettings,
    ModelSettings,
    PlatformSettings,
    PromotionGateSettings,
    TrainingSettings,
)
from tripml.training import (
    HierarchicalMedianBaseline,
    TrainingDataError,
    TrainingError,
    calculate_metrics,
    evaluate_promotion,
    max_bucket_calibration_error_pct,
    train_models,
)

NOW = datetime(2024, 5, 1, tzinfo=UTC)


def _metrics(mae: float, latency: float = 1.0) -> ModelMetrics:
    return ModelMetrics(
        mae_seconds=mae,
        rmse_seconds=mae * 1.2,
        mape_pct=10,
        inference_p95_ms=latency,
    )


def test_hierarchical_baseline_uses_exact_fallback_and_global_levels() -> None:
    matrix = np.array(
        [
            [1, 2, 3, 1.0, 1],
            [1, 2, 3, 1.0, 1],
            [1, 4, 3, 1.0, 1],
            [9, 8, 7, 1.0, 1],
        ],
        dtype=np.float64,
    )
    target = np.array([100, 200, 500, 900], dtype=np.float64)
    baseline = HierarchicalMedianBaseline.fit(matrix, target)
    examples = np.array(
        [
            [1, 2, 3, 99.0, 9],
            [1, 99, 3, 99.0, 9],
            [99, 99, 99, 99.0, 9],
        ],
        dtype=np.float64,
    )

    predictions = baseline.predict(examples)

    assert predictions.tolist() == [150.0, 200.0, 350.0]


def test_metrics_and_distance_bucket_calibration_have_known_answers() -> None:
    actual = np.array([100.0, 200.0, 300.0, 400.0])
    predicted = np.array([90.0, 220.0, 270.0, 440.0])
    distance = np.array([1.0, 3.0, 7.0, 12.0])

    metrics = calculate_metrics(actual, predicted, inference_p95_ms=2.5)
    calibration = max_bucket_calibration_error_pct(actual, predicted, distance)

    assert metrics.mae_seconds == pytest.approx(25.0)
    assert metrics.rmse_seconds == pytest.approx(np.sqrt(750))
    assert metrics.mape_pct == pytest.approx(10.0)
    assert calibration == pytest.approx(10.0)


@pytest.mark.parametrize(
    ("candidate", "calibration", "latency", "expected"),
    [
        (80.0, 5.0, 2.0, PromotionOutcome.PROMOTE),
        (90.0, 5.0, 2.0, PromotionOutcome.REJECT),
        (80.0, 15.0, 2.0, PromotionOutcome.REJECT),
        (80.0, 5.0, 8.0, PromotionOutcome.REJECT),
    ],
)
def test_promotion_gate_enforces_every_threshold(
    candidate: float,
    calibration: float,
    latency: float,
    expected: PromotionOutcome,
) -> None:
    decision = evaluate_promotion(
        candidate_version="candidate-1",
        candidate_metrics=_metrics(candidate, latency),
        baseline_metrics=_metrics(100),
        max_calibration_error_pct=calibration,
        settings=PromotionGateSettings(
            min_mae_improvement_vs_baseline_pct=15,
            max_bucket_calibration_error_pct=10,
            max_inference_p95_ms=5,
        ),
        decided_at=NOW,
    )

    assert decision.outcome is expected


def test_promotion_gate_compares_existing_production_and_handles_zero_mae() -> None:
    rejected = evaluate_promotion(
        candidate_version="candidate-2",
        candidate_metrics=_metrics(80),
        baseline_metrics=_metrics(100),
        production_version="production-1",
        production_metrics=_metrics(80.5),
        max_calibration_error_pct=5,
        settings=PromotionGateSettings(),
        decided_at=NOW,
    )
    perfect = evaluate_promotion(
        candidate_version="candidate-perfect",
        candidate_metrics=_metrics(0),
        baseline_metrics=_metrics(0),
        max_calibration_error_pct=0,
        settings=PromotionGateSettings(min_mae_improvement_vs_baseline_pct=0),
        decided_at=NOW,
    )

    assert rejected.outcome is PromotionOutcome.REJECT
    assert rejected.gate_results[-1].rule == "mae_improvement_vs_production_pct"
    assert perfect.outcome is PromotionOutcome.PROMOTE


def _write_gold(
    data_root: Path,
    month: str,
    row_count: int,
    *,
    feature_model_version: str = "gold-features-v1",
) -> None:
    indices = np.arange(row_count)
    signal = indices % 10
    target = 300 + signal * 50
    destination = data_root / f"gold/yellow/month={month}/training_features.parquet"
    destination.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "pickup_zone_id": np.full(row_count, 1, dtype=np.int16),
                "dropoff_zone_id": np.full(row_count, 2, dtype=np.int16),
                "pickup_hour_of_week": np.full(row_count, 3, dtype=np.int16),
                "trip_distance_miles": np.full(row_count, 3.0),
                "passenger_count": np.full(row_count, 1, dtype=np.int16),
                "pu_zone_trips_15m": signal.astype(np.int32),
                "pu_zone_mean_speed_15m": (10 + signal).astype(np.float64),
                "pu_zone_mean_duration_60m": target.astype(np.float64),
                "do_zone_trips_60m": (signal * 2).astype(np.int32),
                "actual_duration_seconds": target.astype(np.int32),
                "feature_model_version": [feature_model_version] * row_count,
            }
        ),
        destination,
    )


def _settings(tmp_path: Path) -> PlatformSettings:
    return PlatformSettings(
        ingestion=IngestionSettings(data_root=tmp_path / "data"),
        training=TrainingSettings(
            train_months=("2024-01",),
            holdout_month="2024-02",
            artifact_root=tmp_path / "artifacts",
            min_training_rows=100,
            min_holdout_rows=50,
            model=ModelSettings(num_leaves=15, learning_rate=0.1, n_estimators=40, seed=7),
        ),
        promotion_gate=PromotionGateSettings(
            min_mae_improvement_vs_baseline_pct=15,
            max_bucket_calibration_error_pct=20,
            max_inference_p95_ms=100,
        ),
    )


def test_training_compares_candidates_gates_and_publishes_native_artifacts(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    _write_gold(settings.ingestion.data_root, "2024-01", 200)
    _write_gold(settings.ingestion.data_root, "2024-02", 100)

    report = train_models(settings, now=lambda: NOW)
    repeated = train_models(settings, now=lambda: datetime(2024, 5, 2, tzinfo=UTC))

    assert report == repeated
    assert report.train_rows == 200
    assert report.holdout_rows == 100
    assert report.streaming_candidate_metrics.mae_seconds < report.baseline_metrics.mae_seconds
    assert report.streaming_candidate_metrics.mae_seconds < (
        report.static_candidate_metrics.mae_seconds
    )
    assert report.promotion_decision.outcome is PromotionOutcome.PROMOTE
    assert Path(report.baseline_path).is_file()
    assert Path(report.static_model_path).read_text(encoding="utf-8").startswith("tree")
    assert Path(report.streaming_model_path).read_text(encoding="utf-8").startswith("tree")
    assert "Holdout results" in Path(report.model_card_path).read_text(encoding="utf-8")
    assert len(report.streaming_model_sha256) == 64
    assert report.streaming_model_sha256 != "0" * 64
    manifest = json.loads(
        (Path(report.artifact_directory) / "manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["run_id"] == report.run_id

    Path(report.streaming_model_path).write_text("tampered", encoding="utf-8")
    with pytest.raises(TrainingError, match="manifest verification"):
        train_models(settings)


def test_training_requires_every_configured_gold_partition(tmp_path: Path) -> None:
    with pytest.raises(TrainingDataError, match="gold feature partition"):
        train_models(_settings(tmp_path))


def test_training_rejects_small_or_incompatible_datasets(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _write_gold(settings.ingestion.data_root, "2024-01", 50)
    _write_gold(settings.ingestion.data_root, "2024-02", 100)
    with pytest.raises(TrainingDataError, match="training data has 50 rows"):
        train_models(settings)

    other_settings = _settings(tmp_path / "other")
    _write_gold(other_settings.ingestion.data_root, "2024-01", 200)
    _write_gold(
        other_settings.ingestion.data_root,
        "2024-02",
        100,
        feature_model_version="gold-features-v2",
    )
    with pytest.raises(TrainingDataError, match="different feature models"):
        train_models(other_settings)
