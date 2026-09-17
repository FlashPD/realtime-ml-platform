"""Synthetic registry seed for the isolated Kubernetes serving test, never portfolio metrics."""

from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from tripml.contracts import PromotionOutcome
from tripml.settings import (
    IngestionSettings,
    ModelSettings,
    PlatformSettings,
    PromotionGateSettings,
    TrainingSettings,
)
from tripml.tracking import run_training_workflow


def seed_registry() -> None:
    root = Path("/tmp/fixture")
    for month, count in (("2024-01", 200), ("2024-02", 100)):
        signal = np.arange(count) % 10
        target = 300 + signal * 50
        path = root / f"data/gold/yellow/month={month}/training_features.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
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
    report = run_training_workflow(settings)
    assert report.training.promotion_decision.outcome is PromotionOutcome.PROMOTE
    print(f"Synthetic registry fixture ready: version {report.tracking.registered_version}")


if __name__ == "__main__":
    seed_registry()
