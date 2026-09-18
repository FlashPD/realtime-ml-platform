from __future__ import annotations

import os
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tripml.contracts import OnlineZoneWindowFeatures, PromotionOutcome
from tripml.settings import (
    IngestionSettings,
    ModelSettings,
    PlatformSettings,
    PromotionGateSettings,
    TrainingSettings,
)
from tripml.training import TrainingRunReport, train_models


@pytest.fixture
def helm() -> str:
    binary = os.getenv("TRIPML_HELM") or str(
        Path(__file__).resolve().parents[1] / ".tools/bin/helm"
    )
    if not Path(binary).is_file():
        binary = shutil.which("helm") or ""
    if not binary:
        pytest.skip("Helm is unavailable; run make tools")
    return binary


os.environ.setdefault(
    "AIRFLOW_HOME",
    str(Path(__file__).resolve().parents[1] / ".tripml" / "airflow-tests"),
)


@pytest.fixture
def online_snapshots() -> tuple[OnlineZoneWindowFeatures, ...]:
    cutoff = datetime(2024, 4, 15, 16, tzinfo=UTC)
    return tuple(
        OnlineZoneWindowFeatures.model_validate(
            {
                "zone_role": role,
                "zone_id": zone,
                "window_kind": kind,
                "window_start": cutoff - timedelta(seconds=seconds),
                "window_end": cutoff,
                "computed_at": cutoff + timedelta(seconds=1),
                "source_max_event_time": cutoff - timedelta(seconds=2),
                "trip_count": count,
                "mean_speed_mph": 12,
                "mean_duration_seconds": 600,
                "mean_distance_miles": 2,
            }
        )
        for role, zone, kind, seconds, count in (
            ("pickup", 161, "15m", 900, 5),
            ("pickup", 161, "60m", 3600, 10),
            ("dropoff", 236, "60m", 3600, 7),
        )
    )


@pytest.fixture(scope="session")
def trained_report(tmp_path_factory: pytest.TempPathFactory) -> TrainingRunReport:
    # Keep fixture training independent of a developer's configured platform paths.
    with pytest.MonkeyPatch.context() as isolated:
        for key in tuple(os.environ):
            if key.startswith("TRIPML_"):
                isolated.delenv(key)
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
