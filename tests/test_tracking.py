from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path

from mlflow import MlflowClient

from tripml.contracts import (
    GateRuleResult,
    ModelMetrics,
    PromotionDecision,
    PromotionOutcome,
)
from tripml.settings import PlatformSettings, TrackingSettings
from tripml.tracking import create_client, production_reference, publish_training_report
from tripml.training import TrainingRunReport

NOW = datetime(2024, 5, 1, tzinfo=UTC)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _metrics(mae: float) -> ModelMetrics:
    return ModelMetrics(
        mae_seconds=mae,
        rmse_seconds=mae * 1.2,
        mape_pct=mae / 10,
        inference_p95_ms=2,
    )


def _report(tmp_path: Path, run_id: str, outcome: PromotionOutcome) -> TrainingRunReport:
    directory = tmp_path / "bundles" / run_id
    directory.mkdir(parents=True)
    baseline = directory / "baseline.json"
    static = directory / "static-model.txt"
    streaming = directory / "streaming-model.txt"
    card = directory / "model-card.md"
    baseline.write_text('{"global_median": 600}\n', encoding="utf-8")
    static.write_text("tree\nstatic\n", encoding="utf-8")
    streaming.write_text("tree\nstreaming\n", encoding="utf-8")
    card.write_text("# Model card\n", encoding="utf-8")
    (directory / "evaluation.json").write_text("{}\n", encoding="utf-8")
    (directory / "manifest.json").write_text("{}\n", encoding="utf-8")
    passed = outcome is PromotionOutcome.PROMOTE
    streaming_metrics = _metrics(70 if passed else 95)
    decision = PromotionDecision(
        candidate_version=f"{run_id}-streaming",
        candidate_metrics=streaming_metrics,
        baseline_metrics=_metrics(100),
        gate_results=(
            GateRuleResult(
                rule="mae_improvement_vs_baseline_pct",
                observed=30 if passed else 5,
                threshold=15,
                passed=passed,
            ),
        ),
        outcome=outcome,
        decided_at=NOW,
    )
    return TrainingRunReport(
        run_id=run_id,
        train_months=("2024-01",),
        holdout_month="2024-02",
        train_rows=200,
        holdout_rows=100,
        inputs=(),
        static_features=("pickup_zone_id",),
        streaming_features=("pu_zone_trips_15m",),
        baseline_metrics=_metrics(100),
        static_candidate_metrics=_metrics(80),
        streaming_candidate_metrics=streaming_metrics,
        static_max_bucket_calibration_error_pct=8,
        streaming_max_bucket_calibration_error_pct=5,
        promotion_decision=decision,
        artifact_directory=str(directory),
        baseline_path=str(baseline),
        static_model_path=str(static),
        streaming_model_path=str(streaming),
        model_card_path=str(card),
        baseline_sha256=_sha256(baseline),
        static_model_sha256=_sha256(static),
        streaming_model_sha256=_sha256(streaming),
        model_card_sha256=_sha256(card),
        config_fingerprint="a" * 64,
        trained_at=NOW,
    )


def _settings(tmp_path: Path) -> PlatformSettings:
    database = tmp_path / "mlflow.db"
    return PlatformSettings(
        tracking=TrackingSettings(
            tracking_uri=f"sqlite:///{database}",
            local_artifact_root=tmp_path / "mlartifacts",
            experiment_name="test-training",
            registered_model_name="test-trip-duration",
            production_alias="production",
        )
    )


def _all_runs(client: MlflowClient, experiment_id: str) -> list[object]:
    return list(client.search_runs([experiment_id]))


def test_passing_report_is_tracked_registered_and_idempotent(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    client = create_client(settings)
    report = _report(tmp_path, "1111111111111111", PromotionOutcome.PROMOTE)

    first = publish_training_report(report, settings, client=client)
    repeated = publish_training_report(report, settings, client=client)
    production = production_reference(client, settings)

    assert len(first.runs) == 3
    assert len(_all_runs(client, first.experiment_id)) == 3
    assert first.registered_version == "1"
    assert first.production_alias_version == "1"
    assert first.alias_updated is True
    assert repeated.registered_version == "1"
    assert repeated.production_alias_version == "1"
    assert repeated.alias_updated is False
    assert production is not None
    assert production.version == "1"
    assert production.metrics == report.streaming_candidate_metrics


def test_rejection_does_not_register_or_move_production_alias(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    client = create_client(settings)
    passing = _report(tmp_path, "2222222222222222", PromotionOutcome.PROMOTE)
    rejected = _report(tmp_path, "3333333333333333", PromotionOutcome.REJECT)
    original = publish_training_report(passing, settings, client=client)

    publication = publish_training_report(rejected, settings, client=client)

    assert publication.registered_version is None
    assert publication.production_alias_version == original.production_alias_version == "1"
    assert publication.alias_updated is False
    versions = client.search_model_versions(f"name = '{settings.tracking.registered_model_name}'")
    assert len(versions) == 1
    assert len(_all_runs(client, publication.experiment_id)) == 6
