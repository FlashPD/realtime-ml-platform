from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import pytest
from mlflow import MlflowClient

from tripml.contracts import (
    ModelMetrics,
    PromotionOutcome,
)
from tripml.settings import PlatformSettings, TrackingSettings
from tripml.tracking import (
    TrackingError,
    create_client,
    production_reference,
    publish_training_report,
    run_training_workflow,
)
from tripml.training import TrainingError, TrainingInput, TrainingRunReport, evaluate_promotion

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


def _report(
    tmp_path: Path,
    run_id: str,
    outcome: PromotionOutcome,
    *,
    role: Literal["static", "streaming"] = "streaming",
    production: TrainingRunReport | None = None,
    mae: float = 80,
    holdout_sha256: str = "b" * 64,
) -> TrainingRunReport:
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
    static_metrics = _metrics(mae if passed or role == "streaming" else 95)
    decision = evaluate_promotion(
        candidate_version=f"{run_id}-{role}",
        candidate_metrics=static_metrics if role == "static" else streaming_metrics,
        baseline_metrics=_metrics(100),
        max_calibration_error_pct=8,
        settings=PlatformSettings().promotion_gate,
        decided_at=NOW,
        production_version="1" if production else None,
        production_metrics=production.promotion_decision.candidate_metrics if production else None,
    )
    report = TrainingRunReport(
        promotion_role=role,
        run_id=run_id,
        train_months=("2024-01",),
        holdout_month="2024-02",
        train_rows=200,
        holdout_rows=100,
        inputs=(
            TrainingInput(
                month="2024-02",
                path="gold.parquet",
                sha256=holdout_sha256,
                row_count=100,
                feature_model_version="gold-features-v1",
            ),
        ),
        static_features=("pickup_zone_id",),
        streaming_features=("pu_zone_trips_15m",),
        baseline_metrics=_metrics(100),
        static_candidate_metrics=static_metrics,
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
    (directory / "manifest.json").write_text(report.model_dump_json(), encoding="utf-8")
    return report


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


def _static_settings(tmp_path: Path) -> PlatformSettings:
    settings = _settings(tmp_path)
    return settings.model_copy(
        update={"tracking": settings.tracking.model_copy(update={"candidate_role": "static"})}
    )


def test_static_publication_registers_selected_artifact_and_evidence(tmp_path: Path) -> None:
    settings = _static_settings(tmp_path)
    client = create_client(settings)
    report = _report(tmp_path, "4444444444444444", PromotionOutcome.PROMOTE, role="static")
    publication = publish_training_report(report, settings, client=client)
    repeated = publish_training_report(report, settings, client=client)
    version = client.get_model_version_by_alias(
        settings.tracking.registered_model_name, "production"
    )
    run = client.get_run(version.run_id)
    assert publication.promotion_role == "static"
    assert repeated.registered_version == publication.registered_version == "1"
    assert not repeated.alias_updated
    assert version.tags["tripml.artifact_sha256"] == report.static_model_sha256
    assert run.data.tags["tripml.model_role"] == "static_candidate"
    assert run.data.metrics["mae_seconds"] == report.static_candidate_metrics.mae_seconds
    manifest = Path(client.download_artifacts(version.run_id, "evidence/manifest.json"))
    assert TrainingRunReport.model_validate_json(manifest.read_bytes()) == report
    assert production_reference(client, settings).metrics == report.static_candidate_metrics


def test_static_rejection_does_not_register_streaming_winner(tmp_path: Path) -> None:
    settings = _static_settings(tmp_path)
    report = _report(tmp_path, "4444444444444444", PromotionOutcome.REJECT, role="static")
    # The streaming metrics can be better without authorizing a different deployment role.
    report = report.model_copy(update={"streaming_candidate_metrics": _metrics(60)})
    Path(report.artifact_directory, "manifest.json").write_text(report.model_dump_json())
    result = publish_training_report(report, settings)
    assert result.registered_version is None
    assert result.production_alias_version is None


def test_static_upgrade_compares_incumbent_and_old_retry_cannot_roll_back(tmp_path: Path) -> None:
    settings = _static_settings(tmp_path)
    client = create_client(settings)
    first = _report(tmp_path, "4444444444444444", PromotionOutcome.PROMOTE, role="static")
    publish_training_report(first, settings, client=client)
    better = _report(
        tmp_path,
        "5555555555555555",
        PromotionOutcome.PROMOTE,
        role="static",
        production=first,
        mae=70,
    )
    assert publish_training_report(better, settings, client=client).production_alias_version == "2"
    retry = publish_training_report(first, settings, client=client)
    assert retry.production_alias_version == "2"
    assert not retry.alias_updated


def test_stale_static_report_cannot_replace_an_uncompared_incumbent(tmp_path: Path) -> None:
    settings = _static_settings(tmp_path)
    first = _report(tmp_path, "4444444444444444", PromotionOutcome.PROMOTE, role="static")
    publish_training_report(first, settings)
    stale = _report(tmp_path, "5555555555555555", PromotionOutcome.PROMOTE, role="static", mae=60)
    with pytest.raises(TrackingError, match="alias changed"):
        publish_training_report(stale, settings)
    assert production_reference(create_client(settings), settings).version == "1"


def test_different_holdout_cannot_authorize_static_upgrade(tmp_path: Path) -> None:
    settings = _static_settings(tmp_path)
    first = _report(tmp_path, "4444444444444444", PromotionOutcome.PROMOTE, role="static")
    publish_training_report(first, settings)
    different = _report(
        tmp_path,
        "5555555555555555",
        PromotionOutcome.PROMOTE,
        role="static",
        production=first,
        mae=60,
        holdout_sha256="c" * 64,
    )
    with pytest.raises(TrackingError, match="same holdout"):
        publish_training_report(different, settings)


def test_model_roles_cannot_share_registry_name(tmp_path: Path) -> None:
    streaming_settings = _settings(tmp_path)
    first = _report(tmp_path, "4444444444444444", PromotionOutcome.PROMOTE)
    publish_training_report(first, streaming_settings)
    settings = _static_settings(tmp_path)
    static = _report(tmp_path, "5555555555555555", PromotionOutcome.PROMOTE, role="static")
    with pytest.raises(TrackingError, match="different model role"):
        publish_training_report(static, settings)
    with pytest.raises(TrackingError, match="role differs"):
        production_reference(create_client(settings), settings)


@pytest.mark.parametrize("failure", ["role", "decision", "artifact", "manifest"])
def test_inconsistent_publication_fails_before_logging(tmp_path: Path, failure: str) -> None:
    settings = _static_settings(tmp_path)
    report = _report(tmp_path, "4444444444444444", PromotionOutcome.PROMOTE, role="static")
    if failure == "role":
        settings = _settings(tmp_path)
    elif failure == "decision":
        report = report.model_copy(update={"static_candidate_metrics": _metrics(1)})
    elif failure == "artifact":
        Path(report.static_model_path).write_text("tampered")
    else:
        report = report.model_copy(update={"holdout_rows": 101})
    client = create_client(settings)
    with pytest.raises((TrackingError, TrainingError)):
        publish_training_report(report, settings, client=client)
    assert client.get_experiment_by_name(settings.tracking.experiment_name) is None


def test_workflow_supplies_static_incumbent_metrics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _static_settings(tmp_path)
    first = _report(tmp_path, "4444444444444444", PromotionOutcome.PROMOTE, role="static")
    publish_training_report(first, settings)
    better = _report(
        tmp_path,
        "5555555555555555",
        PromotionOutcome.PROMOTE,
        role="static",
        production=first,
        mae=70,
    )

    def train(
        active_settings: PlatformSettings,
        *,
        production_version: str,
        production_metrics: ModelMetrics,
    ) -> TrainingRunReport:
        assert active_settings.tracking.candidate_role == "static"
        assert production_version == "1"
        assert production_metrics == first.static_candidate_metrics
        return better

    monkeypatch.setattr("tripml.tracking.train_models", train)
    result = run_training_workflow(settings)
    assert result.tracking.production_alias_version == "2"
