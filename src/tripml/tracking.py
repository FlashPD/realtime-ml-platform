"""MLflow experiment logging and guarded model-registry promotion."""

from __future__ import annotations

import json
from enum import StrEnum
from pathlib import Path
from time import time

from mlflow import MlflowClient
from mlflow.entities import Experiment, Run
from mlflow.exceptions import MlflowException
from pydantic import BaseModel, ConfigDict

from tripml.contracts import ModelMetrics, PromotionOutcome
from tripml.settings import PlatformSettings
from tripml.training import TrainingRunReport, _load_verified_report, train_models

RESOURCE_DOES_NOT_EXIST = "RESOURCE_DOES_NOT_EXIST"
RESOURCE_ALREADY_EXISTS = "RESOURCE_ALREADY_EXISTS"
INVALID_PARAMETER_VALUE = "INVALID_PARAMETER_VALUE"
MISSING_ALIAS_ERROR_CODES = {RESOURCE_DOES_NOT_EXIST, INVALID_PARAMETER_VALUE}


class TrackingError(RuntimeError):
    """MLflow state is incomplete or inconsistent with the local model bundle."""


class ModelRole(StrEnum):
    BASELINE = "baseline"
    STATIC = "static_candidate"
    STREAMING = "streaming_candidate"


class FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ProductionReference(FrozenModel):
    version: str
    metrics: ModelMetrics
    holdout_month: str | None = None
    holdout_sha256: str | None = None


class TrackedModelRun(FrozenModel):
    role: ModelRole
    run_id: str
    artifact_sha256: str


class TrackingPublication(FrozenModel):
    promotion_role: str = "streaming"
    experiment_id: str
    runs: tuple[TrackedModelRun, ...]
    registered_model_name: str
    registered_version: str | None
    production_alias: str
    production_alias_version: str | None
    alias_updated: bool


class TrainingWorkflowReport(FrozenModel):
    training: TrainingRunReport
    tracking: TrackingPublication


def _prepare_local_stores(settings: PlatformSettings) -> str | None:
    tracking = settings.tracking
    if tracking.tracking_uri.startswith("sqlite:///"):
        database_path = Path(tracking.tracking_uri.removeprefix("sqlite:///"))
        database_path.parent.mkdir(parents=True, exist_ok=True)
        tracking.local_artifact_root.mkdir(parents=True, exist_ok=True)
        return tracking.local_artifact_root.resolve().as_uri()
    return None


def create_client(settings: PlatformSettings) -> MlflowClient:
    """Create a client after preparing directories required by the local SQLite profile."""

    _prepare_local_stores(settings)
    return MlflowClient(
        tracking_uri=settings.tracking.tracking_uri,
        registry_uri=settings.tracking.tracking_uri,
    )


def _experiment(client: MlflowClient, settings: PlatformSettings) -> Experiment:
    name = settings.tracking.experiment_name
    existing = client.get_experiment_by_name(name)
    if existing is not None:
        return existing
    try:
        experiment_id = client.create_experiment(
            name,
            artifact_location=_prepare_local_stores(settings),
            tags={"tripml.purpose": "trip-duration-training"},
        )
    except MlflowException as error:
        if error.error_code != RESOURCE_ALREADY_EXISTS:
            raise
        raced = client.get_experiment_by_name(name)
        if raced is None:
            raise TrackingError(
                f"experiment was concurrently created but is unavailable: {name}"
            ) from error
        return raced
    return client.get_experiment(experiment_id)


def _artifact_for_role(report: TrainingRunReport, role: ModelRole) -> tuple[Path, str]:
    if role is ModelRole.BASELINE:
        return Path(report.baseline_path), report.baseline_sha256
    if role is ModelRole.STATIC:
        return Path(report.static_model_path), report.static_model_sha256
    return Path(report.streaming_model_path), report.streaming_model_sha256


def _metrics_for_role(report: TrainingRunReport, role: ModelRole) -> ModelMetrics:
    if role is ModelRole.BASELINE:
        return report.baseline_metrics
    if role is ModelRole.STATIC:
        return report.static_candidate_metrics
    return report.streaming_candidate_metrics


def _existing_run(
    client: MlflowClient,
    experiment_id: str,
    report: TrainingRunReport,
    role: ModelRole,
    artifact_sha256: str,
) -> Run | None:
    matches = client.search_runs(
        [experiment_id],
        filter_string=(
            f"tags.`tripml.bundle_run_id` = '{report.run_id}' AND "
            f"tags.`tripml.model_role` = '{role.value}'"
        ),
        max_results=1,
    )
    if not matches:
        return None
    run = matches[0]
    if run.data.tags.get("tripml.artifact_sha256") != artifact_sha256:
        raise TrackingError(f"MLflow run artifact does not match bundle: {run.info.run_id}")
    if run.info.status != "FINISHED":
        raise TrackingError(f"MLflow run is not complete: {run.info.run_id}")
    return run


def _log_model_run(
    client: MlflowClient,
    experiment_id: str,
    settings: PlatformSettings,
    report: TrainingRunReport,
    role: ModelRole,
) -> TrackedModelRun:
    artifact, artifact_sha256 = _artifact_for_role(report, role)
    existing = _existing_run(client, experiment_id, report, role, artifact_sha256)
    if existing is not None:
        return TrackedModelRun(
            role=role, run_id=existing.info.run_id, artifact_sha256=artifact_sha256
        )

    metrics = _metrics_for_role(report, role)
    run = client.create_run(
        experiment_id,
        run_name=f"{report.run_id}-{role.value}",
        tags={
            "tripml.bundle_run_id": report.run_id,
            "tripml.model_role": role.value,
            "tripml.artifact_sha256": artifact_sha256,
            "tripml.config_fingerprint": report.config_fingerprint,
            "tripml.promotion_outcome": (
                report.promotion_decision.outcome.value
                if role.value == f"{report.promotion_role}_candidate"
                else "not_selected"
            ),
            "tripml.promotion_role": report.promotion_role,
            "tripml.holdout_sha256": next(
                (item.sha256 for item in report.inputs if item.month == report.holdout_month), ""
            ),
        },
    )
    run_id = run.info.run_id
    try:
        parameters: dict[str, str] = {
            "train_months": ",".join(report.train_months),
            "holdout_month": report.holdout_month,
            "train_rows": str(report.train_rows),
            "holdout_rows": str(report.holdout_rows),
            "static_features": json.dumps(report.static_features),
            "streaming_features": json.dumps(report.streaming_features),
            "model_role": role.value,
            "seed": str(settings.training.model.seed),
        }
        for key, value in parameters.items():
            client.log_param(run_id, key, value)
        metric_values = metrics.model_dump(mode="json")
        if role is ModelRole.STATIC:
            metric_values["max_bucket_calibration_error_pct"] = (
                report.static_max_bucket_calibration_error_pct
            )
        if role is ModelRole.STREAMING:
            metric_values["max_bucket_calibration_error_pct"] = (
                report.streaming_max_bucket_calibration_error_pct
            )
            static_mae = report.static_candidate_metrics.mae_seconds
            metric_values["mae_improvement_vs_static_pct"] = (
                (static_mae - metrics.mae_seconds) / static_mae * 100 if static_mae else 0.0
            )
        timestamp_ms = int(time() * 1000)
        for key, value in metric_values.items():
            client.log_metric(run_id, key, float(value), timestamp=timestamp_ms, step=0)
        client.log_artifact(run_id, str(artifact), artifact_path="model")
        if role in {ModelRole.STATIC, ModelRole.STREAMING}:
            evidence_directory = Path(report.artifact_directory)
            for name in ("manifest.json", "evaluation.json", "model-card.md"):
                client.log_artifact(
                    run_id, str(evidence_directory / name), artifact_path="evidence"
                )
        client.set_terminated(run_id, status="FINISHED")
    except Exception:
        client.set_terminated(run_id, status="FAILED")
        raise
    return TrackedModelRun(role=role, run_id=run_id, artifact_sha256=artifact_sha256)


def _registered_model(client: MlflowClient, name: str) -> None:
    try:
        client.get_registered_model(name)
    except MlflowException as error:
        if error.error_code != RESOURCE_DOES_NOT_EXIST:
            raise
        try:
            client.create_registered_model(
                name,
                tags={"tripml.target": "trip_duration_seconds"},
                description="Guarded NYC taxi trip-duration model",
            )
        except MlflowException as create_error:
            if create_error.error_code != RESOURCE_ALREADY_EXISTS:
                raise


def _alias_version(client: MlflowClient, name: str, alias: str) -> str | None:
    try:
        return str(client.get_model_version_by_alias(name, alias).version)
    except MlflowException as error:
        if error.error_code in MISSING_ALIAS_ERROR_CODES:
            return None
        raise


def production_reference(
    client: MlflowClient, settings: PlatformSettings
) -> ProductionReference | None:
    """Return metrics attached to the current production alias, when one exists."""

    name = settings.tracking.registered_model_name
    alias = settings.tracking.production_alias
    try:
        version = client.get_model_version_by_alias(name, alias)
    except MlflowException as error:
        if error.error_code in MISSING_ALIAS_ERROR_CODES:
            return None
        raise
    if version.run_id is None:
        raise TrackingError(f"production model version has no source run: {version.version}")
    run = client.get_run(version.run_id)
    expected_role = f"{settings.tracking.candidate_role}_candidate"
    if run.info.status != "FINISHED" or run.data.tags.get("tripml.model_role") != expected_role:
        raise TrackingError(
            "production model role differs from candidate; use a separate registry name"
        )
    metrics = run.data.metrics
    required = ("mae_seconds", "rmse_seconds", "mape_pct", "inference_p95_ms")
    missing = [key for key in required if key not in metrics]
    if missing:
        raise TrackingError(f"production run is missing metrics: {', '.join(missing)}")
    return ProductionReference(
        version=str(version.version),
        metrics=ModelMetrics(**{key: metrics[key] for key in required}),
        holdout_month=run.data.params.get("holdout_month"),
        holdout_sha256=run.data.tags.get("tripml.holdout_sha256"),
    )


def publish_training_report(
    report: TrainingRunReport,
    settings: PlatformSettings,
    *,
    client: MlflowClient | None = None,
) -> TrackingPublication:
    """Log all model paths and move the production alias only after a passing decision."""

    active_client = client or create_client(settings)
    if report.promotion_role != settings.tracking.candidate_role:
        raise TrackingError("configured candidate role disagrees with training evidence")
    selected_role = ModelRole(f"{report.promotion_role}_candidate")
    decision = report.promotion_decision
    if (
        decision.candidate_version != f"{report.run_id}-{report.promotion_role}"
        or decision.candidate_metrics != _metrics_for_role(report, selected_role)
        or decision.baseline_metrics != report.baseline_metrics
        or (
            decision.outcome is PromotionOutcome.PROMOTE
            and (
                not decision.gate_results or not all(gate.passed for gate in decision.gate_results)
            )
        )
    ):
        raise TrackingError("selected model disagrees with promotion evidence")
    directory = Path(report.artifact_directory)
    if _load_verified_report(directory / "manifest.json", directory) != report:
        raise TrackingError("report disagrees with the artifact manifest")
    experiment = _experiment(active_client, settings)
    runs = tuple(
        _log_model_run(active_client, experiment.experiment_id, settings, report, role)
        for role in ModelRole
    )
    selected_run = next(item for item in runs if item.role is selected_role)
    model_name = settings.tracking.registered_model_name
    alias = settings.tracking.production_alias
    registered_version: str | None = None
    alias_updated = False

    if decision.outcome is PromotionOutcome.PROMOTE:
        _registered_model(active_client, model_name)
        existing_versions = active_client.search_model_versions(f"name = '{model_name}'")
        for version in existing_versions:
            if (
                not version.run_id
                or active_client.get_run(version.run_id).data.tags.get("tripml.model_role")
                != selected_role.value
            ):
                raise TrackingError("registry contains a different model role; use a separate name")
        existing = next(
            (
                version
                for version in existing_versions
                if version.tags.get("tripml.bundle_run_id") == report.run_id
                and version.run_id == selected_run.run_id
            ),
            None,
        )
        if existing is None:
            incumbent = production_reference(active_client, settings)
            incumbent_version = incumbent.version if incumbent is not None else None
            if decision.production_version != incumbent_version:
                raise TrackingError(
                    "production alias changed since evaluation; retrain before publishing"
                )
            if incumbent is not None and decision.production_metrics != incumbent.metrics:
                raise TrackingError("incumbent metrics disagree with promotion evidence")
            if incumbent is not None:
                _validate_holdout(report, incumbent)
            existing = active_client.create_model_version(
                name=model_name,
                source=f"runs:/{selected_run.run_id}/model",
                run_id=selected_run.run_id,
                tags={
                    "tripml.bundle_run_id": report.run_id,
                    "tripml.artifact_sha256": selected_run.artifact_sha256,
                    "tripml.model_role": selected_role.value,
                },
                description=(
                    f"{report.promotion_role.capitalize()} candidate "
                    "that passed every promotion gate"
                ),
            )
        if existing.tags.get("tripml.artifact_sha256") != selected_run.artifact_sha256:
            raise TrackingError("registered artifact disagrees with the selected model")
        registered_version = str(existing.version)
        current_alias = _alias_version(active_client, model_name, alias)
        can_advance = current_alias is None or int(registered_version) >= int(current_alias)
        if can_advance and current_alias != registered_version:
            if current_alias != decision.production_version:
                raise TrackingError(
                    "production alias changed since evaluation; retrain before publishing"
                )
            active_client.set_registered_model_alias(model_name, alias, registered_version)
            alias_updated = True

    alias_version = _alias_version(active_client, model_name, alias)
    return TrackingPublication(
        promotion_role=report.promotion_role,
        experiment_id=experiment.experiment_id,
        runs=runs,
        registered_model_name=model_name,
        registered_version=registered_version,
        production_alias=alias,
        production_alias_version=alias_version,
        alias_updated=alias_updated,
    )


def _validate_holdout(report: TrainingRunReport, production: ProductionReference) -> None:
    holdout_sha256 = next(
        (item.sha256 for item in report.inputs if item.month == report.holdout_month), None
    )
    if (
        production.holdout_month != report.holdout_month
        or not holdout_sha256
        or production.holdout_sha256 != holdout_sha256
    ):
        raise TrackingError("incumbent comparison requires the same holdout month and checksum")


def run_training_workflow(
    settings: PlatformSettings, *, client: MlflowClient | None = None
) -> TrainingWorkflowReport:
    """Train against the current production reference, then track and conditionally register."""

    active_client = client or create_client(settings)
    production = production_reference(active_client, settings)
    report = train_models(
        settings,
        production_version=production.version if production is not None else None,
        production_metrics=production.metrics if production is not None else None,
    )
    if production is not None:
        _validate_holdout(report, production)
    publication = publish_training_report(report, settings, client=active_client)
    return TrainingWorkflowReport(training=report, tracking=publication)
