"""Deterministic model training, evaluation, and promotion gating."""

from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path
from time import perf_counter_ns
from typing import Self

import lightgbm as lgb
import numpy as np
import numpy.typing as npt
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, Field

from tripml.contracts import (
    AwareDateTime,
    GateRuleResult,
    ModelMetrics,
    PromotionDecision,
    PromotionOutcome,
)
from tripml.ingestion import TAXI_KIND, YearMonth
from tripml.settings import PlatformSettings, PromotionGateSettings

STATIC_FEATURES = (
    "pickup_zone_id",
    "dropoff_zone_id",
    "pickup_hour_of_week",
    "trip_distance_miles",
    "passenger_count",
)
STREAMING_FEATURES = (
    "pu_zone_trips_15m",
    "pu_zone_mean_speed_15m",
    "pu_zone_mean_duration_60m",
    "do_zone_trips_60m",
)
TARGET = "actual_duration_seconds"
MODEL_VERSION_COLUMN = "feature_model_version"
HASH_CHUNK_SIZE = 1024 * 1024
LATENCY_SAMPLE_SIZE = 500
DISTANCE_BUCKET_EDGES = (0.0, 2.0, 5.0, 10.0, float("inf"))

FloatArray = npt.NDArray[np.float64]
IntArray = npt.NDArray[np.int64]


class TrainingError(RuntimeError):
    """Base error for a training run that cannot produce trustworthy artifacts."""


class TrainingDataError(TrainingError):
    """Gold inputs are missing, inconsistent, or too small."""


class FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class TrainingInput(FrozenModel):
    month: str
    path: str
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    row_count: int = Field(gt=0)
    feature_model_version: str


class TrainingRunReport(FrozenModel):
    run_id: str = Field(pattern=r"^[0-9a-f]{16}$")
    train_months: tuple[str, ...]
    holdout_month: str
    train_rows: int = Field(gt=0)
    holdout_rows: int = Field(gt=0)
    inputs: tuple[TrainingInput, ...]
    static_features: tuple[str, ...]
    streaming_features: tuple[str, ...]
    baseline_metrics: ModelMetrics
    static_candidate_metrics: ModelMetrics
    streaming_candidate_metrics: ModelMetrics
    static_max_bucket_calibration_error_pct: float = Field(ge=0)
    streaming_max_bucket_calibration_error_pct: float = Field(ge=0)
    promotion_decision: PromotionDecision
    artifact_directory: str
    baseline_path: str
    static_model_path: str
    streaming_model_path: str
    model_card_path: str
    baseline_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    static_model_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    streaming_model_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    model_card_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    config_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    trained_at: AwareDateTime


@dataclass(frozen=True, slots=True)
class Dataset:
    table: pa.Table
    target: FloatArray
    distance: FloatArray

    def matrix(self, features: Sequence[str]) -> FloatArray:
        columns = [
            np.asarray(
                self.table.column(name).combine_chunks().to_numpy(zero_copy_only=False),
                dtype=np.float64,
            )
            for name in features
        ]
        return np.column_stack(columns)


@dataclass(frozen=True, slots=True)
class MedianLookup:
    keys: IntArray
    values: FloatArray

    @classmethod
    def fit(cls, keys: IntArray, target: FloatArray) -> Self:
        order = np.argsort(keys, kind="stable")
        sorted_keys = keys[order]
        sorted_target = target[order]
        starts = np.concatenate(
            (np.array([0], dtype=np.int64), np.flatnonzero(np.diff(sorted_keys)) + 1)
        )
        ends = np.concatenate((starts[1:], np.array([len(sorted_keys)], dtype=np.int64)))
        medians = np.fromiter(
            (np.median(sorted_target[start:end]) for start, end in zip(starts, ends, strict=True)),
            dtype=np.float64,
            count=len(starts),
        )
        return cls(keys=sorted_keys[starts], values=medians)

    def predict(self, keys: IntArray, fallback: FloatArray | float) -> FloatArray:
        positions = np.searchsorted(self.keys, keys)
        bounded = np.minimum(positions, len(self.keys) - 1)
        found = (positions < len(self.keys)) & (self.keys[bounded] == keys)
        output = np.asarray(fallback, dtype=np.float64).copy()
        if output.ndim == 0:
            output = np.full(len(keys), float(output), dtype=np.float64)
        output[found] = self.values[bounded[found]]
        return output


@dataclass(frozen=True, slots=True)
class HierarchicalMedianBaseline:
    exact: MedianLookup
    pickup_hour: MedianLookup
    global_median: float

    @staticmethod
    def _exact_keys(matrix: FloatArray) -> IntArray:
        pickup = matrix[:, 0].astype(np.int64)
        dropoff = matrix[:, 1].astype(np.int64)
        hour = matrix[:, 2].astype(np.int64)
        return ((pickup * 266 + dropoff) * 168 + hour).astype(np.int64)

    @staticmethod
    def _pickup_hour_keys(matrix: FloatArray) -> IntArray:
        pickup = matrix[:, 0].astype(np.int64)
        hour = matrix[:, 2].astype(np.int64)
        return (pickup * 168 + hour).astype(np.int64)

    @classmethod
    def fit(cls, static_matrix: FloatArray, target: FloatArray) -> Self:
        return cls(
            exact=MedianLookup.fit(cls._exact_keys(static_matrix), target),
            pickup_hour=MedianLookup.fit(cls._pickup_hour_keys(static_matrix), target),
            global_median=float(np.median(target)),
        )

    def predict(self, static_matrix: FloatArray) -> FloatArray:
        fallback = self.pickup_hour.predict(
            self._pickup_hour_keys(static_matrix), self.global_median
        )
        return self.exact.predict(self._exact_keys(static_matrix), fallback)

    def as_document(self) -> dict[str, object]:
        return {
            "kind": "hierarchical_median",
            "levels": [
                "pickup_zone_id+dropoff_zone_id+pickup_hour_of_week",
                "pickup_zone_id+pickup_hour_of_week",
                "global",
            ],
            "exact_keys": self.exact.keys.tolist(),
            "exact_medians": self.exact.values.tolist(),
            "pickup_hour_keys": self.pickup_hour.keys.tolist(),
            "pickup_hour_medians": self.pickup_hour.values.tolist(),
            "global_median": self.global_median,
        }


def calculate_metrics(
    actual: FloatArray, predicted: FloatArray, inference_p95_ms: float
) -> ModelMetrics:
    errors = predicted - actual
    return ModelMetrics(
        mae_seconds=float(np.mean(np.abs(errors))),
        rmse_seconds=float(np.sqrt(np.mean(np.square(errors)))),
        mape_pct=float(np.mean(np.abs(errors) / actual) * 100),
        inference_p95_ms=inference_p95_ms,
    )


def max_bucket_calibration_error_pct(
    actual: FloatArray, predicted: FloatArray, distance: FloatArray
) -> float:
    errors: list[float] = []
    for lower, upper in pairwise(DISTANCE_BUCKET_EDGES):
        selected = (distance >= lower) & (distance < upper)
        if not np.any(selected):
            continue
        actual_mean = float(np.mean(actual[selected]))
        predicted_mean = float(np.mean(predicted[selected]))
        errors.append(abs(predicted_mean - actual_mean) / actual_mean * 100)
    return max(errors, default=0.0)


def evaluate_promotion(
    *,
    candidate_version: str,
    candidate_metrics: ModelMetrics,
    baseline_metrics: ModelMetrics,
    max_calibration_error_pct: float,
    settings: PromotionGateSettings,
    decided_at: datetime,
    production_version: str | None = None,
    production_metrics: ModelMetrics | None = None,
) -> PromotionDecision:
    baseline_improvement = _mae_improvement_pct(
        candidate_metrics.mae_seconds, baseline_metrics.mae_seconds
    )
    gates = [
        GateRuleResult(
            rule="mae_improvement_vs_baseline_pct",
            observed=baseline_improvement,
            threshold=settings.min_mae_improvement_vs_baseline_pct,
            passed=baseline_improvement >= settings.min_mae_improvement_vs_baseline_pct,
        ),
        GateRuleResult(
            rule="max_distance_bucket_calibration_error_pct",
            observed=max_calibration_error_pct,
            threshold=settings.max_bucket_calibration_error_pct,
            passed=max_calibration_error_pct <= settings.max_bucket_calibration_error_pct,
        ),
        GateRuleResult(
            rule="inference_p95_ms",
            observed=candidate_metrics.inference_p95_ms,
            threshold=settings.max_inference_p95_ms,
            passed=candidate_metrics.inference_p95_ms <= settings.max_inference_p95_ms,
        ),
    ]
    if production_metrics is not None:
        production_improvement = _mae_improvement_pct(
            candidate_metrics.mae_seconds, production_metrics.mae_seconds
        )
        gates.append(
            GateRuleResult(
                rule="mae_improvement_vs_production_pct",
                observed=production_improvement,
                threshold=settings.min_mae_improvement_vs_production_pct,
                passed=production_improvement >= settings.min_mae_improvement_vs_production_pct,
            )
        )
    outcome = (
        PromotionOutcome.PROMOTE if all(gate.passed for gate in gates) else PromotionOutcome.REJECT
    )
    return PromotionDecision(
        candidate_version=candidate_version,
        production_version=production_version,
        candidate_metrics=candidate_metrics,
        baseline_metrics=baseline_metrics,
        production_metrics=production_metrics,
        gate_results=tuple(gates),
        outcome=outcome,
        decided_at=decided_at,
    )


def _mae_improvement_pct(candidate_mae: float, reference_mae: float) -> float:
    if reference_mae == 0:
        return 0.0 if candidate_mae == 0 else -100.0
    return (reference_mae - candidate_mae) / reference_mae * 100


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(HASH_CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def _gold_path(data_root: Path, month: YearMonth) -> Path:
    return data_root / "gold" / TAXI_KIND / f"month={month}" / "training_features.parquet"


def _load_input(data_root: Path, month_value: str) -> tuple[TrainingInput, pa.Table]:
    month = YearMonth.parse(month_value)
    path = _gold_path(data_root, month)
    if not path.is_file():
        raise TrainingDataError(f"gold feature partition does not exist: {path}")
    required = {*STATIC_FEATURES, *STREAMING_FEATURES, TARGET, MODEL_VERSION_COLUMN}
    parquet = pq.ParquetFile(path)
    missing = sorted(required - set(parquet.schema_arrow.names))
    if missing:
        raise TrainingDataError(f"gold partition is missing columns: {', '.join(missing)}")
    table = parquet.read(columns=sorted(required))
    versions = pc.unique(table.column(MODEL_VERSION_COLUMN).combine_chunks()).to_pylist()
    if len(versions) != 1 or versions[0] is None:
        raise TrainingDataError(f"gold partition has inconsistent feature model versions: {path}")
    item = TrainingInput(
        month=str(month),
        path=str(path.resolve()),
        sha256=_sha256(path),
        row_count=table.num_rows,
        feature_model_version=str(versions[0]),
    )
    return item, table


def _dataset(tables: Sequence[pa.Table]) -> Dataset:
    table = pa.concat_tables(tables)
    target = np.asarray(
        table.column(TARGET).combine_chunks().to_numpy(zero_copy_only=False), dtype=np.float64
    )
    distance = np.asarray(
        table.column("trip_distance_miles").combine_chunks().to_numpy(zero_copy_only=False),
        dtype=np.float64,
    )
    return Dataset(table=table, target=target, distance=distance)


def _train_booster(
    matrix: FloatArray,
    target: FloatArray,
    *,
    feature_names: Sequence[str],
    settings: Mapping[str, object],
    rounds: int,
) -> lgb.Booster:
    categorical_names = {"pickup_zone_id", "dropoff_zone_id", "pickup_hour_of_week"}
    categorical = [index for index, name in enumerate(feature_names) if name in categorical_names]
    training_data = lgb.Dataset(
        matrix,
        label=target,
        feature_name=list(feature_names),
        categorical_feature=categorical,
        free_raw_data=True,
    )
    return lgb.train(dict(settings), training_data, num_boost_round=rounds)


def _prediction_latency_p95_ms(booster: lgb.Booster, matrix: FloatArray) -> float:
    sample_size = min(len(matrix), LATENCY_SAMPLE_SIZE)
    indices = np.linspace(0, len(matrix) - 1, sample_size, dtype=np.int64)
    booster.predict(matrix[indices[:1]])
    durations = np.empty(sample_size, dtype=np.float64)
    for output_index, row_index in enumerate(indices):
        started = perf_counter_ns()
        booster.predict(matrix[row_index : row_index + 1])
        durations[output_index] = (perf_counter_ns() - started) / 1_000_000
    return float(np.percentile(durations, 95))


def _baseline_latency_p95_ms(baseline: HierarchicalMedianBaseline, matrix: FloatArray) -> float:
    sample_size = min(len(matrix), LATENCY_SAMPLE_SIZE)
    indices = np.linspace(0, len(matrix) - 1, sample_size, dtype=np.int64)
    baseline.predict(matrix[indices[:1]])
    durations = np.empty(sample_size, dtype=np.float64)
    for output_index, row_index in enumerate(indices):
        started = perf_counter_ns()
        baseline.predict(matrix[row_index : row_index + 1])
        durations[output_index] = (perf_counter_ns() - started) / 1_000_000
    return float(np.percentile(durations, 95))


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _model_card(report: TrainingRunReport) -> str:
    decision = report.promotion_decision
    metric_rows = "".join(
        _metric_row(label, metrics)
        for label, metrics in (
            ("Hierarchical median", report.baseline_metrics),
            ("Static LightGBM", report.static_candidate_metrics),
            ("Streaming-feature LightGBM", report.streaming_candidate_metrics),
        )
    )
    gate_rows = "".join(
        (
            f"| {gate.rule} | {gate.observed:.3f} | {gate.threshold:.3f} | "
            f"{'yes' if gate.passed else 'no'} |\n"
        )
        for gate in decision.gate_results
    )
    return f"""# Trip-duration model card: {report.run_id}

## Intended use

Estimate yellow-taxi trip duration at pickup time for this portfolio platform. It is not suitable
for pricing, employment, enforcement, or decisions about individual passengers or drivers.

## Data

- Training months: {", ".join(report.train_months)} ({report.train_rows:,} rows)
- Holdout month: {report.holdout_month} ({report.holdout_rows:,} rows)
- Feature model: {report.inputs[0].feature_model_version}

## Holdout results

| Model | MAE (s) | RMSE (s) | MAPE (%) | Inference P95 (ms) |
|---|---:|---:|---:|---:|
{metric_rows}

## Promotion decision

**{decision.outcome.value.upper()}** `{decision.candidate_version}`.

| Gate | Observed | Threshold | Passed |
|---|---:|---:|:---:|
{gate_rows}

## Limitations

The data represents completed medallion-taxi trips and local wall-clock timestamps. Performance can
shift across seasons, policy changes, vehicle types, and unusual demand. Online feature parity and
live error monitoring are separate acceptance gates before serving claims are made.
"""


def _metric_row(label: str, metrics: ModelMetrics) -> str:
    return (
        f"| {label} | {metrics.mae_seconds:.2f} | {metrics.rmse_seconds:.2f} | "
        f"{metrics.mape_pct:.2f} | {metrics.inference_p95_ms:.3f} |\n"
    )


def _run_identifier(
    inputs: Sequence[TrainingInput],
    settings: PlatformSettings,
    production_version: str | None,
    production_metrics: ModelMetrics | None,
) -> str:
    payload = {
        "inputs": [item.model_dump(mode="json", exclude={"path"}) for item in inputs],
        "training": settings.training.model_dump(mode="json", exclude={"artifact_root"}),
        "promotion_gate": settings.promotion_gate.model_dump(mode="json"),
        "static_features": STATIC_FEATURES,
        "streaming_features": STREAMING_FEATURES,
        "production_version": production_version,
        "production_metrics": (
            production_metrics.model_dump(mode="json") if production_metrics is not None else None
        ),
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def _load_verified_report(manifest: Path, destination: Path) -> TrainingRunReport:
    report = TrainingRunReport.model_validate_json(manifest.read_text(encoding="utf-8"))
    if Path(report.artifact_directory) != destination:
        raise TrainingError(f"artifact manifest points outside its run directory: {manifest}")
    artifacts = (
        (Path(report.baseline_path), report.baseline_sha256),
        (Path(report.static_model_path), report.static_model_sha256),
        (Path(report.streaming_model_path), report.streaming_model_sha256),
        (Path(report.model_card_path), report.model_card_sha256),
    )
    for path, expected_sha256 in artifacts:
        if path.parent != destination or not path.is_file() or _sha256(path) != expected_sha256:
            raise TrainingError(f"artifact failed manifest verification: {path}")
    return report


def train_models(
    settings: PlatformSettings,
    *,
    production_version: str | None = None,
    production_metrics: ModelMetrics | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> TrainingRunReport:
    """Train, evaluate, gate, and atomically publish a reproducible model bundle."""

    input_tables = [
        _load_input(settings.ingestion.data_root, month)
        for month in (*settings.training.train_months, settings.training.holdout_month)
    ]
    inputs = tuple(item for item, _table in input_tables)
    versions = {item.feature_model_version for item in inputs}
    if len(versions) != 1:
        raise TrainingDataError("training and holdout partitions use different feature models")
    training_count = len(settings.training.train_months)
    train = _dataset([table for _item, table in input_tables[:training_count]])
    holdout = _dataset([input_tables[-1][1]])
    if len(train.target) < settings.training.min_training_rows:
        raise TrainingDataError(
            f"training data has {len(train.target)} rows; "
            f"requires {settings.training.min_training_rows}"
        )
    if len(holdout.target) < settings.training.min_holdout_rows:
        raise TrainingDataError(
            f"holdout data has {len(holdout.target)} rows; "
            f"requires {settings.training.min_holdout_rows}"
        )

    if (production_version is None) != (production_metrics is None):
        raise ValueError("production_version and production_metrics must be supplied together")
    run_id = _run_identifier(inputs, settings, production_version, production_metrics)
    artifact_root = settings.training.artifact_root.resolve()
    destination = artifact_root / run_id
    existing_manifest = destination / "manifest.json"
    if existing_manifest.is_file():
        return _load_verified_report(existing_manifest, destination)
    if destination.exists():
        raise TrainingError(f"incomplete artifact directory already exists: {destination}")

    static_train = train.matrix(STATIC_FEATURES)
    static_holdout = holdout.matrix(STATIC_FEATURES)
    full_features = (*STATIC_FEATURES, *STREAMING_FEATURES)
    full_train = train.matrix(full_features)
    full_holdout = holdout.matrix(full_features)
    baseline = HierarchicalMedianBaseline.fit(static_train, train.target)
    baseline_predictions = baseline.predict(static_holdout)

    model = settings.training.model
    parameters: dict[str, object] = {
        "objective": model.objective,
        "metric": "mae",
        "num_leaves": model.num_leaves,
        "learning_rate": model.learning_rate,
        "seed": model.seed,
        "feature_fraction_seed": model.seed,
        "bagging_seed": model.seed,
        "data_random_seed": model.seed,
        "deterministic": True,
        "force_col_wise": True,
        "verbosity": -1,
        "num_threads": 1,
    }
    static_booster = _train_booster(
        static_train,
        train.target,
        feature_names=STATIC_FEATURES,
        settings=parameters,
        rounds=model.n_estimators,
    )
    streaming_booster = _train_booster(
        full_train,
        train.target,
        feature_names=full_features,
        settings=parameters,
        rounds=model.n_estimators,
    )
    static_predictions = np.asarray(static_booster.predict(static_holdout), dtype=np.float64)
    streaming_predictions = np.asarray(streaming_booster.predict(full_holdout), dtype=np.float64)
    baseline_metrics = calculate_metrics(
        holdout.target,
        baseline_predictions,
        _baseline_latency_p95_ms(baseline, static_holdout),
    )
    static_metrics = calculate_metrics(
        holdout.target,
        static_predictions,
        _prediction_latency_p95_ms(static_booster, static_holdout),
    )
    streaming_metrics = calculate_metrics(
        holdout.target,
        streaming_predictions,
        _prediction_latency_p95_ms(streaming_booster, full_holdout),
    )
    static_calibration = max_bucket_calibration_error_pct(
        holdout.target, static_predictions, holdout.distance
    )
    streaming_calibration = max_bucket_calibration_error_pct(
        holdout.target, streaming_predictions, holdout.distance
    )
    trained_at = now()
    decision = evaluate_promotion(
        candidate_version=f"{run_id}-streaming",
        candidate_metrics=streaming_metrics,
        baseline_metrics=baseline_metrics,
        max_calibration_error_pct=streaming_calibration,
        settings=settings.promotion_gate,
        decided_at=trained_at,
        production_version=production_version,
        production_metrics=production_metrics,
    )

    artifact_root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{run_id}.", dir=artifact_root))
    final_paths = {
        "baseline": destination / "baseline.json",
        "static": destination / "static-model.txt",
        "streaming": destination / "streaming-model.txt",
        "card": destination / "model-card.md",
    }
    try:
        _write_json(temporary / "baseline.json", baseline.as_document())
        static_booster.save_model(temporary / "static-model.txt")
        streaming_booster.save_model(temporary / "streaming-model.txt")
        report = TrainingRunReport(
            run_id=run_id,
            train_months=settings.training.train_months,
            holdout_month=settings.training.holdout_month,
            train_rows=len(train.target),
            holdout_rows=len(holdout.target),
            inputs=inputs,
            static_features=STATIC_FEATURES,
            streaming_features=STREAMING_FEATURES,
            baseline_metrics=baseline_metrics,
            static_candidate_metrics=static_metrics,
            streaming_candidate_metrics=streaming_metrics,
            static_max_bucket_calibration_error_pct=static_calibration,
            streaming_max_bucket_calibration_error_pct=streaming_calibration,
            promotion_decision=decision,
            artifact_directory=str(destination),
            baseline_path=str(final_paths["baseline"]),
            static_model_path=str(final_paths["static"]),
            streaming_model_path=str(final_paths["streaming"]),
            model_card_path=str(final_paths["card"]),
            baseline_sha256="0" * 64,
            static_model_sha256="0" * 64,
            streaming_model_sha256="0" * 64,
            model_card_sha256="0" * 64,
            config_fingerprint=settings.fingerprint,
            trained_at=trained_at,
        )
        (temporary / "model-card.md").write_text(_model_card(report), encoding="utf-8")
        report = TrainingRunReport.model_validate(
            report.model_dump()
            | {
                "baseline_sha256": _sha256(temporary / "baseline.json"),
                "static_model_sha256": _sha256(temporary / "static-model.txt"),
                "streaming_model_sha256": _sha256(temporary / "streaming-model.txt"),
                "model_card_sha256": _sha256(temporary / "model-card.md"),
            }
        )
        _write_json(temporary / "evaluation.json", report.model_dump(mode="json"))
        _write_json(temporary / "manifest.json", report.model_dump(mode="json"))
        temporary.replace(destination)
        return report
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
