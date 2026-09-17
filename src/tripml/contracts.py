"""Versioned contracts shared by producers, consumers, storage, and serving."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal, Self
from uuid import UUID, uuid4

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveInt,
    model_validator,
)

SchemaVersion = Literal["1.0"]


def _require_aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must include a UTC offset")
    return value


AwareDateTime = Annotated[datetime, AfterValidator(_require_aware)]
TripId = Annotated[str, Field(min_length=1, max_length=128)]
ModelVersion = Annotated[str, Field(min_length=1, max_length=128)]
ZoneId = Annotated[int, Field(ge=1, le=265)]


class ContractModel(BaseModel):
    """Strict, immutable base for messages crossing a system boundary."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class EventModel(ContractModel):
    event_id: UUID = Field(default_factory=uuid4)
    schema_version: SchemaVersion = "1.0"
    event_time: AwareDateTime


class TripStarted(EventModel):
    trip_id: TripId
    pickup_zone_id: ZoneId
    dropoff_zone_id: ZoneId
    trip_distance_miles: NonNegativeFloat
    passenger_count: NonNegativeInt = Field(le=9)


class TripCompleted(EventModel):
    trip_id: TripId
    actual_duration_seconds: PositiveInt = Field(le=3 * 60 * 60)
    fare_amount: NonNegativeFloat


class ETARequest(ContractModel):
    trip_id: TripId
    pickup_zone_id: ZoneId
    dropoff_zone_id: ZoneId
    pickup_time: AwareDateTime
    trip_distance_miles: float = Field(ge=0, allow_inf_nan=False)
    passenger_count: NonNegativeInt = Field(le=9)


FeatureValue = float | int | str | bool


class Prediction(ContractModel):
    prediction_id: UUID = Field(default_factory=uuid4)
    trip_id: TripId
    model_version: ModelVersion
    features_used: dict[str, FeatureValue]
    feature_timestamps: dict[str, AwareDateTime]
    feature_fallback: bool
    estimated_duration_seconds: float = Field(gt=0, allow_inf_nan=False)
    served_at: AwareDateTime
    schema_version: SchemaVersion = "1.0"


class GroundTruth(ContractModel):
    prediction_id: UUID
    actual_duration_seconds: PositiveInt
    absolute_error_seconds: NonNegativeFloat
    joined_at: AwareDateTime
    join_latency_seconds: NonNegativeFloat
    schema_version: SchemaVersion = "1.0"


class WindowKind(StrEnum):
    SHORT = "15m"
    LONG = "60m"


class ZoneWindowFeatures(ContractModel):
    zone_id: ZoneId
    window_kind: WindowKind
    window_start: AwareDateTime
    window_end: AwareDateTime
    trip_count: NonNegativeInt
    mean_duration_seconds: NonNegativeFloat
    mean_speed_mph: NonNegativeFloat
    mean_distance_miles: NonNegativeFloat
    computed_at: AwareDateTime
    schema_version: SchemaVersion = "1.0"

    @model_validator(mode="after")
    def timestamps_are_ordered(self) -> Self:
        if self.window_start >= self.window_end:
            raise ValueError("window_start must be earlier than window_end")
        if self.computed_at < self.window_end:
            raise ValueError("computed_at must not be earlier than window_end")
        return self


class QualityCheckResult(ContractModel):
    partition: Annotated[str, Field(min_length=1)]
    rule: Annotated[str, Field(min_length=1)]
    violations: NonNegativeInt
    total_rows: NonNegativeInt
    threshold: float = Field(ge=0, le=1)
    passed: bool
    contract_version: SchemaVersion = "1.0"

    @model_validator(mode="after")
    def counts_and_decision_are_consistent(self) -> Self:
        if self.violations > self.total_rows:
            raise ValueError("violations cannot exceed total_rows")
        rate = self.violations / self.total_rows if self.total_rows else 0.0
        if self.passed != (rate <= self.threshold):
            raise ValueError("passed must agree with the violation rate and threshold")
        return self


class ModelMetrics(ContractModel):
    mae_seconds: NonNegativeFloat
    rmse_seconds: NonNegativeFloat
    mape_pct: NonNegativeFloat
    inference_p95_ms: NonNegativeFloat


class GateRuleResult(ContractModel):
    rule: Annotated[str, Field(min_length=1)]
    observed: float
    threshold: float
    passed: bool


class PromotionOutcome(StrEnum):
    PROMOTE = "promote"
    REJECT = "reject"


class PromotionDecision(ContractModel):
    candidate_version: ModelVersion
    production_version: ModelVersion | None = None
    candidate_metrics: ModelMetrics
    baseline_metrics: ModelMetrics
    production_metrics: ModelMetrics | None = None
    gate_results: tuple[GateRuleResult, ...]
    outcome: PromotionOutcome
    decided_at: AwareDateTime
    schema_version: SchemaVersion = "1.0"

    @model_validator(mode="after")
    def outcome_matches_gates(self) -> Self:
        all_passed = bool(self.gate_results) and all(result.passed for result in self.gate_results)
        if (self.outcome is PromotionOutcome.PROMOTE) != all_passed:
            raise ValueError("promotion outcome must agree with all gate results")
        return self


class DriftAction(StrEnum):
    NONE = "none"
    ALERT = "alert"
    RETRAIN = "retrain"


class DriftStatistic(ContractModel):
    statistic: float
    threshold: float
    drifted: bool


class DriftReport(ContractModel):
    window_start: AwareDateTime
    window_end: AwareDateTime
    feature_statistics: dict[str, DriftStatistic]
    target_statistic: DriftStatistic | None = None
    action: DriftAction
    created_at: AwareDateTime
    schema_version: SchemaVersion = "1.0"

    @model_validator(mode="after")
    def timestamps_are_ordered(self) -> Self:
        if self.window_start >= self.window_end:
            raise ValueError("window_start must be earlier than window_end")
        return self


CONTRACTS: dict[str, type[BaseModel]] = {
    "eta-request-v1": ETARequest,
    "trip-started-v1": TripStarted,
    "trip-completed-v1": TripCompleted,
    "prediction-v1": Prediction,
    "ground-truth-v1": GroundTruth,
    "zone-window-features-v1": ZoneWindowFeatures,
    "quality-check-result-v1": QualityCheckResult,
    "promotion-decision-v1": PromotionDecision,
    "drift-report-v1": DriftReport,
}


def contract_schemas() -> dict[str, dict[str, Any]]:
    """Build JSON Schemas keyed by their stable registry subject names."""

    return {
        name: model.model_json_schema(mode="serialization") for name, model in CONTRACTS.items()
    }
