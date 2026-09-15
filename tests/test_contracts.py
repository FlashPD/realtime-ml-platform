from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from tripml.contracts import (
    CONTRACTS,
    GateRuleResult,
    ModelMetrics,
    PromotionDecision,
    PromotionOutcome,
    QualityCheckResult,
    TripStarted,
    WindowKind,
    ZoneWindowFeatures,
    contract_schemas,
)

NOW = datetime(2024, 4, 15, 12, tzinfo=UTC)


def test_trip_started_accepts_valid_event() -> None:
    event = TripStarted(
        trip_id="trip-123",
        event_time=NOW,
        pickup_zone_id=161,
        dropoff_zone_id=236,
        trip_distance_miles=3.2,
        passenger_count=2,
    )

    assert event.schema_version == "1.0"
    assert event.pickup_zone_id == 161


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_time", datetime(2024, 4, 15, 12)),
        ("pickup_zone_id", 0),
        ("dropoff_zone_id", 266),
        ("trip_distance_miles", -0.1),
        ("passenger_count", 10),
    ],
)
def test_trip_started_rejects_invalid_values(field: str, value: object) -> None:
    payload: dict[str, object] = {
        "trip_id": "trip-123",
        "event_time": NOW,
        "pickup_zone_id": 161,
        "dropoff_zone_id": 236,
        "trip_distance_miles": 3.2,
        "passenger_count": 2,
    }
    payload[field] = value

    with pytest.raises(ValidationError):
        TripStarted.model_validate(payload)


def test_contracts_reject_unknown_fields_and_mutation() -> None:
    payload = {
        "trip_id": "trip-123",
        "event_time": NOW,
        "pickup_zone_id": 161,
        "dropoff_zone_id": 236,
        "trip_distance_miles": 3.2,
        "passenger_count": 2,
        "surprise": "not allowed",
    }
    with pytest.raises(ValidationError, match="surprise"):
        TripStarted.model_validate(payload)

    payload.pop("surprise")
    event = TripStarted.model_validate(payload)
    with pytest.raises(ValidationError):
        event.pickup_zone_id = 1


def test_window_feature_timestamps_must_be_ordered() -> None:
    with pytest.raises(ValidationError, match="window_start"):
        ZoneWindowFeatures(
            zone_id=161,
            window_kind=WindowKind.SHORT,
            window_start=NOW,
            window_end=NOW,
            trip_count=4,
            mean_duration_seconds=800,
            mean_speed_mph=12,
            mean_distance_miles=2.5,
            computed_at=NOW + timedelta(seconds=1),
        )


@pytest.mark.parametrize(
    ("violations", "total_rows", "threshold", "passed"),
    [(1, 10, 0.2, True), (3, 10, 0.2, False)],
)
def test_quality_result_derives_consistent_decision(
    violations: int, total_rows: int, threshold: float, passed: bool
) -> None:
    result = QualityCheckResult(
        partition="2024-04",
        rule="duration_range",
        violations=violations,
        total_rows=total_rows,
        threshold=threshold,
        passed=passed,
    )
    assert result.passed is passed


def test_quality_result_rejects_inconsistent_decision() -> None:
    with pytest.raises(ValidationError, match="passed must agree"):
        QualityCheckResult(
            partition="2024-04",
            rule="duration_range",
            violations=5,
            total_rows=10,
            threshold=0.2,
            passed=True,
        )


def test_promotion_requires_all_gates_to_pass() -> None:
    metrics = ModelMetrics(
        mae_seconds=200,
        rmse_seconds=300,
        mape_pct=18,
        inference_p95_ms=2,
    )
    gate = GateRuleResult(rule="baseline_mae", observed=20, threshold=15, passed=True)
    decision = PromotionDecision(
        candidate_version="candidate-1",
        candidate_metrics=metrics,
        baseline_metrics=metrics,
        gate_results=(gate,),
        outcome=PromotionOutcome.PROMOTE,
        decided_at=NOW,
    )
    assert decision.outcome is PromotionOutcome.PROMOTE

    with pytest.raises(ValidationError, match="promotion outcome"):
        PromotionDecision.model_validate(
            decision.model_dump() | {"outcome": PromotionOutcome.REJECT}
        )


def test_every_contract_exports_a_strict_json_schema() -> None:
    schemas = contract_schemas()

    assert schemas.keys() == CONTRACTS.keys()
    assert all(schema["additionalProperties"] is False for schema in schemas.values())
    assert schemas["trip-started-v1"]["properties"]["schema_version"]["const"] == "1.0"
