from __future__ import annotations

from typing import Any

import pytest
from kind_load import current_cpu_utilization, scaling_checks


def _row(phase: str, desired: int, ready: int, cpu: int | None = 30) -> dict[str, Any]:
    return {
        "phase": phase,
        "desired_replicas": desired,
        "ready_replicas": ready,
        "cpu_utilization": cpu,
    }


@pytest.mark.parametrize("status", [{}, {"currentMetrics": None}, {"currentMetrics": []}])
def test_hpa_metric_initialization_is_not_a_measurement(status: dict[str, Any]) -> None:
    assert current_cpu_utilization({"status": status}) is None


def test_zero_cpu_utilization_is_a_valid_measurement() -> None:
    assert (
        current_cpu_utilization(
            {
                "status": {
                    "currentMetrics": [
                        {"resource": {"name": "cpu", "current": {"averageUtilization": 0}}},
                    ]
                }
            }
        )
        == 0
    )


def test_scaling_requires_ready_replicas_traffic_and_recovery() -> None:
    rows = [_row("baseline", 1, 1), _row("load", 3, 3), _row("cooldown", 1, 1)]
    assert all(scaling_checks(rows, {"pod-a": 1000, "pod-b": 1000}).values())


@pytest.mark.parametrize(
    ("rows", "traffic", "failed"),
    [
        (
            [_row("load", 3, 3), _row("cooldown", 1, 1)],
            {"a": 1, "b": 1},
            "one_ready_replica_before_load",
        ),
        (
            [_row("baseline", 1, 1), _row("load", 3, 1), _row("cooldown", 1, 1)],
            {"a": 1, "b": 1},
            "additional_replicas_ready_under_load",
        ),
        (
            [_row("baseline", 1, 1), _row("load", 3, 3, None), _row("cooldown", 1, 1)],
            {"a": 1, "b": 1},
            "cpu_metrics_available_under_load",
        ),
        (
            [_row("baseline", 1, 1), _row("load", 3, 3)],
            {"a": 1, "b": 1},
            "returned_to_one_replica_after_load",
        ),
        (
            [_row("baseline", 1, 1), _row("load", 3, 3), _row("cooldown", 1, 1)],
            {"a": 1, "b": 0},
            "multiple_pods_served_predictions",
        ),
    ],
)
def test_partial_scaling_evidence_cannot_pass(
    rows: list[dict[str, Any]], traffic: dict[str, float], failed: str
) -> None:
    assert not scaling_checks(rows, traffic)[failed]
