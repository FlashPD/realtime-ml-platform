from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tripml.contracts import OnlineZoneWindowFeatures

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
