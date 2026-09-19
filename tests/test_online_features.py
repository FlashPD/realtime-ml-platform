from __future__ import annotations

import json
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr, ValidationError
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from tripml.contracts import ETARequest, OnlineZoneWindowFeatures
from tripml.online_features import LookupOutcome, RedisFeatureStore, feature_key
from tripml.settings import ServingSettings


def test_nullable_feature_keys_and_snapshots_cannot_mix_with_legacy_population(
    online_snapshots: tuple[OnlineZoneWindowFeatures, ...],
) -> None:
    client = MagicMock(spec=Redis)
    client.mget.return_value = [item.model_dump_json() for item in online_snapshots]
    store = RedisFeatureStore(client, ServingSettings())
    store.feature_model_version = "gold-features-v2"
    assert store.lookup(_request(online_snapshots)).outcome is LookupOutcome.INVALID
    assert all(key.startswith("tripml:features:v2:") for key in client.mget.call_args.args[0])
    client.mget.return_value = [
        item.model_copy(update={"feature_model_version": "gold-features-v2"}).model_dump_json()
        for item in online_snapshots
    ]
    assert store.lookup(_request(online_snapshots)).outcome is LookupOutcome.FRESH
    store.feature_model_version = "gold-features-v1"
    assert store.lookup(_request(online_snapshots)).outcome is LookupOutcome.INVALID


def _request(snapshots: tuple[OnlineZoneWindowFeatures, ...], offset: float = 0) -> ETARequest:
    return ETARequest(
        trip_id="test-trip",
        pickup_zone_id=161,
        dropoff_zone_id=236,
        pickup_time=snapshots[0].window_end + timedelta(seconds=offset),
        trip_distance_miles=3.2,
        passenger_count=2,
    )


def test_lookup_reads_three_role_specific_keys_and_records_exclusive_cutoff(
    online_snapshots: tuple[OnlineZoneWindowFeatures, ...],
) -> None:
    client = MagicMock(spec=Redis)
    client.mget.return_value = [item.model_dump_json().encode() for item in online_snapshots]
    result = RedisFeatureStore(client, ServingSettings()).lookup(_request(online_snapshots, 30))
    client.mget.assert_called_once_with(
        [
            "tripml:features:v1:pickup:zone:161:15m",
            "tripml:features:v1:pickup:zone:161:60m",
            "tripml:features:v1:dropoff:zone:236:60m",
        ]
    )
    assert result.outcome is LookupOutcome.FRESH
    assert result.features is not None
    assert result.features.values == {
        "pu_zone_trips_15m": 5,
        "pu_zone_mean_speed_15m": 12,
        "pu_zone_mean_duration_60m": 600,
        "do_zone_trips_60m": 7,
    }
    assert result.features.age_seconds == 30
    assert set(result.features.timestamps.values()) == {online_snapshots[0].window_end}


@pytest.mark.parametrize(
    ("offset", "expected"),
    [
        (-0.001, LookupOutcome.FUTURE),
        (0, LookupOutcome.FRESH),
        (600, LookupOutcome.FRESH),
        (600.001, LookupOutcome.STALE),
    ],
)
def test_freshness_uses_pickup_event_time_and_exact_boundaries(
    online_snapshots: tuple[OnlineZoneWindowFeatures, ...],
    offset: float,
    expected: LookupOutcome,
) -> None:
    client = MagicMock(spec=Redis)
    client.mget.return_value = [item.model_dump_json() for item in online_snapshots]
    result = RedisFeatureStore(client, ServingSettings()).lookup(_request(online_snapshots, offset))
    assert result.outcome is expected
    assert (result.features is not None) == (expected is LookupOutcome.FRESH)


@pytest.mark.parametrize("error", [RedisTimeoutError("timeout"), RedisConnectionError("offline")])
def test_network_failure_selects_fallback(
    online_snapshots: tuple[OnlineZoneWindowFeatures, ...],
    error: Exception,
) -> None:
    client = MagicMock(spec=Redis)
    client.mget.side_effect = error
    result = RedisFeatureStore(client, ServingSettings()).lookup(_request(online_snapshots))
    assert result.outcome is LookupOutcome.UNAVAILABLE
    assert result.features is None
    client.mget.assert_called_once()


@pytest.mark.parametrize("bad_payload", [None, b"not-json", b"\xff", b"{}"])
def test_absent_and_malformed_data_select_fallback(
    online_snapshots: tuple[OnlineZoneWindowFeatures, ...],
    bad_payload: bytes | None,
) -> None:
    client = MagicMock(spec=Redis)
    client.mget.return_value = [
        bad_payload,
        *[item.model_dump_json() for item in online_snapshots[1:]],
    ]
    result = RedisFeatureStore(client, ServingSettings()).lookup(_request(online_snapshots))
    expected = LookupOutcome.MISSING if bad_payload is None else LookupOutcome.INVALID
    assert result.outcome is expected
    assert result.features is None


@pytest.mark.parametrize(
    "changes",
    [
        {"zone_id": 162},
        {"zone_role": "dropoff"},
        {"feature_model_version": "v2"},
        {"schema_version": "2.0"},
        {"mean_speed_mph": float("inf")},
        {"trip_count": 11},
    ],
)
def test_mismatched_or_invalid_snapshot_selects_fallback(
    online_snapshots: tuple[OnlineZoneWindowFeatures, ...],
    changes: dict[str, object],
) -> None:
    documents = [item.model_dump(mode="json") for item in online_snapshots]
    documents[0].update(changes)
    client = MagicMock(spec=Redis)
    client.mget.return_value = [json.dumps(item) for item in documents]
    result = RedisFeatureStore(client, ServingSettings()).lookup(_request(online_snapshots))
    assert result.outcome is LookupOutcome.INVALID


def test_mixed_window_cutoffs_are_not_combined(
    online_snapshots: tuple[OnlineZoneWindowFeatures, ...],
) -> None:
    documents = [item.model_dump() for item in online_snapshots]
    for key in ("window_start", "window_end", "computed_at", "source_max_event_time"):
        documents[2][key] -= timedelta(seconds=1)
    client = MagicMock(spec=Redis)
    client.mget.return_value = [
        OnlineZoneWindowFeatures.model_validate(item).model_dump_json() for item in documents
    ]
    result = RedisFeatureStore(client, ServingSettings()).lookup(_request(online_snapshots))
    assert result.outcome is LookupOutcome.INCONSISTENT


@pytest.mark.parametrize("index", [0, 2])
def test_empty_pickup_uses_static_but_empty_dropoff_is_a_valid_zero_count(
    online_snapshots: tuple[OnlineZoneWindowFeatures, ...],
    index: int,
) -> None:
    documents = [item.model_dump() for item in online_snapshots]
    documents[index].update(
        trip_count=0,
        source_max_event_time=None,
        mean_speed_mph=0,
        mean_duration_seconds=0,
        mean_distance_miles=0,
    )
    client = MagicMock(spec=Redis)
    client.mget.return_value = [
        OnlineZoneWindowFeatures.model_validate(item).model_dump_json() for item in documents
    ]
    result = RedisFeatureStore(client, ServingSettings()).lookup(_request(online_snapshots))
    assert result.outcome is (LookupOutcome.EMPTY if index == 0 else LookupOutcome.FRESH)


@pytest.mark.parametrize("violation", ["duration", "future_source", "missing_source", "empty"])
def test_contract_rejects_invalid_window_provenance(
    online_snapshots: tuple[OnlineZoneWindowFeatures, ...],
    violation: str,
) -> None:
    document = online_snapshots[0].model_dump()
    if violation == "duration":
        document["window_start"] += timedelta(seconds=1)
    elif violation == "future_source":
        document["source_max_event_time"] = document["window_end"]
    elif violation == "missing_source":
        document["source_max_event_time"] = None
    else:
        document["trip_count"] = 0
    with pytest.raises(ValidationError):
        OnlineZoneWindowFeatures.model_validate(document)


def test_connection_options_bound_waits_disable_retries_and_release_pool() -> None:
    settings = ServingSettings(redis_url=SecretStr("redis://localhost:6379/0"))
    with patch("tripml.online_features.Redis.from_url") as factory:
        store = RedisFeatureStore.from_settings(settings)
        options = factory.call_args.kwargs
        assert options["socket_timeout"] == 0.01
        assert options["socket_connect_timeout"] == 0.01
        assert options["max_connections"] == 32
        assert options["retry"].get_retries() == 0
        store.close()
        factory.return_value.close.assert_called_once()
    with pytest.raises(ValueError, match="redis_url is required"):
        RedisFeatureStore.from_settings(ServingSettings())


def test_malformed_read_shape_is_rejected(
    online_snapshots: tuple[OnlineZoneWindowFeatures, ...],
) -> None:
    client = MagicMock(spec=Redis)
    client.mget.return_value = []
    result = RedisFeatureStore(client, ServingSettings()).lookup(_request(online_snapshots))
    assert result.outcome is LookupOutcome.INVALID


def test_key_includes_role_to_avoid_pickup_dropoff_collisions(
    online_snapshots: tuple[OnlineZoneWindowFeatures, ...],
) -> None:
    snapshot = online_snapshots[0]
    assert feature_key("pickup", 161, snapshot.window_kind) != feature_key(
        "dropoff", 161, snapshot.window_kind
    )
