"""Opt-in Redis protocol integration; use a dedicated, disposable Redis database."""

from __future__ import annotations

import os

import pytest
from pydantic import SecretStr

from tripml.contracts import ETARequest, OnlineZoneWindowFeatures
from tripml.online_features import LookupOutcome, RedisFeatureStore, feature_key
from tripml.settings import ServingSettings

pytestmark = pytest.mark.integration


def test_real_redis_round_trip_and_expiry(
    online_snapshots: tuple[OnlineZoneWindowFeatures, ...],
) -> None:
    url = os.getenv("TRIPML_TEST_REDIS_URL")
    if url is None:
        pytest.skip("TRIPML_TEST_REDIS_URL is not configured")
    settings = ServingSettings(redis_url=SecretStr(url), redis_timeout_seconds=0.2)
    store = RedisFeatureStore.from_settings(settings)
    keys = [
        feature_key(item.zone_role, item.zone_id, item.window_kind) for item in online_snapshots
    ]
    request = ETARequest(
        trip_id="redis-integration",
        pickup_zone_id=161,
        dropoff_zone_id=236,
        pickup_time=online_snapshots[0].window_end,
        trip_distance_miles=3.2,
        passenger_count=2,
    )
    try:
        with store.client.pipeline(transaction=True) as pipeline:
            for key, snapshot in zip(keys, online_snapshots, strict=True):
                pipeline.set(key, snapshot.model_dump_json(), ex=60)
            pipeline.execute()
        assert store.lookup(request).outcome is LookupOutcome.FRESH
        assert 0 < store.client.ttl(keys[0]) <= 60
        store.client.pexpire(keys[0], 0)
        assert store.lookup(request).outcome is LookupOutcome.MISSING
        store.client.set(keys[0], "malformed", ex=60)
        assert store.lookup(request).outcome is LookupOutcome.INVALID
    finally:
        store.client.delete(*keys)
        store.close()
