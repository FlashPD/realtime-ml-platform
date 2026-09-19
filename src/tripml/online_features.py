"""Versioned Redis reads with event-time validation and explicit degradation reasons."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Literal, cast

from pydantic import ValidationError
from redis import Redis
from redis.backoff import NoBackoff
from redis.exceptions import RedisError
from redis.retry import Retry

from tripml.contracts import ETARequest, OnlineZoneWindowFeatures, WindowKind
from tripml.settings import ServingSettings


class LookupOutcome(StrEnum):
    FRESH = "fresh"
    DISABLED = "disabled"
    UNAVAILABLE = "unavailable"
    MISSING = "missing"
    INVALID = "invalid"
    FUTURE = "future"
    STALE = "stale"
    INCONSISTENT = "inconsistent"
    EMPTY = "empty"


@dataclass(frozen=True)
class OnlineFeatures:
    values: dict[str, float | int]
    timestamps: dict[str, datetime]
    age_seconds: float


@dataclass(frozen=True)
class FeatureLookup:
    outcome: LookupOutcome
    features: OnlineFeatures | None = None


def feature_key(
    role: Literal["pickup", "dropoff"],
    zone_id: int,
    window: WindowKind,
    *,
    feature_model_version: str = "gold-features-v1",
) -> str:
    if feature_model_version not in {"gold-features-v1", "gold-features-v2"}:
        raise ValueError("unsupported online feature model version")
    version = feature_model_version.removeprefix("gold-features-")
    return f"tripml:features:{version}:{role}:zone:{zone_id}:{window.value}"


class RedisFeatureStore:
    feature_model_version: str = "gold-features-v1"

    def __init__(self, client: Redis, settings: ServingSettings) -> None:
        self.client = client
        self.settings = settings

    @classmethod
    def from_settings(cls, settings: ServingSettings) -> RedisFeatureStore:
        if settings.redis_url is None:
            raise ValueError("redis_url is required for the online feature store")
        client = Redis.from_url(
            settings.redis_url.get_secret_value(),
            socket_connect_timeout=settings.redis_timeout_seconds,
            socket_timeout=settings.redis_timeout_seconds,
            max_connections=settings.redis_max_connections,
            retry=Retry(NoBackoff(), 0),
            decode_responses=False,
        )
        return cls(client, settings)

    def close(self) -> None:
        self.client.close()

    def lookup(self, request: ETARequest) -> FeatureLookup:
        identities: tuple[tuple[Literal["pickup", "dropoff"], int, WindowKind], ...] = (
            ("pickup", request.pickup_zone_id, WindowKind.SHORT),
            ("pickup", request.pickup_zone_id, WindowKind.LONG),
            ("dropoff", request.dropoff_zone_id, WindowKind.LONG),
        )
        try:
            # A single command sees one Redis state and avoids three network round trips.
            payloads = cast(
                list[bytes | None],
                self.client.mget(
                    [
                        feature_key(*identity, feature_model_version=self.feature_model_version)
                        for identity in identities
                    ]
                ),
            )
        except RedisError:
            return FeatureLookup(LookupOutcome.UNAVAILABLE)
        if len(payloads) != len(identities):
            return FeatureLookup(LookupOutcome.INVALID)
        if any(payload is None for payload in payloads):
            return FeatureLookup(LookupOutcome.MISSING)
        try:
            snapshots = [
                OnlineZoneWindowFeatures.model_validate_json(cast(bytes, value))
                for value in payloads
            ]
        except (ValidationError, ValueError, TypeError):
            return FeatureLookup(LookupOutcome.INVALID)
        for snapshot, identity in zip(snapshots, identities, strict=True):
            if snapshot.feature_model_version != self.feature_model_version:
                return FeatureLookup(LookupOutcome.INVALID)
            if (snapshot.zone_role, snapshot.zone_id, snapshot.window_kind) != identity:
                return FeatureLookup(LookupOutcome.INVALID)
            age = (request.pickup_time - snapshot.window_end).total_seconds()
            if age < 0:
                return FeatureLookup(LookupOutcome.FUTURE)
            if age > self.settings.feature_staleness_limit_seconds:
                return FeatureLookup(LookupOutcome.STALE)
        if len({item.window_end for item in snapshots}) != 1:
            return FeatureLookup(LookupOutcome.INCONSISTENT)
        short, long, dropoff = snapshots
        if short.trip_count > long.trip_count:
            return FeatureLookup(LookupOutcome.INVALID)
        if short.trip_count == 0 or long.trip_count == 0:
            # Gold has null means in empty windows; never substitute a fabricated zero mean.
            return FeatureLookup(LookupOutcome.EMPTY)
        values = {
            "pu_zone_trips_15m": short.trip_count,
            "pu_zone_mean_speed_15m": short.mean_speed_mph,
            "pu_zone_mean_duration_60m": long.mean_duration_seconds,
            "do_zone_trips_60m": dropoff.trip_count,
        }
        return FeatureLookup(
            LookupOutcome.FRESH,
            OnlineFeatures(
                values=values,
                timestamps=dict.fromkeys(values, short.window_end),
                age_seconds=(request.pickup_time - short.window_end).total_seconds(),
            ),
        )
