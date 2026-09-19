"""Typed platform configuration with YAML and environment overrides."""

from __future__ import annotations

import hashlib
import json
from importlib.resources import files
from pathlib import Path
from typing import Any, Literal, Self
from urllib.parse import urlsplit

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PositiveFloat,
    PositiveInt,
    SecretStr,
    field_validator,
    model_validator,
)
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


class ImmutableModel(BaseModel):
    """Strict, immutable base for nested configuration sections."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class ModelSettings(ImmutableModel):
    objective: Literal["regression_l1"] = "regression_l1"
    num_leaves: PositiveInt = 63
    learning_rate: PositiveFloat = 0.05
    n_estimators: PositiveInt = 800
    seed: int = 42


class TrainingSettings(ImmutableModel):
    train_months: tuple[str, ...] = ("2024-01", "2024-02", "2024-03")
    holdout_month: str = "2024-04"
    target: Literal["trip_duration_seconds"] = "trip_duration_seconds"
    artifact_root: Path = Path("artifacts/training")
    min_training_rows: PositiveInt = 1_000
    min_holdout_rows: PositiveInt = 100
    model: ModelSettings = Field(default_factory=ModelSettings)

    @model_validator(mode="after")
    def holdout_is_not_training_data(self) -> Self:
        if not self.train_months:
            raise ValueError("at least one training month is required")
        if self.holdout_month in self.train_months:
            raise ValueError("holdout_month must not appear in train_months")
        return self


class PromotionGateSettings(ImmutableModel):
    min_mae_improvement_vs_baseline_pct: float = Field(default=15.0, ge=0, le=100)
    min_mae_improvement_vs_production_pct: float = Field(default=1.0, ge=0, le=100)
    max_bucket_calibration_error_pct: float = Field(default=10.0, ge=0, le=100)
    max_inference_p95_ms: PositiveFloat = 5.0


class StreamingSettings(ImmutableModel):
    short_window_seconds: PositiveInt = 15 * 60
    long_window_seconds: PositiveInt = 60 * 60
    allowed_lateness_seconds: PositiveInt = 5 * 60
    feature_ttl_seconds: PositiveInt = 2 * 60 * 60

    @model_validator(mode="after")
    def windows_are_consistent(self) -> Self:
        if self.short_window_seconds >= self.long_window_seconds:
            raise ValueError("short_window_seconds must be less than long_window_seconds")
        if self.allowed_lateness_seconds >= self.long_window_seconds:
            raise ValueError("allowed_lateness_seconds must be less than long_window_seconds")
        if self.feature_ttl_seconds <= self.long_window_seconds:
            raise ValueError("feature_ttl_seconds must exceed long_window_seconds")
        return self


class ServingSettings(ImmutableModel):
    feature_staleness_limit_seconds: PositiveInt = 10 * 60
    p95_latency_objective_ms: PositiveFloat = 50.0
    redis_url: SecretStr | None = Field(default=None, exclude=True)
    redis_timeout_seconds: float = Field(default=0.01, gt=0, le=1, allow_inf_nan=False)
    redis_max_connections: PositiveInt = 32

    @field_validator("redis_url")
    @classmethod
    def redis_url_has_no_option_overrides(cls, value: SecretStr | None) -> SecretStr | None:
        if value is not None:
            parsed = urlsplit(value.get_secret_value())
            if parsed.scheme not in {"redis", "rediss"} or not parsed.hostname:
                raise ValueError("redis_url must use redis:// or rediss:// with a hostname")
            if parsed.query or parsed.fragment:
                raise ValueError("redis_url must not override connection options")
        return value


class IngestionSettings(ImmutableModel):
    passenger_count_policy: Literal["required", "allow_unknown"] = "required"
    data_root: Path = Path("data")
    source_base_url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    max_partition_violation_rate: float = Field(default=0.10, ge=0, le=1)
    batch_size: PositiveInt = 65_536
    download_timeout_seconds: PositiveFloat = 120.0


class FeatureSettings(ImmutableModel):
    duckdb_memory_limit: str = Field(default="2GB", pattern=r"^[1-9][0-9]*(MB|GB)$")
    duckdb_max_temp_directory_size: str = Field(default="8GB", pattern=r"^[1-9][0-9]*(MB|GB)$")
    duckdb_threads: int = Field(default=2, ge=1, le=32)


class LineageSettings(ImmutableModel):
    database_url: SecretStr | None = Field(default=None, exclude=True)
    connect_timeout_seconds: PositiveInt = 10
    auto_migrate: bool = True


class TrackingSettings(ImmutableModel):
    candidate_role: Literal["static", "streaming"] = "streaming"
    tracking_uri: str = "sqlite:///artifacts/mlflow/mlflow.db"
    local_artifact_root: Path = Path("artifacts/mlflow/runs")
    experiment_name: str = "tripml-training"
    registered_model_name: str = "tripml-trip-duration"
    production_alias: str = "production"


class PublicationSettings(ImmutableModel):
    bootstrap_servers: str | None = Field(default=None, min_length=1)
    topic: str = Field(default="predictions", min_length=1, max_length=249, pattern=r"^[\w.-]+$")
    delivery_timeout_ms: int = Field(default=1000, ge=1, le=30_000)
    ack_timeout_seconds: float = Field(default=1.5, gt=0, le=30, allow_inf_nan=False)
    shutdown_timeout_seconds: float = Field(default=2, gt=0, le=30, allow_inf_nan=False)
    queue_max_messages: PositiveInt = 10_000

    @model_validator(mode="after")
    def publication_options_are_consistent(self) -> Self:
        if self.topic in {".", ".."} or not self.topic.isascii():
            raise ValueError("topic must be an ASCII Kafka topic name other than . or ..")
        if self.bootstrap_servers is not None and not self.bootstrap_servers.strip():
            raise ValueError("bootstrap_servers must not be blank")
        if self.ack_timeout_seconds < self.delivery_timeout_ms / 1000:
            raise ValueError("ack timeout must be at least the producer delivery timeout")
        return self


class PlatformSettings(BaseSettings):
    """Root settings object. Environment variables take precedence over YAML."""

    model_config = SettingsConfigDict(
        env_prefix="TRIPML_",
        env_nested_delimiter="__",
        extra="forbid",
        frozen=True,
    )

    training: TrainingSettings = Field(default_factory=TrainingSettings)
    promotion_gate: PromotionGateSettings = Field(default_factory=PromotionGateSettings)
    streaming: StreamingSettings = Field(default_factory=StreamingSettings)
    serving: ServingSettings = Field(default_factory=ServingSettings)
    ingestion: IngestionSettings = Field(default_factory=IngestionSettings)
    features: FeatureSettings = Field(default_factory=FeatureSettings)
    lineage: LineageSettings = Field(default_factory=LineageSettings)
    tracking: TrackingSettings = Field(default_factory=TrackingSettings)
    publication: PublicationSettings = Field(default_factory=PublicationSettings)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        del cls, settings_cls
        return env_settings, init_settings, dotenv_settings, file_secret_settings

    @property
    def fingerprint(self) -> str:
        """Return a stable digest suitable for lineage and run metadata."""

        canonical = json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()


def load_settings(config_path: Path | None = None) -> PlatformSettings:
    """Load packaged defaults or a supplied YAML file, then apply environment overrides."""

    if config_path is None:
        config_resource = files("tripml").joinpath("default_config.yaml")
        with config_resource.open(encoding="utf-8") as config_file:
            raw = yaml.safe_load(config_file)
    else:
        with config_path.open(encoding="utf-8") as config_file:
            raw = yaml.safe_load(config_file)

    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError("configuration root must be a YAML mapping")
    return PlatformSettings(**raw)


def settings_as_dict(settings: PlatformSettings) -> dict[str, Any]:
    """Return JSON-compatible settings for logs and CLI output."""

    return settings.model_dump(mode="json")
