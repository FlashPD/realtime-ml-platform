"""Typed platform configuration with YAML and environment overrides."""

from __future__ import annotations

import hashlib
import json
from importlib.resources import files
from pathlib import Path
from typing import Any, Literal, Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, PositiveFloat, PositiveInt, model_validator
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
