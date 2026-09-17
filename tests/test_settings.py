from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

from tripml.settings import (
    PlatformSettings,
    PublicationSettings,
    ServingSettings,
    load_settings,
    settings_as_dict,
)


def test_packaged_configuration_is_valid_and_stable() -> None:
    first = load_settings()
    second = load_settings()

    assert first.training.holdout_month == "2024-04"
    assert first.training.artifact_root == Path("artifacts/training")
    assert first.tracking.production_alias == "production"
    assert first.streaming.short_window_seconds == 900
    assert first.ingestion.max_partition_violation_rate == 0.1
    assert first.fingerprint == second.fingerprint
    assert len(first.fingerprint) == 64


def test_environment_has_priority_over_yaml(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRIPML_SERVING__P95_LATENCY_OBJECTIVE_MS", "75")

    settings = load_settings()

    assert settings.serving.p95_latency_objective_ms == 75


def test_lineage_database_url_is_excluded_from_output_and_fingerprint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    without_secret = load_settings()
    monkeypatch.setenv(
        "TRIPML_LINEAGE__DATABASE_URL",
        "postgresql://tripml:do-not-print@postgresql:5432/tripml",
    )

    with_secret = load_settings()
    public_settings = settings_as_dict(with_secret)

    assert with_secret.lineage.database_url is not None
    assert with_secret.fingerprint == without_secret.fingerprint
    assert "database_url" not in public_settings["lineage"]
    assert "do-not-print" not in str(public_settings)


@pytest.mark.parametrize(
    "payload",
    [
        "streaming:\n  short_window_seconds: 3600\n  long_window_seconds: 900\n",
        "training:\n  train_months: ['2024-04']\n  holdout_month: '2024-04'\n",
        "unknown_section: true\n",
    ],
)
def test_invalid_configuration_fails_loudly(tmp_path: Path, payload: str) -> None:
    config = tmp_path / "invalid.yaml"
    config.write_text(payload, encoding="utf-8")

    with pytest.raises(ValidationError):
        load_settings(config)


def test_non_mapping_yaml_is_rejected(tmp_path: Path) -> None:
    config = tmp_path / "invalid.yaml"
    config.write_text("- not\n- a\n- mapping\n", encoding="utf-8")

    with pytest.raises(ValueError, match="YAML mapping"):
        load_settings(config)


def test_settings_are_immutable() -> None:
    settings = PlatformSettings()
    with pytest.raises(ValidationError):
        settings.serving.p95_latency_objective_ms = 100


def test_redis_credentials_are_excluded_from_output_and_fingerprint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = load_settings()
    monkeypatch.setenv("TRIPML_SERVING__REDIS_URL", "redis://:do-not-print@localhost:6379/0")
    configured = load_settings()
    assert configured.serving.redis_url is not None
    assert "redis_url" not in settings_as_dict(configured)["serving"]
    assert configured.fingerprint == original.fingerprint


@pytest.mark.parametrize(
    "url", ["http://localhost", "redis:///0", "redis://localhost?socket_timeout=20"]
)
def test_redis_url_cannot_override_timeout_policy(url: str) -> None:
    with pytest.raises(ValidationError):
        ServingSettings(redis_url=SecretStr(url))


@pytest.mark.parametrize(
    "options",
    [
        {"topic": ".."},
        {"topic": "has space"},
        {"topic": "é"},
        {"bootstrap_servers": " "},
        {"ack_timeout_seconds": 0.5},
        {"delivery_timeout_ms": 0},
        {"queue_max_messages": 0},
    ],
)
def test_invalid_publication_configuration_is_rejected(options: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        PublicationSettings.model_validate(options)
