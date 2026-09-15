from pathlib import Path

import pytest
from pydantic import ValidationError

from tripml.settings import PlatformSettings, load_settings


def test_packaged_configuration_is_valid_and_stable() -> None:
    first = load_settings()
    second = load_settings()

    assert first.training.holdout_month == "2024-04"
    assert first.streaming.short_window_seconds == 900
    assert first.fingerprint == second.fingerprint
    assert len(first.fingerprint) == 64


def test_environment_has_priority_over_yaml(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRIPML_SERVING__P95_LATENCY_OBJECTIVE_MS", "75")

    settings = load_settings()

    assert settings.serving.p95_latency_objective_ms == 75


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
