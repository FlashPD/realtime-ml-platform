"""Point-in-time-correct offline feature materialization with dbt-duckdb."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from importlib.resources import files
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import yaml
from pydantic import BaseModel, ConfigDict, Field

from tripml.contracts import AwareDateTime
from tripml.ingestion import TAXI_KIND, YearMonth
from tripml.settings import PlatformSettings

MODEL_VERSION = "gold-features-v1"
HASH_CHUNK_SIZE = 1024 * 1024


class FeatureBuildError(RuntimeError):
    """The feature build could not produce a tested gold artifact."""


class FeatureSourceError(FeatureBuildError):
    """Required accepted silver input is missing."""


class FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class FeatureInput(FrozenModel):
    path: str
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    row_count: int = Field(ge=0)


class FeatureBuildReport(FrozenModel):
    month: str
    model_version: str
    output_path: str
    manifest_path: str
    row_count: int = Field(ge=0)
    output_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    output_size_bytes: int = Field(gt=8)
    input_rows: int = Field(ge=0)
    inputs: tuple[FeatureInput, ...]
    short_window_seconds: int = Field(gt=0)
    long_window_seconds: int = Field(gt=0)
    config_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    built_at: AwareDateTime


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(HASH_CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def _previous_month(month: YearMonth) -> YearMonth:
    if month.month == 1:
        return YearMonth(month.year - 1, 12)
    return YearMonth(month.year, month.month - 1)


def _silver_path(data_root: Path, month: YearMonth) -> Path:
    return data_root / "silver" / TAXI_KIND / f"month={month}" / "trips.parquet"


def _gold_paths(data_root: Path, month: YearMonth) -> tuple[Path, Path]:
    directory = data_root / "gold" / TAXI_KIND / f"month={month}"
    return directory / "training_features.parquet", directory / "manifest.json"


def _feature_inputs(data_root: Path, month: YearMonth) -> tuple[FeatureInput, ...]:
    target = _silver_path(data_root, month)
    if not target.is_file():
        raise FeatureSourceError(f"accepted silver partition does not exist: {target}")

    candidates = (_silver_path(data_root, _previous_month(month)), target)
    return tuple(
        FeatureInput(
            path=str(path.resolve()),
            sha256=_sha256(path),
            row_count=pq.ParquetFile(path).metadata.num_rows,
        )
        for path in candidates
        if path.is_file()
    )


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, delete=False
        ) as temporary:
            temporary_path = Path(temporary.name)
            json.dump(payload, temporary, indent=2, sort_keys=True)
            temporary.write("\n")
            temporary.flush()
            os.fsync(temporary.fileno())
        temporary_path.replace(path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _write_dbt_profile(directory: Path, database_path: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    profile = {
        "tripml": {
            "target": "local",
            "outputs": {
                "local": {
                    "type": "duckdb",
                    "path": str(database_path),
                    "schema": "tripml",
                    "threads": 4,
                }
            },
        }
    }
    (directory / "profiles.yml").write_text(
        yaml.safe_dump(profile, sort_keys=False), encoding="utf-8"
    )


def _invoke_dbt(arguments: list[str]) -> None:
    try:
        from dbt.cli.main import dbtRunner
    except ImportError as error:
        raise FeatureBuildError(
            "dbt-duckdb is required; install the 'transformation' project extra"
        ) from error

    result = dbtRunner().invoke(arguments)
    if not result.success:
        detail = str(result.exception) if result.exception is not None else "dbt build failed"
        raise FeatureBuildError(detail)


def build_gold_features(
    month_value: str,
    *,
    settings: PlatformSettings,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> FeatureBuildReport:
    """Build and atomically publish one tested monthly gold feature partition."""

    month = YearMonth.parse(month_value)
    inputs = _feature_inputs(settings.ingestion.data_root, month)
    output_path, manifest_path = _gold_paths(settings.ingestion.data_root, month)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    project_dir = Path(str(files("tripml").joinpath("dbt")))
    streaming = settings.streaming

    with tempfile.TemporaryDirectory(prefix="tripml-dbt-") as temporary_directory:
        temporary_root = Path(temporary_directory)
        temporary_output = output_path.parent / f".{output_path.name}.{os.getpid()}.tmp"
        variables = {
            "silver_paths": [item.path for item in inputs],
            "gold_path": str(temporary_output.resolve()),
            "month_start": month.start.isoformat(sep=" "),
            "month_end": month.end.isoformat(sep=" "),
            "short_window_seconds": streaming.short_window_seconds,
            "long_window_seconds": streaming.long_window_seconds,
            "model_version": MODEL_VERSION,
        }
        profiles_dir = temporary_root / "profiles"
        _write_dbt_profile(profiles_dir, temporary_root / "tripml.duckdb")
        try:
            _invoke_dbt(
                [
                    "--quiet",
                    "build",
                    "--project-dir",
                    str(project_dir),
                    "--profiles-dir",
                    str(profiles_dir),
                    "--target-path",
                    str(temporary_root / "target"),
                    "--log-path",
                    str(temporary_root / "logs"),
                    "--vars",
                    json.dumps(variables, separators=(",", ":")),
                    "--select",
                    "+training_features",
                ]
            )
            if not temporary_output.is_file():
                raise FeatureBuildError("dbt succeeded without producing the gold Parquet file")
            row_count = pq.ParquetFile(temporary_output).metadata.num_rows
            temporary_output.replace(output_path)
        finally:
            temporary_output.unlink(missing_ok=True)

    report = FeatureBuildReport(
        month=str(month),
        model_version=MODEL_VERSION,
        output_path=str(output_path),
        manifest_path=str(manifest_path),
        row_count=row_count,
        output_sha256=_sha256(output_path),
        output_size_bytes=output_path.stat().st_size,
        input_rows=sum(item.row_count for item in inputs),
        inputs=inputs,
        short_window_seconds=streaming.short_window_seconds,
        long_window_seconds=streaming.long_window_seconds,
        config_fingerprint=settings.fingerprint,
        built_at=now(),
    )
    _write_json_atomic(manifest_path, report.model_dump(mode="json"))
    return report
