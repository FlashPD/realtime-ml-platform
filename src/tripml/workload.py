"""Reproducible, bounded-memory serving workloads from accepted TLC silver data."""

from __future__ import annotations

import hashlib
import json
import platform
import random
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from tripml.contracts import ETARequest
from tripml.ingestion import PartitionPaths, PartitionQualityReport, PartitionStatus, YearMonth
from tripml.settings import PlatformSettings

NEW_YORK = ZoneInfo("America/New_York")
COLUMNS = (
    "trip_id",
    "pickup_datetime",
    "pickup_zone_id",
    "dropoff_zone_id",
    "trip_distance_miles",
    "passenger_count",
    "contract_version",
)
DIMENSIONS = ("pickup_hour_of_week", "pickup_zone", "dropoff_zone", "distance", "passengers")
Profile = dict[str, Counter[str]]


def _sha256(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def _pickup_time(value: datetime) -> datetime | None:
    if value.tzinfo is not None:
        raise ValueError("silver pickup_datetime must be a naive New York timestamp")
    local = value.replace(tzinfo=NEW_YORK)
    # TLC wall-clock timestamps cannot identify an autumn fold or a spring gap.
    # Exclude both explicitly instead of guessing an instant for a serving request.
    if local.utcoffset() != local.replace(fold=1).utcoffset():
        return None
    return local


def _observe(profile: Profile, request: ETARequest) -> None:
    pickup = request.pickup_time
    distance = request.trip_distance_miles
    bucket = (
        "[0,1)"
        if distance < 1
        else "[1,3)"
        if distance < 3
        else "[3,10)"
        if distance < 10
        else "[10,inf)"
    )
    values = (
        str(pickup.weekday() * 24 + pickup.hour),
        str(request.pickup_zone_id),
        str(request.dropoff_zone_id),
        bucket,
        str(request.passenger_count),
    )
    for dimension, value in zip(DIMENSIONS, values, strict=True):
        profile[dimension][value] += 1


def build_workload(
    month_value: str,
    *,
    settings: PlatformSettings,
    output: Path,
    rows: int = 10_000,
    seed: int = 42,
) -> dict[str, Any]:
    """Sample uniformly without replacement; never read a full partition into memory."""
    if not 1 <= rows <= 100_000:
        raise ValueError("rows must be between 1 and 100,000")
    if output.exists():
        raise FileExistsError(f"workload output already exists: {output}")
    month = YearMonth.parse(month_value)
    paths = PartitionPaths.from_root(settings.ingestion.data_root, month)
    quality_bytes = paths.quality_report.read_bytes()
    quality = PartitionQualityReport.model_validate_json(quality_bytes)
    if (
        quality.status is not PartitionStatus.ACCEPTED
        or quality.month != str(month)
        or quality.silver_path is None
        or Path(quality.silver_path).resolve() != paths.silver_file.resolve()
    ):
        raise ValueError("workload requires a matching accepted silver quality report")
    source_sha256 = _sha256(paths.silver_file)
    population: Profile = {name: Counter() for name in DIMENSIONS}
    sample: list[ETARequest] = []
    rng = random.Random(seed)
    eligible = excluded = scanned = 0
    with pq.ParquetFile(paths.silver_file) as source:
        if source.metadata.num_rows != quality.valid_rows:
            raise ValueError("silver row count does not match the accepted quality report")
        if not set(COLUMNS) <= set(source.schema_arrow.names):
            raise ValueError("silver is missing required workload columns")
        for batch in source.iter_batches(batch_size=settings.ingestion.batch_size, columns=COLUMNS):
            for row in batch.to_pylist():
                scanned += 1
                if row.pop("contract_version") != "1.0":
                    raise ValueError("unsupported silver contract version")
                pickup = row.pop("pickup_datetime")
                if not isinstance(pickup, datetime):
                    raise ValueError("silver pickup timestamp must be a datetime")
                aware = _pickup_time(pickup)
                if not month.start <= pickup < month.end:
                    raise ValueError("silver pickup timestamp must be within the requested month")
                if aware is None:
                    excluded += 1
                    continue
                request = ETARequest(pickup_time=aware, **row)
                _observe(population, request)
                eligible += 1
                if len(sample) < rows:
                    sample.append(request)
                else:
                    position = rng.randrange(eligible)
                    if position < rows:
                        sample[position] = request
    if eligible < rows:
        raise ValueError(f"requested {rows} rows but only {eligible} eligible trips exist")
    if (
        _sha256(paths.silver_file) != source_sha256
        or paths.quality_report.read_bytes() != quality_bytes
    ):
        raise ValueError("workload source changed during sampling; retry with stable inputs")
    # Shuffle even when every eligible trip was selected; source order is often chronological.
    rng.shuffle(sample)
    sampled: Profile = {name: Counter() for name in DIMENSIONS}
    for request in sample:
        _observe(sampled, request)
    fixture = "".join(request.model_dump_json() + "\n" for request in sample).encode("utf-8")
    report = {
        "schema_version": "1.0",
        "built_at": datetime.now(UTC).isoformat(),
        "month": str(month),
        "sampling": {"algorithm": "reservoir-v1", "seed": seed, "rows": rows},
        "python": platform.python_version(),
        "config_fingerprint": settings.fingerprint,
        "source": {
            "silver_path": str(paths.silver_file.resolve()),
            "silver_sha256": source_sha256,
            "quality_report_sha256": hashlib.sha256(quality_bytes).hexdigest(),
            "bronze_sha256": quality.source_sha256,
            "scanned_rows": scanned,
            "eligible_rows": eligible,
            "excluded_ambiguous_or_nonexistent_local_times": excluded,
        },
        "timezone": "America/New_York",
        "population_profile": population,
        "sample_profile": sampled,
        "total_variation_distance": {
            name: 0.5
            * sum(
                abs(population[name][key] / eligible - sampled[name][key] / rows)
                for key in population[name].keys() | sampled[name].keys()
            )
            for name in DIMENSIONS
        },
        "artifacts_sha256": {"requests.jsonl": hashlib.sha256(fixture).hexdigest()},
    }
    # Complete validation before reserving a new evidence directory. Write the manifest last;
    # interrupted writes cannot look like a completed evidence bundle.
    output.mkdir(parents=True, exist_ok=False)
    (output / "requests.jsonl").write_bytes(fixture)
    (output / "quality-report.json").write_bytes(quality_bytes)
    (output / "README.md").write_text(
        "# TLC serving workload\n\n"
        f"Sampled {rows:,} of {eligible:,} eligible {month} trips with seed {seed}. "
        f"Excluded {excluded:,} ambiguous or nonexistent New York pickup times.\n\n"
        "[Request fixture](requests.jsonl), [manifest and distribution comparison](manifest.json), "
        "[ingestion quality report](quality-report.json).\n\n"
        "Sampling is uniform without replacement, using bounded Arrow batches and a reservoir. "
        "The same input bytes, seed, and Python version reproduce the request fixture. "
        "Histogram total variation distance compares sample and eligible-population marginals "
        "(0 means identical; 1 means disjoint). It is descriptive, not a performance gate.\n\n"
        "Pickup timestamps retain historical New York offsets. No duration or fare labels "
        "are included. The HTTP benchmark replaces trip IDs and cycles this shuffled fixture "
        "at its configured arrival rate. This is a sampled request mix, not event-time replay. "
        "It does not establish model accuracy, online feature freshness, or serving latency. "
        "Online measurements also require matching event-time Redis features.\n\n"
        "The quality report records ingestion acceptance and bronze provenance; it does not "
        "contain a historical silver checksum. This manifest fingerprints the silver file "
        "observed during sampling, not proof that it was never altered after ingestion.\n",
        encoding="utf-8",
    )
    (output / "manifest.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return report
