"""Idempotent TLC bronze ingestion and transactional silver quality gates."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from types import TracebackType
from typing import Protocol, Self, cast
from urllib.request import Request, urlopen

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, Field, model_validator

from tripml.contracts import AwareDateTime, QualityCheckResult

TAXI_KIND = "yellow"
CONTRACT_VERSION = "1.0"
PARQUET_MAGIC = b"PAR1"
DOWNLOAD_CHUNK_SIZE = 1024 * 1024

SOURCE_COLUMNS = (
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
)

SILVER_SCHEMA = pa.schema(
    [
        ("trip_id", pa.string()),
        ("pickup_datetime", pa.timestamp("us")),
        ("dropoff_datetime", pa.timestamp("us")),
        ("pickup_zone_id", pa.int16()),
        ("dropoff_zone_id", pa.int16()),
        ("trip_distance_miles", pa.float64()),
        ("passenger_count", pa.int16()),
        ("fare_amount", pa.float64()),
        ("actual_duration_seconds", pa.int32()),
        ("source_row_number", pa.int64()),
        ("contract_version", pa.string()),
    ]
)


class IngestionError(RuntimeError):
    """Base class for errors that should fail an ingestion run loudly."""


class BronzeIntegrityError(IngestionError):
    """The downloaded object or an existing bronze object failed integrity checks."""


class SourceSchemaError(IngestionError):
    """The source partition does not expose the expected TLC columns."""


@dataclass(frozen=True, slots=True)
class YearMonth:
    """A validated calendar month with stable path and URL formatting."""

    year: int
    month: int

    def __post_init__(self) -> None:
        if not 2009 <= self.year <= 2100:
            raise ValueError("year must be between 2009 and 2100")
        if not 1 <= self.month <= 12:
            raise ValueError("month must be between 1 and 12")

    @classmethod
    def parse(cls, value: str) -> Self:
        try:
            year_text, month_text = value.split("-", maxsplit=1)
            if len(year_text) != 4 or len(month_text) != 2:
                raise ValueError
            return cls(year=int(year_text), month=int(month_text))
        except ValueError as error:
            raise ValueError("month must use YYYY-MM format") from error

    def __str__(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"

    @property
    def start(self) -> datetime:
        return datetime(self.year, self.month, 1)

    @property
    def end(self) -> datetime:
        if self.month == 12:
            return datetime(self.year + 1, 1, 1)
        return datetime(self.year, self.month + 1, 1)


@dataclass(frozen=True, slots=True)
class PartitionPaths:
    bronze_file: Path
    bronze_manifest: Path
    silver_file: Path
    invalid_rows_file: Path
    quarantine_source_file: Path
    quality_report: Path

    @classmethod
    def from_root(cls, data_root: Path, month: YearMonth) -> Self:
        partition = f"month={month}"
        bronze = data_root / "bronze" / TAXI_KIND / partition
        silver = data_root / "silver" / TAXI_KIND / partition
        quarantine = data_root / "quarantine" / TAXI_KIND / partition
        quality = data_root / "quality" / TAXI_KIND / partition
        return cls(
            bronze_file=bronze / f"yellow_tripdata_{month}.parquet",
            bronze_manifest=bronze / "manifest.json",
            silver_file=silver / "trips.parquet",
            invalid_rows_file=quarantine / "invalid_rows.parquet",
            quarantine_source_file=quarantine / "source.parquet",
            quality_report=quality / "report.json",
        )


class FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class BronzeManifest(FrozenModel):
    month: str
    source_url: str
    object_path: str
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size_bytes: int = Field(gt=8)
    retrieved_at: AwareDateTime
    contract_version: str = CONTRACT_VERSION


class PartitionStatus(StrEnum):
    ACCEPTED = "accepted"
    QUARANTINED = "quarantined"


class PartitionQualityReport(FrozenModel):
    month: str
    status: PartitionStatus
    total_rows: int = Field(ge=0)
    valid_rows: int = Field(ge=0)
    invalid_rows: int = Field(ge=0)
    violation_rate: float = Field(ge=0, le=1)
    max_violation_rate: float = Field(ge=0, le=1)
    checks: tuple[QualityCheckResult, ...]
    source_path: str
    source_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    silver_path: str | None
    quarantine_path: str | None
    evaluated_at: AwareDateTime
    contract_version: str = CONTRACT_VERSION

    @model_validator(mode="after")
    def counts_are_consistent(self) -> Self:
        if self.valid_rows + self.invalid_rows != self.total_rows:
            raise ValueError("valid_rows and invalid_rows must add up to total_rows")
        expected_rate = self.invalid_rows / self.total_rows if self.total_rows else 0.0
        if abs(self.violation_rate - expected_rate) > 1e-12:
            raise ValueError("violation_rate must agree with row counts")
        accepted = self.total_rows > 0 and expected_rate <= self.max_violation_rate
        if (self.status is PartitionStatus.ACCEPTED) != accepted:
            raise ValueError("status must agree with the partition quality gate")
        return self


class DownloadStream(Protocol):
    def read(self, size: int = -1) -> bytes: ...

    def __enter__(self) -> Self: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None: ...


DownloadOpener = Callable[[Request, float], DownloadStream]


def _default_opener(request: Request, timeout: float) -> DownloadStream:
    return cast(DownloadStream, urlopen(request, timeout=timeout))


def source_url(month: YearMonth, base_url: str) -> str:
    """Return the official TLC object URL for a monthly yellow-taxi partition."""

    return f"{base_url.rstrip('/')}/yellow_tripdata_{month}.parquet"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(DOWNLOAD_CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def _is_parquet(path: Path) -> bool:
    if path.stat().st_size <= 8:
        return False
    with path.open("rb") as stream:
        if stream.read(4) != PARQUET_MAGIC:
            return False
        stream.seek(-4, os.SEEK_END)
        return stream.read(4) == PARQUET_MAGIC


def _write_json_atomic(path: Path, payload: Mapping[str, object]) -> None:
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


def _existing_manifest(
    paths: PartitionPaths, month: YearMonth, expected_url: str
) -> BronzeManifest | None:
    if not paths.bronze_file.exists() or not paths.bronze_manifest.exists():
        return None
    manifest = BronzeManifest.model_validate_json(paths.bronze_manifest.read_text(encoding="utf-8"))
    actual_size = paths.bronze_file.stat().st_size
    actual_checksum = _sha256(paths.bronze_file)
    matches = (
        manifest.month == str(month)
        and manifest.source_url == expected_url
        and manifest.object_path == str(paths.bronze_file)
        and manifest.size_bytes == actual_size
        and manifest.sha256 == actual_checksum
        and _is_parquet(paths.bronze_file)
    )
    if not matches:
        raise BronzeIntegrityError(
            f"existing bronze object does not match its manifest: {paths.bronze_file}"
        )
    return manifest


def download_bronze(
    month: YearMonth,
    paths: PartitionPaths,
    *,
    base_url: str,
    timeout_seconds: float,
    opener: DownloadOpener = _default_opener,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> BronzeManifest:
    """Download a partition atomically, or return its verified existing manifest."""

    url = source_url(month, base_url)
    if manifest := _existing_manifest(paths, month, url):
        return manifest

    paths.bronze_file.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=paths.bronze_file.parent, delete=False
        ) as temporary:
            temporary_path = Path(temporary.name)
            request = Request(url, headers={"User-Agent": "tripml/0.1"})
            with opener(request, timeout_seconds) as response:
                while chunk := response.read(DOWNLOAD_CHUNK_SIZE):
                    temporary.write(chunk)
            temporary.flush()
            os.fsync(temporary.fileno())

        if not _is_parquet(temporary_path):
            raise BronzeIntegrityError(f"download is not a valid Parquet object: {url}")

        size_bytes = temporary_path.stat().st_size
        checksum = _sha256(temporary_path)
        temporary_path.replace(paths.bronze_file)
        temporary_path = None
        manifest = BronzeManifest(
            month=str(month),
            source_url=url,
            object_path=str(paths.bronze_file),
            sha256=checksum,
            size_bytes=size_bytes,
            retrieved_at=now(),
        )
        _write_json_atomic(
            paths.bronze_manifest,
            manifest.model_dump(mode="json"),
        )
        return manifest
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _temporary_parquet_path(destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=destination.parent, suffix=".parquet", delete=False
    ) as file:
        return Path(file.name)


def _copy_atomic(source: Path, destination: Path) -> None:
    temporary = _temporary_parquet_path(destination)
    try:
        shutil.copyfile(source, temporary)
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)


def _false_on_null(condition: pa.Array) -> pa.Array:
    return pc.fill_null(condition, False)


def _true_on_null(condition: pa.Array, *values: pa.Array) -> pa.Array:
    any_null: pa.Array = pc.invert(pc.is_valid(values[0]))
    for value in values[1:]:
        any_null = pc.or_(any_null, pc.invert(pc.is_valid(value)))
    return pc.or_(any_null, _false_on_null(condition))


def _and_all(conditions: Iterator[pa.Array], row_count: int) -> pa.Array:
    result: pa.Array = pa.array([True] * row_count, type=pa.bool_())
    for condition in conditions:
        result = pc.and_(result, condition)
    return result


def _batch_rule_masks(
    batch: pa.RecordBatch, month: YearMonth, known_zone_ids: pa.Array
) -> dict[str, pa.Array]:
    columns = {name: batch.column(batch.schema.get_field_index(name)) for name in SOURCE_COLUMNS}
    pickup = pc.cast(columns["tpep_pickup_datetime"], pa.timestamp("us"))
    dropoff = pc.cast(columns["tpep_dropoff_datetime"], pa.timestamp("us"))
    passenger = pc.cast(columns["passenger_count"], pa.float64())
    distance = pc.cast(columns["trip_distance"], pa.float64())
    pickup_zone = pc.cast(columns["PULocationID"], pa.int64())
    dropoff_zone = pc.cast(columns["DOLocationID"], pa.int64())
    fare = pc.cast(columns["fare_amount"], pa.float64())

    required = _and_all((pc.is_valid(value) for value in columns.values()), batch.num_rows)
    duration_microseconds = pc.subtract(pc.cast(dropoff, pa.int64()), pc.cast(pickup, pa.int64()))
    duration_range = pc.and_(
        pc.greater_equal(duration_microseconds, 60 * 1_000_000),
        pc.less_equal(duration_microseconds, 3 * 60 * 60 * 1_000_000),
    )
    distance_range = pc.and_(
        pc.greater_equal(distance, 0.0),
        pc.less_equal(distance, 100.0),
    )
    passenger_range = pc.and_(
        pc.and_(pc.greater_equal(passenger, 0.0), pc.less_equal(passenger, 9.0)),
        pc.equal(passenger, pc.floor(passenger)),
    )
    month_range = pc.and_(
        pc.and_(
            pc.greater_equal(pickup, pa.scalar(month.start, type=pa.timestamp("us"))),
            pc.less(pickup, pa.scalar(month.end, type=pa.timestamp("us"))),
        ),
        pc.and_(
            pc.greater_equal(dropoff, pa.scalar(month.start, type=pa.timestamp("us"))),
            pc.less(dropoff, pa.scalar(month.end, type=pa.timestamp("us"))),
        ),
    )
    return {
        "required_fields": required,
        "pickup_before_dropoff": _true_on_null(pc.less(pickup, dropoff), pickup, dropoff),
        "duration_range": _true_on_null(duration_range, pickup, dropoff),
        "distance_range": _true_on_null(distance_range, distance),
        "passenger_count_range": _true_on_null(passenger_range, passenger),
        "known_pickup_zone": _true_on_null(
            pc.is_in(pickup_zone, value_set=known_zone_ids), pickup_zone
        ),
        "known_dropoff_zone": _true_on_null(
            pc.is_in(dropoff_zone, value_set=known_zone_ids), dropoff_zone
        ),
        "non_negative_fare": _true_on_null(pc.greater_equal(fare, 0.0), fare),
        "timestamps_in_partition": _true_on_null(month_range, pickup, dropoff),
    }


def _normalized_table(
    batch: pa.RecordBatch,
    valid_mask: pa.Array,
    month: YearMonth,
    row_offset: int,
) -> pa.Table:
    source = pa.Table.from_batches([batch]).filter(valid_mask)
    selected_indices = pc.indices_nonzero(valid_mask).to_pylist()
    source_rows = pa.array([row_offset + int(index) for index in selected_indices], type=pa.int64())
    pickup = pc.cast(source["tpep_pickup_datetime"], pa.timestamp("us"))
    dropoff = pc.cast(source["tpep_dropoff_datetime"], pa.timestamp("us"))
    duration_seconds = pc.cast(
        pc.divide(
            pc.subtract(pc.cast(dropoff, pa.int64()), pc.cast(pickup, pa.int64())),
            1_000_000,
        ),
        pa.int32(),
    )
    return pa.table(
        {
            "trip_id": pa.array(
                [f"{TAXI_KIND}:{month}:{row_number}" for row_number in source_rows.to_pylist()]
            ),
            "pickup_datetime": pickup,
            "dropoff_datetime": dropoff,
            "pickup_zone_id": pc.cast(source["PULocationID"], pa.int16()),
            "dropoff_zone_id": pc.cast(source["DOLocationID"], pa.int16()),
            "trip_distance_miles": pc.cast(source["trip_distance"], pa.float64()),
            "passenger_count": pc.cast(source["passenger_count"], pa.int16()),
            "fare_amount": pc.cast(source["fare_amount"], pa.float64()),
            "actual_duration_seconds": duration_seconds,
            "source_row_number": source_rows,
            "contract_version": pa.array([CONTRACT_VERSION] * len(source_rows)),
        },
        schema=SILVER_SCHEMA,
    )


def _invalid_table(
    batch: pa.RecordBatch,
    invalid_mask: pa.Array,
    rule_masks: Mapping[str, pa.Array],
    row_offset: int,
) -> pa.Table:
    table = pa.Table.from_batches([batch])
    table = table.append_column(
        "source_row_number",
        pa.array(range(row_offset, row_offset + batch.num_rows), type=pa.int64()),
    )
    for rule, passed in rule_masks.items():
        table = table.append_column(f"violates_{rule}", pc.invert(passed))
    return table.filter(invalid_mask)


def _quality_results(
    month: YearMonth,
    violations: Mapping[str, int],
    total_rows: int,
    threshold: float,
    invalid_rows: int,
) -> tuple[QualityCheckResult, ...]:
    all_violations = {"any_contract_violation": invalid_rows, **violations}
    return tuple(
        QualityCheckResult(
            partition=str(month),
            rule=rule,
            violations=count,
            total_rows=total_rows,
            threshold=threshold,
            passed=(count / total_rows if total_rows else 0.0) <= threshold,
        )
        for rule, count in all_violations.items()
    )


def validate_partition(
    source: Path,
    month: YearMonth,
    paths: PartitionPaths,
    *,
    max_violation_rate: float,
    batch_size: int,
    known_zone_ids: frozenset[int] = frozenset(range(1, 266)),
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> PartitionQualityReport:
    """Validate one TLC partition and publish silver only when its gate passes."""

    if not 0 <= max_violation_rate <= 1:
        raise ValueError("max_violation_rate must be between zero and one")
    parquet = pq.ParquetFile(source)
    missing = sorted(set(SOURCE_COLUMNS) - set(parquet.schema_arrow.names))
    if missing:
        raise SourceSchemaError(f"source partition is missing columns: {', '.join(missing)}")

    silver_temporary = _temporary_parquet_path(paths.silver_file)
    invalid_temporary = _temporary_parquet_path(paths.invalid_rows_file)
    silver_writer: pq.ParquetWriter | None = None
    invalid_writer: pq.ParquetWriter | None = None
    total_rows = 0
    valid_rows = 0
    violation_counts: dict[str, int] = {}
    zone_array = pa.array(sorted(known_zone_ids), type=pa.int64())
    scan_completed = False
    try:
        for batch in parquet.iter_batches(batch_size=batch_size, columns=list(SOURCE_COLUMNS)):
            rule_masks = _batch_rule_masks(batch, month, zone_array)
            valid_mask = _and_all(iter(rule_masks.values()), batch.num_rows)
            invalid_mask = pc.invert(valid_mask)
            batch_valid_rows = int(pc.sum(pc.cast(valid_mask, pa.int64())).as_py() or 0)
            for rule, passed in rule_masks.items():
                passed_count = int(pc.sum(pc.cast(passed, pa.int64())).as_py() or 0)
                violation_counts[rule] = (
                    violation_counts.get(rule, 0) + batch.num_rows - passed_count
                )

            if batch_valid_rows:
                silver_table = _normalized_table(batch, valid_mask, month, total_rows)
                if silver_writer is None:
                    silver_writer = pq.ParquetWriter(silver_temporary, SILVER_SCHEMA)
                silver_writer.write_table(silver_table)
            if batch_valid_rows < batch.num_rows:
                invalid_table = _invalid_table(batch, invalid_mask, rule_masks, total_rows)
                if invalid_writer is None:
                    invalid_writer = pq.ParquetWriter(invalid_temporary, invalid_table.schema)
                invalid_writer.write_table(invalid_table)

            total_rows += batch.num_rows
            valid_rows += batch_valid_rows
        scan_completed = True
    finally:
        if silver_writer is not None:
            silver_writer.close()
        if invalid_writer is not None:
            invalid_writer.close()
        if not scan_completed:
            silver_temporary.unlink(missing_ok=True)
            invalid_temporary.unlink(missing_ok=True)

    invalid_rows = total_rows - valid_rows
    violation_rate = invalid_rows / total_rows if total_rows else 0.0
    accepted = total_rows > 0 and violation_rate <= max_violation_rate
    try:
        if accepted:
            if silver_writer is None:
                raise AssertionError("accepted partition did not produce silver rows")
            silver_temporary.replace(paths.silver_file)
            paths.quarantine_source_file.unlink(missing_ok=True)
            if invalid_writer is not None:
                invalid_temporary.replace(paths.invalid_rows_file)
            else:
                paths.invalid_rows_file.unlink(missing_ok=True)
        else:
            _copy_atomic(source, paths.quarantine_source_file)
            if invalid_writer is not None:
                invalid_temporary.replace(paths.invalid_rows_file)
            else:
                paths.invalid_rows_file.unlink(missing_ok=True)

        report = PartitionQualityReport(
            month=str(month),
            status=PartitionStatus.ACCEPTED if accepted else PartitionStatus.QUARANTINED,
            total_rows=total_rows,
            valid_rows=valid_rows,
            invalid_rows=invalid_rows,
            violation_rate=violation_rate,
            max_violation_rate=max_violation_rate,
            checks=_quality_results(
                month, violation_counts, total_rows, max_violation_rate, invalid_rows
            ),
            source_path=str(source),
            source_sha256=_sha256(source),
            silver_path=str(paths.silver_file) if accepted else None,
            quarantine_path=(
                str(paths.invalid_rows_file if accepted else paths.quarantine_source_file)
                if invalid_rows or not accepted
                else None
            ),
            evaluated_at=now(),
        )
        _write_json_atomic(paths.quality_report, report.model_dump(mode="json"))
        return report
    finally:
        silver_temporary.unlink(missing_ok=True)
        invalid_temporary.unlink(missing_ok=True)
