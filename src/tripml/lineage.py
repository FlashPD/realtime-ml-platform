"""Durable PostgreSQL lineage for ingestion runs and quality checks."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from enum import StrEnum
from importlib.resources import files
from types import TracebackType
from typing import Any, Protocol, Self, cast
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row
from pydantic import BaseModel, ConfigDict

from tripml.contracts import AwareDateTime, QualityCheckResult
from tripml.ingestion import PartitionQualityReport, PartitionStatus

DATASET = "yellow_taxi"
MIGRATIONS = ("0001_ingestion_lineage.sql",)
MIGRATION_LOCK_ID = 7_629_724_651

Parameter = Sequence[object] | Mapping[str, object] | None


class LineageError(RuntimeError):
    """Base error for invalid lineage operations."""


class LineageStateError(LineageError):
    """A run could not transition from its expected state."""


class RunStatus(StrEnum):
    RUNNING = "running"
    ACCEPTED = "accepted"
    QUARANTINED = "quarantined"
    FAILED = "failed"


class FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class IngestionRun(FrozenModel):
    run_id: UUID
    dataset: str
    partition_key: str
    source_url: str | None
    source_path: str
    source_sha256: str | None
    config_fingerprint: str
    status: RunStatus
    started_at: AwareDateTime
    finished_at: AwareDateTime | None
    total_rows: int | None
    valid_rows: int | None
    invalid_rows: int | None
    violation_rate: float | None
    contract_version: str | None
    silver_path: str | None
    quarantine_path: str | None
    error_type: str | None
    error_message: str | None


class LineageRecord(FrozenModel):
    run: IngestionRun
    checks: tuple[QualityCheckResult, ...]


class Cursor(Protocol):
    rowcount: int

    def execute(
        self, query: str, params: Parameter = None, *, prepare: bool | None = None
    ) -> Self: ...

    def executemany(self, query: str, params_seq: Iterable[Sequence[object]]) -> None: ...

    def fetchone(self) -> Mapping[str, Any] | None: ...

    def fetchall(self) -> list[Mapping[str, Any]]: ...


class Connection(Protocol):
    def cursor(self) -> AbstractContextManager[Cursor]: ...

    def __enter__(self) -> Self: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None: ...


ConnectionFactory = Callable[[], Connection]


def postgres_connection_factory(dsn: str, connect_timeout_seconds: int) -> ConnectionFactory:
    """Build connections lazily so repositories are safe to construct before Postgres is ready."""

    def connect() -> Connection:
        connection = psycopg.connect(
            dsn,
            connect_timeout=connect_timeout_seconds,
            row_factory=dict_row,
        )
        return cast(Connection, connection)

    return connect


def _migration_sql(name: str) -> str:
    resource = files("tripml.migrations").joinpath(name)
    return resource.read_text(encoding="utf-8")


class PostgresLineageRepository:
    """Persist state transitions with one transaction per public operation."""

    def __init__(self, connection_factory: ConnectionFactory) -> None:
        self._connection_factory = connection_factory

    @classmethod
    def from_dsn(cls, dsn: str, connect_timeout_seconds: int = 10) -> Self:
        return cls(postgres_connection_factory(dsn, connect_timeout_seconds))

    def migrate(self) -> None:
        with self._connection_factory() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", (MIGRATION_LOCK_ID,))
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS tripml_schema_migrations (
                    version TEXT PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL
                )
                """
            )
            cursor.execute("SELECT version FROM tripml_schema_migrations")
            applied = {str(row["version"]) for row in cursor.fetchall()}
            for migration in MIGRATIONS:
                if migration in applied:
                    continue
                cursor.execute(_migration_sql(migration), prepare=False)
                cursor.execute(
                    "INSERT INTO tripml_schema_migrations (version, applied_at) VALUES (%s, %s)",
                    (migration, datetime.now(UTC)),
                )

    def start_run(
        self,
        *,
        partition_key: str,
        source_path: str,
        source_url: str | None,
        config_fingerprint: str,
        started_at: datetime | None = None,
        run_id: UUID | None = None,
    ) -> UUID:
        identifier = run_id or uuid4()
        with self._connection_factory() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO ingestion_runs (
                    run_id, dataset, partition_key, source_url, source_path,
                    config_fingerprint, status, started_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    identifier,
                    DATASET,
                    partition_key,
                    source_url,
                    source_path,
                    config_fingerprint,
                    RunStatus.RUNNING.value,
                    started_at or datetime.now(UTC),
                ),
            )
        return identifier

    def complete_run(
        self,
        run_id: UUID,
        report: PartitionQualityReport,
        *,
        finished_at: datetime | None = None,
    ) -> None:
        status = (
            RunStatus.ACCEPTED
            if report.status is PartitionStatus.ACCEPTED
            else RunStatus.QUARANTINED
        )
        completed_at = finished_at or datetime.now(UTC)
        with self._connection_factory() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE ingestion_runs
                SET source_sha256 = %s, status = %s, finished_at = %s,
                    total_rows = %s, valid_rows = %s, invalid_rows = %s,
                    violation_rate = %s, contract_version = %s,
                    silver_path = %s, quarantine_path = %s
                WHERE run_id = %s AND status = 'running'
                """,
                (
                    report.source_sha256,
                    status.value,
                    completed_at,
                    report.total_rows,
                    report.valid_rows,
                    report.invalid_rows,
                    report.violation_rate,
                    report.contract_version,
                    report.silver_path,
                    report.quarantine_path,
                    run_id,
                ),
            )
            if cursor.rowcount != 1:
                raise LineageStateError(f"run is missing or not running: {run_id}")
            cursor.executemany(
                """
                INSERT INTO ingestion_quality_checks (
                    run_id, rule, violations, total_rows, threshold, passed, contract_version
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    (
                        run_id,
                        check.rule,
                        check.violations,
                        check.total_rows,
                        check.threshold,
                        check.passed,
                        check.contract_version,
                    )
                    for check in report.checks
                ),
            )

    def fail_run(
        self,
        run_id: UUID,
        error: BaseException,
        *,
        finished_at: datetime | None = None,
    ) -> None:
        with self._connection_factory() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE ingestion_runs
                SET status = %s, finished_at = %s, error_type = %s, error_message = %s
                WHERE run_id = %s AND status = 'running'
                """,
                (
                    RunStatus.FAILED.value,
                    finished_at or datetime.now(UTC),
                    type(error).__name__,
                    str(error),
                    run_id,
                ),
            )
            if cursor.rowcount != 1:
                raise LineageStateError(f"run is missing or not running: {run_id}")

    def get_run(self, run_id: UUID) -> LineageRecord | None:
        with self._connection_factory() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT * FROM ingestion_runs WHERE run_id = %s", (run_id,))
            run_row = cursor.fetchone()
            if run_row is None:
                return None
            cursor.execute(
                """
                SELECT r.partition_key AS partition, q.rule, q.violations, q.total_rows,
                       q.threshold, q.passed, q.contract_version
                FROM ingestion_quality_checks AS q
                JOIN ingestion_runs AS r ON r.run_id = q.run_id
                WHERE q.run_id = %s ORDER BY q.rule
                """,
                (run_id,),
            )
            checks = tuple(QualityCheckResult.model_validate(row) for row in cursor.fetchall())
        return LineageRecord(run=IngestionRun.model_validate(run_row), checks=checks)
