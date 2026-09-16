from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, Self
from uuid import UUID

import pytest

from tripml.contracts import QualityCheckResult
from tripml.ingestion import PartitionQualityReport, PartitionStatus
from tripml.lineage import (
    DATASET,
    MIGRATIONS,
    IngestionRun,
    LineageStateError,
    PostgresLineageRepository,
    RunStatus,
)

NOW = datetime(2024, 1, 31, 12, tzinfo=UTC)
RUN_ID = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")


class FakeCursor:
    def __init__(self) -> None:
        self.rowcount = 1
        self.executed: list[tuple[str, object]] = []
        self.many: list[tuple[object, ...]] = []
        self.fetchone_result: Mapping[str, Any] | None = None
        self.fetchall_result: list[Mapping[str, Any]] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None

    def execute(
        self,
        query: str,
        params: Sequence[object] | Mapping[str, object] | None = None,
        *,
        prepare: bool | None = None,
    ) -> Self:
        self.executed.append((query, (params, prepare)))
        return self

    def executemany(self, query: str, params_seq: Iterable[Sequence[object]]) -> None:
        self.executed.append((query, None))
        self.many = [tuple(params) for params in params_seq]

    def fetchone(self) -> Mapping[str, Any] | None:
        return self.fetchone_result

    def fetchall(self) -> list[Mapping[str, Any]]:
        return self.fetchall_result


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None

    def cursor(self) -> FakeCursor:
        return self._cursor


def _repository(cursor: FakeCursor) -> PostgresLineageRepository:
    connection = FakeConnection(cursor)
    return PostgresLineageRepository(lambda: connection)


def _report(status: PartitionStatus = PartitionStatus.ACCEPTED) -> PartitionQualityReport:
    invalid_rows = 1 if status is PartitionStatus.ACCEPTED else 5
    total_rows = 10
    rate = invalid_rows / total_rows
    check = QualityCheckResult(
        partition="2024-01",
        rule="any_contract_violation",
        violations=invalid_rows,
        total_rows=total_rows,
        threshold=0.2,
        passed=rate <= 0.2,
    )
    return PartitionQualityReport(
        month="2024-01",
        status=status,
        total_rows=total_rows,
        valid_rows=total_rows - invalid_rows,
        invalid_rows=invalid_rows,
        violation_rate=rate,
        max_violation_rate=0.2,
        checks=(check,),
        source_path="data/bronze/source.parquet",
        source_sha256="a" * 64,
        silver_path="data/silver/trips.parquet" if status is PartitionStatus.ACCEPTED else None,
        quarantine_path="data/quarantine/invalid.parquet",
        evaluated_at=NOW,
    )


def test_migrations_are_applied_once() -> None:
    cursor = FakeCursor()
    repository = _repository(cursor)

    repository.migrate()

    queries = [query for query, _params in cursor.executed]
    assert any("pg_advisory_xact_lock" in query for query in queries)
    assert any("CREATE TABLE IF NOT EXISTS ingestion_runs" in query for query in queries)
    assert any("INSERT INTO tripml_schema_migrations" in query for query in queries)

    cursor.executed.clear()
    cursor.fetchall_result = [{"version": MIGRATIONS[0]}]
    repository.migrate()
    queries = [query for query, _params in cursor.executed]
    assert not any("CREATE TABLE IF NOT EXISTS ingestion_runs" in query for query in queries)


def test_run_lifecycle_writes_normalized_quality_rows() -> None:
    cursor = FakeCursor()
    repository = _repository(cursor)

    identifier = repository.start_run(
        partition_key="2024-01",
        source_path="data/bronze/source.parquet",
        source_url="https://example.test/source.parquet",
        config_fingerprint="b" * 64,
        started_at=NOW,
        run_id=RUN_ID,
    )
    repository.complete_run(RUN_ID, _report(), finished_at=NOW)

    assert identifier == RUN_ID
    insert_params = cursor.executed[0][1]
    assert isinstance(insert_params, tuple)
    assert insert_params[0][1] == DATASET
    assert cursor.many == [(RUN_ID, "any_contract_violation", 1, 10, 0.2, True, "1.0")]


def test_quarantined_and_failed_runs_use_terminal_states() -> None:
    cursor = FakeCursor()
    repository = _repository(cursor)

    repository.complete_run(RUN_ID, _report(PartitionStatus.QUARANTINED), finished_at=NOW)
    update_params = cursor.executed[-2][1]
    assert isinstance(update_params, tuple)
    assert update_params[0][1] == RunStatus.QUARANTINED.value

    cursor.executed.clear()
    repository.fail_run(RUN_ID, ValueError("bad partition"), finished_at=NOW)
    failure_params = cursor.executed[0][1]
    assert isinstance(failure_params, tuple)
    assert failure_params[0][0] == RunStatus.FAILED.value
    assert failure_params[0][2:] == ("ValueError", "bad partition", RUN_ID)


@pytest.mark.parametrize("operation", ["complete", "fail"])
def test_terminal_transition_requires_a_running_record(operation: str) -> None:
    cursor = FakeCursor()
    cursor.rowcount = 0
    repository = _repository(cursor)
    if operation == "complete":
        with pytest.raises(LineageStateError, match="not running"):
            repository.complete_run(RUN_ID, _report())
        return

    with pytest.raises(LineageStateError, match="not running"):
        repository.fail_run(RUN_ID, RuntimeError("failed"))


def test_get_run_returns_record_and_quality_checks() -> None:
    cursor = FakeCursor()
    run = IngestionRun(
        run_id=RUN_ID,
        dataset=DATASET,
        partition_key="2024-01",
        source_url="https://example.test/source.parquet",
        source_path="data/bronze/source.parquet",
        source_sha256="a" * 64,
        config_fingerprint="b" * 64,
        status=RunStatus.ACCEPTED,
        started_at=NOW,
        finished_at=NOW,
        total_rows=10,
        valid_rows=9,
        invalid_rows=1,
        violation_rate=0.1,
        contract_version="1.0",
        silver_path="data/silver/trips.parquet",
        quarantine_path="data/quarantine/invalid.parquet",
        error_type=None,
        error_message=None,
    )
    cursor.fetchone_result = run.model_dump()
    cursor.fetchall_result = [
        {
            "partition": "2024-01",
            "rule": "any_contract_violation",
            "violations": 1,
            "total_rows": 10,
            "threshold": 0.2,
            "passed": True,
            "contract_version": "1.0",
        }
    ]

    record = _repository(cursor).get_run(RUN_ID)

    assert record is not None
    assert record.run == run
    assert record.checks[0].rule == "any_contract_violation"


def test_get_run_returns_none_when_identifier_is_unknown() -> None:
    assert _repository(FakeCursor()).get_run(RUN_ID) is None
