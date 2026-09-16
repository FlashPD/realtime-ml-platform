from __future__ import annotations

import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from tripml.contracts import QualityCheckResult
from tripml.ingestion import PartitionQualityReport, PartitionStatus
from tripml.lineage import PostgresLineageRepository, RunStatus

pytestmark = pytest.mark.integration


def test_postgres_lineage_round_trip() -> None:
    database_url = os.getenv("TRIPML_TEST_DATABASE_URL")
    if database_url is None:
        pytest.skip("TRIPML_TEST_DATABASE_URL is not configured")

    repository = PostgresLineageRepository.from_dsn(database_url)
    repository.migrate()
    run_id = uuid4()
    repository.start_run(
        partition_key="2024-01",
        source_path="data/bronze/source.parquet",
        source_url="https://example.test/source.parquet",
        config_fingerprint="a" * 64,
        run_id=run_id,
    )
    check = QualityCheckResult(
        partition="2024-01",
        rule="any_contract_violation",
        violations=1,
        total_rows=10,
        threshold=0.2,
        passed=True,
    )
    report = PartitionQualityReport(
        month="2024-01",
        status=PartitionStatus.ACCEPTED,
        total_rows=10,
        valid_rows=9,
        invalid_rows=1,
        violation_rate=0.1,
        max_violation_rate=0.2,
        checks=(check,),
        source_path="data/bronze/source.parquet",
        source_sha256="b" * 64,
        silver_path="data/silver/trips.parquet",
        quarantine_path="data/quarantine/invalid.parquet",
        evaluated_at=datetime.now(UTC),
    )

    repository.complete_run(run_id, report)
    record = repository.get_run(run_id)

    assert record is not None
    assert record.run.status is RunStatus.ACCEPTED
    assert record.checks == (check,)
