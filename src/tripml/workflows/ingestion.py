"""Orchestrator-independent ingestion workflow."""

from __future__ import annotations

from pathlib import Path
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from tripml.ingestion import (
    PartitionPaths,
    PartitionQualityReport,
    PartitionStatus,
    YearMonth,
    download_bronze,
    source_url,
    validate_partition,
)
from tripml.lineage import PostgresLineageRepository
from tripml.settings import load_settings


class IngestionConfigurationError(RuntimeError):
    """The workflow cannot satisfy its configured operational guarantees."""


class IngestionExecution(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    report: PartitionQualityReport
    lineage_run_id: UUID | None

    @property
    def exit_code(self) -> int:
        return 0 if self.report.status is PartitionStatus.ACCEPTED else 2

    def as_document(self) -> dict[str, object]:
        document: dict[str, object] = self.report.model_dump(mode="json")
        document["lineage_run_id"] = (
            str(self.lineage_run_id) if self.lineage_run_id is not None else None
        )
        return document


def run_ingestion(
    month_value: str,
    *,
    config_path: Path | None = None,
    source: Path | None = None,
    require_lineage: bool = False,
) -> IngestionExecution:
    """Run one idempotent partition ingestion with optional durable lineage."""

    month = YearMonth.parse(month_value)
    settings = load_settings(config_path)
    ingestion = settings.ingestion
    paths = PartitionPaths.from_root(ingestion.data_root, month)
    lineage_settings = settings.lineage
    database_url = lineage_settings.database_url
    if require_lineage and database_url is None:
        raise IngestionConfigurationError(
            "TRIPML_LINEAGE__DATABASE_URL is required for orchestrated ingestion"
        )
    lineage = (
        PostgresLineageRepository.from_dsn(
            database_url.get_secret_value(), lineage_settings.connect_timeout_seconds
        )
        if database_url is not None
        else None
    )
    if lineage is not None and lineage_settings.auto_migrate:
        lineage.migrate()

    expected_source_path = source if source is not None else paths.bronze_file
    expected_source_url = (
        expected_source_path.resolve().as_uri()
        if source is not None
        else source_url(month, ingestion.source_base_url)
    )
    lineage_run_id = (
        lineage.start_run(
            partition_key=str(month),
            source_path=str(expected_source_path),
            source_url=expected_source_url,
            config_fingerprint=settings.fingerprint,
        )
        if lineage is not None
        else None
    )
    try:
        if source is None:
            manifest = download_bronze(
                month,
                paths,
                base_url=ingestion.source_base_url,
                timeout_seconds=ingestion.download_timeout_seconds,
            )
            source = Path(manifest.object_path)
        report = validate_partition(
            source,
            month,
            paths,
            max_violation_rate=ingestion.max_partition_violation_rate,
            batch_size=ingestion.batch_size,
        )
    except Exception as error:
        if lineage is not None and lineage_run_id is not None:
            lineage.fail_run(lineage_run_id, error)
        raise
    if lineage is not None and lineage_run_id is not None:
        lineage.complete_run(lineage_run_id, report)
    return IngestionExecution(report=report, lineage_run_id=lineage_run_id)
