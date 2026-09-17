"""Manually triggered, quality-gated TLC ingestion DAG."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import Param, dag, task
from airflow.sdk.exceptions import AirflowFailException

from tripml.features import build_gold_features
from tripml.ingestion import PartitionStatus
from tripml.settings import load_settings
from tripml.workflows.ingestion import run_ingestion


def execute_ingestion_task(month: str, config_path: str | None) -> dict[str, object]:
    """Execute the shared workflow and map quarantine to a non-retryable Airflow failure."""

    execution = run_ingestion(
        month,
        config_path=Path(config_path) if config_path else None,
        require_lineage=True,
    )
    if execution.report.status is PartitionStatus.QUARANTINED:
        raise AirflowFailException(
            f"partition {month} exceeded its quality threshold; see lineage run "
            f"{execution.lineage_run_id}"
        )
    return execution.as_document()


def execute_feature_task(month: str, config_path: str | None) -> dict[str, object]:
    """Build gold only after the upstream partition has been accepted."""

    settings = load_settings(Path(config_path) if config_path else None)
    report = build_gold_features(month, settings=settings)
    return report.model_dump(mode="json")


@dag(
    dag_id="tripml_ingestion",
    description="Download and quality-gate one NYC TLC yellow-taxi partition",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=45),
    render_template_as_native_obj=True,
    params={
        "month": Param(
            "2024-01",
            type="string",
            pattern=r"^20[0-9]{2}-(0[1-9]|1[0-2])$",
            description="TLC source month in YYYY-MM format",
        ),
        "config_path": Param(
            None,
            type=["null", "string"],
            description="Optional configuration file visible to the Airflow worker",
        ),
    },
    tags=["tripml", "batch", "ingestion"],
)
def _tripml_ingestion() -> None:
    @task(
        task_id="ingest_partition",
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=20),
    )
    def ingest_partition(month: str, config_path: str | None) -> dict[str, object]:
        return execute_ingestion_task(month, config_path)

    @task(
        task_id="build_gold_features",
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=15),
    )
    def build_features(month: str, config_path: str | None) -> dict[str, object]:
        return execute_feature_task(month, config_path)

    ingestion_result = ingest_partition("{{ params.month }}", "{{ params.config_path }}")
    gold_result = build_features("{{ params.month }}", "{{ params.config_path }}")
    ingestion_result >> gold_result


tripml_ingestion_dag = _tripml_ingestion()
